use std::{cell::RefCell, collections::HashMap, ops::{Deref, DerefMut}};

use crate::storage::{Storage, StorageError};

pub struct PageStore<S: Storage> {
    pool: RefCell<PoolInternal<S>>
}
impl<'store, S: Storage> PageStore<S> {
    pub fn new(storage: S) -> PageStore<S> {
        PageStore {
            pool: RefCell::new(PoolInternal::new(storage))
        }
    }

    pub fn pin_page(&'store self, page: &PageId) -> Result<PinnedPage<'store, S>, PageError> {
        self.pool.borrow_mut().pin_page(page)?;
        Ok(PinnedPage { id: *page, store: &self })
    }
    
    pub fn allocate_page(&'store self, page: &PageId) -> Result<PinnedPage<'store, S>, PageError> {
        self.pool.borrow_mut().create_and_pin_page(page)?;
        Ok(PinnedPage { id: *page, store: &self })
    }

    fn unpin_page(&'store self, page: &PageId) -> Result<(), PageError> {
        self.pool.borrow_mut().unpin_page(page)
    }

    fn try_get_read(&'store self, page: &PageId) -> Result<*const Data, PageError> {
        self.pool.borrow_mut().try_get_read(page)
    }

    fn release_read(&'store self, page: &PageId) -> Result<(), PageError> {
        self.pool.borrow_mut().release_read(page)
    }

    fn try_get_write(&'store self, page: &PageId) -> Result<*mut Data, PageError> {
        self.pool.borrow_mut().try_get_write(page)
    }

    fn release_write(&'store self, page: &PageId) -> Result<(), PageError> {
        self.pool.borrow_mut().release_write(page)
    }
    
}

const POOL_SIZE: usize = 40;
struct PoolInternal<S: Storage> {
    storage: S,
    pages: Vec<Page>,
    page_state: HashMap<PageId, PageMeta>,
}
impl<S: Storage> PoolInternal<S> {
    fn new(storage: S) -> PoolInternal<S> {
        PoolInternal { storage, pages: Vec::with_capacity(POOL_SIZE), page_state: HashMap::new() }
    }

    fn allocate_page(&mut self) -> Result<PageMeta, PageError> {
        if self.pages.len() >= POOL_SIZE {
            return Err(PageError::PoolIsFull)
        }
        let index = self.pages.len();
        self.pages.push(Page { buf: [0u8; 4096] });
        Ok(PageMeta {
            index,
            pins: 0,
            readers: 0,
            writer: false
        })
    }

    fn create_and_pin_page(&mut self, page: &PageId) -> Result<(), PageError> {
        self.storage.create_page(page).map_err(|e| PageError::Storage(e))?;
        self.pin_page(page)
    }

    fn pin_page(&mut self, page: &PageId) -> Result<(), PageError> {
        if let Some(meta) = self.page_state.get_mut(page) {
            meta.pins += 1;
        } else {
            let mut meta = self.allocate_page()?;
            meta.pins += 1;
            self.page_state.insert(page.clone(), meta);
            let meta = &self.page_state[page];
            let index = meta.index;
            self.storage.load_page(&mut self.pages[index].buf, page).map_err(|e| PageError::Storage(e))?;
        }
        Ok(())
    }

    fn unpin_page(&mut self, page: &PageId) -> Result<(), PageError> {
        let meta = self.get_meta(page)?;
        meta.pins -= 1;
        Ok(())
    }

    fn try_get_read(&mut self, page: &PageId) -> Result<*const Data, PageError> {
        let meta = self.get_meta(page)?;
        if meta.writer {
            return Err(PageError::PageInUseForWrite)
        }
        meta.readers += 1;
        let index = meta.index;
        Ok(&self.pages[index].buf)
    }

    fn get_meta(&mut self, page: &PageId) -> Result<&mut PageMeta, PageError> {
        self.page_state.get_mut(page).ok_or(PageError::PageNotInPool)
    }

    fn release_read(&mut self, page: &PageId) -> Result<(), PageError> {
        let meta = self.get_meta(page)?;
        meta.readers -= 1;
        Ok(())
    }

    fn try_get_write(&mut self, page: &PageId) -> Result<*mut Data, PageError> {
        let meta = self.get_meta(page)?;
        if meta.writer {
            return Err(PageError::PageInUseForWrite)
        }
        if meta.readers > 0 {
            return Err(PageError::PageInUseForRead)
        }
        meta.writer = true;
        let index = meta.index;
        Ok(&mut self.pages[index].buf)
    }

    fn release_write(&mut self, page: &PageId) -> Result<(), PageError> {
        let meta = self.get_meta(page)?;
        meta.writer = false;
        Ok(())
    }
}
struct PageMeta {
    index: usize,
    pins: usize,
    readers: usize,
    writer: bool,
}

pub struct PinnedPage<'store, S: Storage> {
    id: PageId,
    store: &'store PageStore<S>
}
impl<'pin, 'store, S: Storage> PinnedPage<'store, S> {
    fn try_read(&'pin self) -> Result<ConstPage<'pin, 'store, S>, PageError> {
        let data = self.store.try_get_read(&self.id)?;
        Ok(ConstPage { pinned: self, data: data })
    }

    fn try_write(&'pin self) -> Result<MutPage<'pin, 'store, S>, PageError> {
        let data = self.store.try_get_write(&self.id)?;
        Ok(MutPage { pinned: self, data: data })
    }
}
impl<S: Storage> Drop for PinnedPage<'_, S> {
    fn drop(&mut self) {
        self.store.unpin_page(&self.id).unwrap();
    }
}

struct ConstPage<'pin, 'store, S: Storage> {
    pinned: &'pin PinnedPage<'store, S>,
    data: *const Data
}
impl<S: Storage> Drop for ConstPage<'_, '_, S> {
    fn drop(&mut self) {
        self.pinned.store.release_read(&self.pinned.id).unwrap()
    }
}
impl<S: Storage> Deref for ConstPage<'_, '_, S> {
    type Target = Data;

    fn deref(&self) -> &Self::Target {
        // SAFETY: InternalPool will prevent construction of MutPage for lifetime of ConstPage
        unsafe {
            &*self.data
        }
    }
}

struct MutPage<'pin, 'store, S: Storage> {
    pinned: &'pin PinnedPage<'store, S>,
    data: *mut Data
}
impl<S: Storage> Deref for MutPage<'_, '_, S> {
    type Target = Data;

    fn deref(&self) -> &Self::Target {
        //SAFETY: If we have exclusive write, we can also read
        unsafe {
            &*self.data
        }
    }
}
impl<S: Storage> DerefMut for MutPage<'_, '_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: InternalPool should maintain exclusive writer status on pages
        unsafe {
            &mut *self.data
        }
    }
}
impl<S: Storage> Drop for MutPage<'_, '_, S> {
    fn drop(&mut self) {
        self.pinned.store.release_write(&self.pinned.id).unwrap();
    }
}

#[derive(Debug, PartialEq)]
pub enum PageError {
    PageNotInPool,
    PageInUseForWrite,
    PageInUseForRead,
    Storage(StorageError),
    PoolIsFull,
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct PageId {
    offset: usize
}
pub type Data = [u8; 4096];
pub struct Page {
    buf: Data 
}

#[cfg(test)]
mod tests {
    use crate::storage::TestStorage;

    use super::{PageStore, PageId, PageError};

    #[test]
    fn test_happy() -> Result<(), PageError> {
        let page_store = PageStore::new(TestStorage::new());

        {
            let page = page_store.allocate_page(&PageId { offset: 0 })?;
            (*page.try_write()?)[0] = 255u8;

            assert_eq!((*page.try_read()?)[0], 255u8);
        }

        {
            let page = page_store.pin_page(&PageId { offset: 0 })?;
            assert_eq!((*page.try_read()?)[0], 255u8);
        }

        Ok(())
    }

    #[test]
    fn test_writer_exclusion() -> Result<(), PageError> {
        let page_store = PageStore::new(TestStorage::new());
        let page = page_store.allocate_page(&PageId { offset: 0 })?;
        let reader = page.try_read()?;

        assert_eq!(page.try_write().err().unwrap(), PageError::PageInUseForRead);

        let _v = reader[0];

        Ok(())
    }

    #[test]
    fn test_reader_exclusion() -> Result<(), PageError> {
        let page_store = PageStore::new(TestStorage::new());
        let page = page_store.allocate_page(&PageId { offset: 0 })?;
        let mut writer = page.try_write()?;

        assert_eq!(page.try_read().err().unwrap(), PageError::PageInUseForWrite);

        writer[0] = 255u8;

        Ok(())
    }
}