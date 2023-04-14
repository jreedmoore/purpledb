use std::collections::HashMap;

use crate::page_store::{Data, PageId};

pub trait Storage {
    fn load_page(&self, buf: &mut Data, page: &PageId) -> Result<(), StorageError>;
    fn create_page(&mut self, page: &PageId) -> Result<(), StorageError>;
    fn write_page(&mut self, buf: &Data, page: &PageId) -> Result<(), StorageError>;
}

#[derive(Debug, PartialEq)]
pub enum StorageError {
    NotFound,
    PageAlreadyExists,
}

#[cfg(test)]
pub struct TestStorage {
    map: HashMap<PageId, Data>
}
#[cfg(test)]
impl TestStorage {
    pub(crate) fn new() -> TestStorage {
        TestStorage { map: HashMap::new() }
    }
}
#[cfg(test)]
impl Storage for TestStorage {
    fn load_page(&self, buf: &mut Data, page: &PageId) -> Result<(), StorageError> {
        let data = self.map.get(page).ok_or(StorageError::NotFound)?;
        buf.copy_from_slice(data);
        Ok(())
    }

    fn write_page(&mut self, buf: &Data, page: &PageId) -> Result<(), StorageError> {
        let dst = self.map.entry(page.clone()).or_insert_with(|| {
            [0u8; 4096]
        });
        dst.copy_from_slice(buf);
        Ok(())
    }

    fn create_page(&mut self, page: &PageId) -> Result<(), StorageError> {
        if self.map.contains_key(page) {
            return Err(StorageError::PageAlreadyExists)
        }
        self.map.insert(page.clone(), [0u8; 4096]);
        Ok(()) 
    }
}