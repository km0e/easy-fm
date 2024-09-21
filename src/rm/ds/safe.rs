use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::Mutex;

use super::DataStorage;

#[derive(Clone)]
pub struct SafeDs(Arc<Mutex<Box<dyn DataStorage + Send + Sync>>>);

impl SafeDs {
    pub fn new(ds: Box<dyn DataStorage + Send + Sync>) -> Self {
        Self(Arc::new(Mutex::new(ds)))
    }
}

impl Deref for SafeDs {
    type Target = Arc<Mutex<Box<dyn DataStorage + Send + Sync>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SafeDs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
