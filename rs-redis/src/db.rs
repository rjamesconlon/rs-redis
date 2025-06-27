use std::collections::HashMap;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use std::vec::Vec;

pub static REDIS_DB: Lazy<Mutex<HashMap<String, Vec<u8>>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});