use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;
use std::vec::Vec;

static REDIS_DB: Lazy<Mutex<HashMap<String, Vec<u8>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static EXPIRE_DB: Lazy<Mutex<HashMap<String, u128>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn set(k: String, v: Vec<u8>, t: u128) -> Result<String, String> {
    let mut db = REDIS_DB.lock().expect("DB mutex lock failed");
    db.insert(k.clone(), v.clone());

    if t > 0 {
        expire(&k, t);
    }

    Ok("OK".to_string())
}

pub fn get(k: &str) -> Option<Vec<u8>> {
    let k_expire: u128;

    // get lock on expire db
    {
        let db = EXPIRE_DB.lock().expect("DB mutex lock failed");

        // check if key is in expire db
        k_expire = match db.get(k).cloned() {
            Some(t) => t,
            None => 0,
        };
    } 

    // if key exists, and is expired then delete from expire db and db
    if k_expire > 0
        && SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            > k_expire
    {
        delete(vec![k.to_string()]);
        return None;
    }

    // get lock on redis db
    let db = REDIS_DB.lock().unwrap();

    // return option for key
    db.get(k).cloned()
}

fn expire(k: &str, t: u128) {
    let mut db = EXPIRE_DB.lock().unwrap();
    db.insert(k.to_string(), t);
}

pub fn delete(keys: Vec<String>) -> i32 {
    let mut db = REDIS_DB.lock().unwrap();
    let mut e_db = EXPIRE_DB.lock().unwrap();

    let mut counter = 0;
    for k in keys {
        if let Some(_) = db.remove(&k) {
            counter += 1;
            e_db.remove(&k);
        }
    }

    counter
}

pub fn increment(k: &str) -> Result<String, String> {
    // get key
        
    // check if key exists
    match get(k) {
        // if key exists, try to parse to int
        Some(val) => match String::from_utf8(val).unwrap().parse::<i64>() {
            Ok(i) => {
                // if key is parsed correctly, increment and set
                match set(k.to_string(), (i + 1_i64).to_string().as_bytes().to_vec(), 0) {
                Ok(_) => return Ok("OK".to_string()),
                Err(e) => return Err(e)
            }
            }
            // error parsing...
            Err(_) => return Err("value is not an integer or out of range".to_string()),
        },
        // if no key, set key to 1
        None => {
            match set(k.to_string(), "1".as_bytes().to_vec(), 0) {
                Ok(_) => return Ok("OK".to_string()),
                Err(e) => return Err(e)
            }
        }
    };
}

pub fn decrement(k: &str) -> Result<String, String> {
    // get key
        
    // check if key exists
    match get(k) {
        // if key exists, try to parse to int
        Some(val) => match String::from_utf8(val).unwrap().parse::<i64>() {
            Ok(i) => {
                // if key is parsed correctly, increment and set
                match set(k.to_string(), (i - 1_i64).to_string().as_bytes().to_vec(), 0) {
                Ok(_) => return Ok("OK".to_string()),
                Err(e) => return Err(e)
            }
            }
            // error parsing...
            Err(_) => return Err("value is not an integer or out of range".to_string()),
        },
        // if no key, set key to 1
        None => {
            match set(k.to_string(), "1".as_bytes().to_vec(), 0) {
                Ok(_) => return Ok("OK".to_string()),
                Err(e) => return Err(e)
            }
        }
    };
}

pub fn exists(keys: Vec<String>) -> i32 {
    let db = REDIS_DB.lock().unwrap();

    let mut counter = 0;
    for k in keys {
        if let Some(_) = db.get(&k) {
            counter += 1;
        }
    }

    counter
}
