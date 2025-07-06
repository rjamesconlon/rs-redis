use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;
use std::vec::Vec;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use chrono;

use crate::types::DB_TYPE;


static REDIS_DB: Lazy<Mutex<HashMap<String, DB_TYPE>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static EXPIRE_DB: Lazy<Mutex<HashMap<String, u128>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn set(k: String, v: DB_TYPE, t: u128) -> Result<String, String> {
    let mut db = REDIS_DB.lock().expect("DB mutex lock failed");

    db.insert(k.clone(), v);

    if t > 0 {
        expire(&k, t);
    }

    Ok("OK".to_string())
}

pub fn get(k: &str) -> Option<DB_TYPE> {
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
    match get(k) {
        Some(val) => match val {
            DB_TYPE::Int(i) => {
                match set(k.to_string(), DB_TYPE::Int(i + 1_i64), 0) {
                    Ok(_) => return Ok("OK".to_string()),
                    Err(e) => return Err(e)
                }
            },
            // error parsing...
            _ => return Err("value is not an integer or out of range".to_string()),
        },
        None => {
            match set(k.to_string(), DB_TYPE::Int(1), 0) {
                Ok(_) => return Ok("OK".to_string()),
                Err(e) => return Err(e)
            }
        }
    };
}

pub fn decrement(k: &str) -> Result<String, String> {
    match get(k) {
        Some(val) => match val {
            DB_TYPE::Int(i) => {
                // if key is parsed correctly, increment and set
                match set(k.to_string(), DB_TYPE::Int(i - 1_i64), 0) {
                    Ok(_) => return Ok("OK".to_string()),
                    Err(e) => return Err(e)
                }
            },
            // error parsing...
            _ => return Err("value is not an integer or out of range".to_string()),
        },
        // if no key, set key to 1
        None => {
            match set(k.to_string(), DB_TYPE::Int(1), 0) {
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

pub fn lpush(k: &str, values: Vec<DB_TYPE>) -> Result<i64, String> {
    let mut db = REDIS_DB.lock().unwrap();

    let mut v = match db.get(k) {
        Some(v) => match v {
            DB_TYPE::Array(arr) => arr.clone(),
            _ => return Err("Not an array".to_string()),
        },
        None => Vec::<DB_TYPE>::new()
    };

    
    for value in values {
        v.insert(0, value);
    }

    let l = v.len();

    db.insert(k.to_string(), DB_TYPE::Array(v));

    Ok(l as i64)
}

pub fn rpush(k: &str, values: Vec<DB_TYPE>) -> Result<i64, String> {
    let mut db = REDIS_DB.lock().unwrap();

    let mut v = match db.get(k) {
        Some(v) => match v {
            DB_TYPE::Array(arr) => arr.clone(),
            _ => return Err("Not an array".to_string()),
        },
        None => Vec::<DB_TYPE>::new()
    };

    
    for value in values {
        v.push(value);
    }

    let l = v.len();

    db.insert(k.to_string(), DB_TYPE::Array(v));

    Ok(l as i64)
}

pub fn write_db_to_file() -> Result<String, String> {
    let db = REDIS_DB.lock().unwrap();
    let e_db = EXPIRE_DB.lock().unwrap();

    // get some metadata
    let time = chrono::Utc::now();
    // create file
    let file = File::create("./REDIS.rdb").expect("Cannot create rdb file");
    let mut buf_writer = BufWriter::new(file);

    // append general detail
     buf_writer.write("--------------------------------------------------------\r\n".as_bytes()).ok();
     buf_writer.write("REDIS\r\n".as_bytes()).ok();
     buf_writer.write("0001\r\n".as_bytes()).ok();
     buf_writer.write("--------------------------------------------------------\r\n".as_bytes()).ok();
     buf_writer.write(time.to_string().as_bytes()).ok();
     buf_writer.write("\r\n--------------------------------------------------------\r\n".as_bytes()).ok();
     buf_writer.write("KEYS-VALUES\r\n".as_bytes()).ok();
    
    // for each string
        // save expire
        // get type
        // save val as bytestring
        // if array, say as array of bytestrings...
    for(key, value) in db.iter() {
        buf_writer.write("--------------------------------------------------------\r\n".as_bytes()).ok();

        // get expire time
        let exp_time = match e_db.get(key) {
            Some(i) => i,
            None => &0
        };
        buf_writer.write(format!("FD {exp_time}\r\n").as_bytes()).ok();

        // write value type

        // write key
        buf_writer.write(format!("${key}\r\n").as_bytes()).ok();

        // write value
        match value {
            DB_TYPE::Int(i) => { 
                buf_writer.write(format!("$i\r\n").as_bytes()).ok();
                buf_writer.write(format!("${i}\r\n").as_bytes()).ok();
            },
            DB_TYPE::Str(s) => {
                buf_writer.write(format!("$s\r\n").as_bytes()).ok();
                let slen = s.len();
                buf_writer.write(format!("${slen}${s}\r\n").as_bytes()).ok();
            },
            DB_TYPE::Array(a) => {
                let alen = a.len();
                buf_writer.write(format!("$a\r\n").as_bytes()).ok();
                buf_writer.write(format!("*{alen}\r\n").as_bytes()).ok();
                for v in a {
                    match v {
                        DB_TYPE::Int(i) => {
                            buf_writer.write(format!("$i\r\n").as_bytes()).ok();
                            buf_writer.write(format!("${i}\r\n").as_bytes()).ok();
                        },
                        DB_TYPE::Str(s) => {
                            buf_writer.write(format!("$s\r\n").as_bytes()).ok();
                            let slen = s.len();
                            buf_writer.write(format!("${slen}${s}\r\n").as_bytes()).ok();
                        },
                        DB_TYPE::Array(_) => return Err("nested arrays are not supported".to_string()),
                    }
                }
            }
        }
    }
    
    buf_writer.write("--------------------------------------------------------\r\n".as_bytes()).ok();
    buf_writer.write("EOF\r\n".as_bytes()).ok();

    Ok("OK".to_string())
}


pub fn read_db_from_file(file_path: &str) -> Result<String, String> {
    // open local file if existing via file path
    let file = File::open(file_path).expect("Cannot create rdb file");
    let mut buf_reader = BufReader::new(file);
    let mut line = String::new();

    // db lock
    // expire db lock
    let mut db = REDIS_DB.lock().unwrap();
    let mut e_db = EXPIRE_DB.lock().unwrap();

    // read until strings.. ignore metadata
    let mut kv = false;
    // for each string
    while !kv {
        line.clear();
        match buf_reader.read_line(&mut line) {
            Ok(s)  => { 
                if s == 0 { 
                    return Err("EOF".to_string());
                }
                if line.trim_end() == "KEYS-VALUES".to_string() {
                    kv = true;
                }
            },
            Err(e) => return Err(e.to_string()),
        }
    }

    buf_reader.read_line(&mut line).ok();
    line.clear();

    // now at strings.. read until eof
    let eof = false;

    while !eof {
        line.clear();

        // get expire
        let exp: u128 = match buf_reader.read_line(&mut line) {
            Ok(size)  => { 
                if size == 0 {
                    return Err("Unexpected EOF".to_string());
                }
                if line.trim() == "EOF" {
                    return Ok("OK".to_string());
                }

                let vals: Vec<&str> = line.trim_end().split(" ").collect();
                if vals.len() == 2 {
                    if vals[0] != "FD" {
                        return Err("Cannot correctly read expire for object".to_string());
                    }
                    else {
                        vals[1].parse().expect("Valid int")
                    }
                }
                else {
                    return Err("Cannot correctly read expire for object".to_string());
                }
            },
            Err(e) => return Err(e.to_string()),
        };

        // get value type
        line.clear();
        let typing: char = match buf_reader.read_line(&mut line) {
            Ok(_)  => { 
                line.chars().nth(1).expect("No char????")
            },
            Err(e) => return Err(e.to_string()),
        };

        // get key
        line.clear();
        let key = match buf_reader.read_line(&mut line) {
            Ok(_)  => { 
                line.trim_end()[1..].to_string()
            },
            Err(e) => return Err(e.to_string()),
        };
        
        line.clear();
        // get value
        let value: DB_TYPE = match typing {
            // int
            'i' => {
                match buf_reader.read_line(&mut line) {
                    Ok(_)  => { 
                        DB_TYPE::Int(line.trim_end()[1..].parse::<i64>().expect("Not a valid integer"))
                    },
                    Err(e) => return Err(e.to_string()),
                }
            },
            // string
            's' => {
                // read bulk string value
                let bytes_to_read = match buf_reader.read_line(&mut line) {
                    Ok(_)  => { 
                        line.trim_end()[1..].parse::<usize>().expect("Not a valid integer")
                    },
                    Err(e) => return Err(e.to_string()),
                };

                line.clear();

                // read bulk string
                let mut bulk = vec![0u8; bytes_to_read + 3]; 
                buf_reader.read_exact(&mut bulk).ok();

                let s = match String::from_utf8(bulk[1..(bulk.len() - 2)].to_vec()) {
                    Ok(s) => s,
                    Err(e) => return Err(e.to_string()),
                };

                DB_TYPE::Str(s)
            },
            // array
            'a' => {
                // objects to read
                let objects_to_read = match buf_reader.read_line(&mut line) {
                    Ok(_)  => { 
                        line.trim_end()[1..].parse::<usize>().expect("Not a valid integer")
                    },
                    Err(e) => return Err(e.to_string()),
                };

                let mut objects: Vec<DB_TYPE> = Vec::new();

                for _ in 0..objects_to_read {
                    // get value type
                    line.clear();
                    let t: char = match buf_reader.read_line(&mut line) {
                        Ok(_)  => { 
                            line.chars().nth(1).expect("No char????")
                        },
                        Err(e) => return Err(e.to_string()),
                    };

                    line.clear();

                    // save key with value
                    let value: DB_TYPE = match t {
                    // int
                    'i' => {
                        match buf_reader.read_line(&mut line) {
                            Ok(_)  => { 
                                DB_TYPE::Int(line.trim_end()[1..].parse::<i64>().expect("Not a valid integer"))
                            },
                            Err(e) => return Err(e.to_string()),
                        }
                    },
                    // string
                    's' => {
                        // read bulk string value
                        let bytes_to_read = match buf_reader.read_line(&mut line) {
                            Ok(_)  => { 
                                line.trim_end()[1..].parse::<usize>().expect("Not a valid integer")
                            },
                            Err(e) => return Err(e.to_string()),
                        };

                        line.clear();

                        // read bulk string
                        let mut bulk = vec![0u8; bytes_to_read + 3]; 
                        buf_reader.read_exact(&mut bulk).ok();

                        let s = match String::from_utf8(bulk[1..(bulk.len() - 2)].to_vec()) {
                            Ok(s) => s,
                            Err(e) => return Err(e.to_string()),
                        };

                        DB_TYPE::Str(s)
                    },
                    _ => return Err("Invalid char encountered for object type".to_string()),
                    };

                    objects.push(value);
                }

                line.clear();

                DB_TYPE::Array(objects)
            },
            _ => return Err("Invalid char encountered for object type".to_string()),
        };

        // save expire
        if exp > 0 {
            e_db.insert(key.clone(), exp);
        }
        db.insert(key.clone(), value);


        buf_reader.read_line(&mut line).ok();
    }

    Ok("OK".to_string())
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::fs::OpenOptions;
    
    #[test]
    fn test_write_db_to_file_basic() {
        {
            let mut db = REDIS_DB.lock().unwrap();
            let mut exp = EXPIRE_DB.lock().unwrap();
            db.clear();
            exp.clear();

            db.insert("intkey".to_string(), DB_TYPE::Int(42));
            db.insert("strkey".to_string(), DB_TYPE::Str("hello".to_string()));
            db.insert(
                "arrkey".to_string(),
                DB_TYPE::Array(vec![
                    DB_TYPE::Int(1),
                    DB_TYPE::Str("hi".to_string()),
                ]),
            );

            exp.insert("intkey".to_string(), 100);
            exp.insert("strkey".to_string(), 200);
            // no expire on arrkey
        }

        let result = write_db_to_file();
        println!("{:?}", result);
        assert!(result.is_ok());

        // read the file and check contents
        let contents = fs::read_to_string("./REDIS.rdb").expect("Failed to read RDB file");
        assert!(contents.contains("REDIS"));
        assert!(contents.contains("KEYS-VALUES"));
        assert!(contents.contains("FD 100"));
        assert!(contents.contains("FD 200"));
        assert!(contents.contains("$i"));
        assert!(contents.contains("$s"));
        assert!(contents.contains("EOF"));
        assert!(contents.contains("hello"));
    }

    #[test]
    fn test_write_db_to_file_with_nested_array_should_fail() {
        {
            let mut db = REDIS_DB.lock().unwrap();
            let mut exp = EXPIRE_DB.lock().unwrap();
            db.clear();
            exp.clear();

            db.insert(
                "bad".to_string(),
                DB_TYPE::Array(vec![
                    DB_TYPE::Array(vec![DB_TYPE::Int(1)])
                ]),
            );
        }

        let result = write_db_to_file();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "nested arrays are not supported".to_string());
    }

    #[tokio::test]
    async fn test_read_db_from_file() {
        // Prepare a dummy REDIS.rdb file with minimal valid content
        let test_file_path = "REDIS.rdb";

        // Create or overwrite the file with a minimal valid DB content
        let mut file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(test_file_path)
                .expect("Failed to create test RDB file")
        );

        let content = "--------------------------------------------------------
REDIS
0001
--------------------------------------------------------
2025-07-06 16:19:16.580645320 UTC
--------------------------------------------------------
KEYS-VALUES
--------------------------------------------------------
FD 100
$i
$intkey
$42
--------------------------------------------------------
FD 200
$s
$strkey
$5
$hello
--------------------------------------------------------
FD 0
$a
$arrkey
*2
$i
$1
$s
$2
$hi
--------------------------------------------------------
EOF";

        file.write_all(content.as_bytes()).expect("Write failed");
        file.flush().expect("Flush failed");

        // Call the function to read from file
        let result = read_db_from_file(test_file_path);

        // Assert it returns OK
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "OK".to_string());

        // Check that the key exists in the DB with expected value
        let db_lock = REDIS_DB.lock().unwrap();
        let expire_lock = EXPIRE_DB.lock().unwrap();

        assert!(db_lock.contains_key("intkey"));
        assert!(db_lock.contains_key("strkey"));
        assert!(db_lock.contains_key("arrkey"));

        assert!(expire_lock.contains_key("intkey"));
        assert!(expire_lock.contains_key("strkey"));
        assert!(!expire_lock.contains_key("arrkey"));

        assert_eq!(expire_lock.get("intkey").unwrap(), &100_u128);
        assert_eq!(expire_lock.get("strkey").unwrap(), &200_u128);

        assert_eq!(db_lock.get("intkey").unwrap(), &DB_TYPE::Int(42));
        assert_eq!(db_lock.get("strkey").unwrap(), &DB_TYPE::Str("hello".to_string()));
        assert_eq!(db_lock.get("arrkey").unwrap(), &DB_TYPE::Array(vec![DB_TYPE::Int(1), DB_TYPE::Str("hi".to_string())]));
    }
}