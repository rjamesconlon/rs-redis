use crate::types::{RESPResult, DB_TYPE};
use crate::db::{self};
use std::time::SystemTime;

pub fn command_router(command: &str, data: &[RESPResult]) -> Result<RESPResult, String> {
    if command == "ECHO" {
        match echo_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "SET" {
        match set_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "GET" {
        match get_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "PING" {
        Ok(RESPResult::SimpleString("PONG".to_string()))
    }
    else if command == "EXISTS" {
        match exists_command(data) {
            Ok(i) => Ok(RESPResult::Integer(i as i64)),
            Err(e) => Err(e)
        }
    }
    else if command == "DEL" {
        match delete_command(data) {
            Ok(i) => Ok(RESPResult::Integer(i as i64)),
            Err(e) => Err(e)
        }
    }
    else if command == "INCR" {
        match increment_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "DECR" {
        match decrement_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "LPUSH" {
        match lpush_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "RPUSH" {
        match rpush_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "SAVE" {
        match save_command() {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else if command == "LOAD" {
        match load_command(data) {
            Ok(s) => Ok(RESPResult::SimpleString(s)),
            Err(e) => Err(e)
        }
    }
    else {
        Err("panic".to_string())
    }
}

fn echo_command(data: &[RESPResult]) -> Result<String, String> {    
    if data.len() != 1 {
        return Err("Incorrect number of arguments for echo".to_string());
    }

    let message_bulk_string = match &data[0] {
        RESPResult::BulkString(Some(message)) => message,
        _ => return Err("Error: Not bulk string".to_string()),
    };

    match std::str::from_utf8(&message_bulk_string) {
        Ok(s) => Ok(s.to_string()), 
        Err(e) => Err(format!("Invalid UTF-8: {}", e)),
    }
}

fn set_command(data: &[RESPResult]) -> Result<String, String> {
    if data.len() != 2 && data.len() != 4 {
        return Err("Missing key/value for SET".to_string());
    }

    // get key
    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    // get value
    let value = match &data[1] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).expect("Valid string"),
        _ => return Err("Error: Not bulk string".to_string()),
    };


    // if optional arguments, get argument, convert value
    let mut t: u128 = 0;
    if data.len() == 4 {
        let command_arg = match &data[2] {
            RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
            _ => return Err("Error processing command argument".to_string()),
        };

        t = match &data[3] {
            RESPResult::BulkString(Some(message)) => 
                String::from_utf8(message.clone()).unwrap().parse().unwrap(),
            _ => return Err("Error converting t to ms".to_string()),
        };

        t = match command_arg.as_str() {
            // t is expire in seconds
            "EX" => SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() + (t * 1000),
            // t is expire in milliseconds
            "PX" => SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() + t,
            // unix time in seconds
            "EXAT" => t * 1000,
            // unix time in milliseconds
            "PXAT" => t,
            _ => return Err("Optional argument not understood".to_string()),
        }
    }

    let set_val = match value.parse::<i64>() {
        Ok(v) => DB_TYPE::Int(v),
        Err(_) => DB_TYPE::Str(value),
    };

    match db::set(key.clone(), set_val, t) {
        Ok(_) => Ok("OK".to_string()),
        Err(e) => Err(e)
    }
}

fn get_command(data: &[RESPResult]) -> Result<String, String> { 
    
    if data.len() != 1 {
        return Err("Missing key/value for GET".to_string());
    }

    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    let value: Option<DB_TYPE> = db::get(&key);
    
    match value {
        Some(val) => {
            match val {
                DB_TYPE::Int(i) => Ok(i.to_string()),
                DB_TYPE::Str(s) =>  Ok(s),
                
                _ => return Err("Cannot unpack DB_TYPE".to_string())
            }
        },
        None => Ok(String::new())
    }
}

fn exists_command(data: &[RESPResult]) -> Result<i32, String> { 
    
    if data.len() != 1 {
        return Err("Missing key/value for EXISTS".to_string());
    }

    let mut keys: Vec<String> = Vec::new();
    for key in data {
        match &key {
                RESPResult::BulkString(Some(message)) => keys.push(String::from_utf8(message.clone()).unwrap()),
                _ => return Err("Error: Not bulk string".to_string()),
        }
    }
    
    Ok(db::exists(keys))
}

fn delete_command(data: &[RESPResult]) -> Result<i32, String> { 
    
    if data.len() != 1 {
        return Err("Missing key/value for DEL".to_string());
    }

    let mut keys: Vec<String> = Vec::new();
    for key in data {
        match &key {
                RESPResult::BulkString(Some(message)) => keys.push(String::from_utf8(message.clone()).unwrap()),
                _ => return Err("Error: Not bulk string".to_string()),
        }
    }
    
    Ok(db::delete(keys))
}

fn increment_command(data: &[RESPResult]) -> Result<String, String> { 
    
    if data.len() != 1 {
        return Err("Missing key/value for INCR".to_string());
    }

    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    db::increment(&key)
}

fn decrement_command(data: &[RESPResult]) -> Result<String, String> { 
    
    if data.len() != 1 {
        return Err("Missing key/value for DECR".to_string());
    }

    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    db::decrement(&key)
}

fn lpush_command(data: &[RESPResult]) -> Result<String, String> {
    
    if data.len() == 1 {
        return Err("Missing key/value for LPUSH".to_string());
    }

    // get key
    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    // values to push
    let mut values: Vec<DB_TYPE> = Vec::new();
    for value in &data[1..] {
        match &value {
                RESPResult::BulkString(Some(message)) => {
                    let str_val = String::from_utf8(message.clone()).expect("Not valid string");

                    let v = match str_val.parse::<i64>() {
                        Ok(v) => DB_TYPE::Int(v),
                        Err(_) => DB_TYPE::Str(str_val),
                    };

                    values.push(v);
                },
                _ => return Err("Error: Not bulk string".to_string()),
        }
    };

    match db::lpush(&key, values) {
        Ok(i) => Ok(i.to_string()),
        Err(e) => Err(e)
    }
}

fn rpush_command(data: &[RESPResult]) -> Result<String, String> {
    
    if data.len() == 1 {
        return Err("Missing key/value for RPUSH".to_string());
    }

    // get key
    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    // values to push
    let mut values: Vec<DB_TYPE> = Vec::new();
    for value in &data[1..] {
        match &value {
                RESPResult::BulkString(Some(message)) => {
                    let str_val = String::from_utf8(message.clone()).expect("Not valid string");

                    let v = match str_val.parse::<i64>() {
                        Ok(v) => DB_TYPE::Int(v),
                        Err(_) => DB_TYPE::Str(str_val),
                    };

                    values.push(v);
                },
                _ => return Err("Error: Not bulk string".to_string()),
        }
    };
    
    match db::rpush(&key, values) {
        Ok(i) => Ok(i.to_string()),
        Err(e) => Err(e)
    }
}

pub fn save_command() -> Result<String, String> {
    db::write_db_to_file()
}

pub fn load_command(data: &[RESPResult]) -> Result<String, String> {
    if data.len() != 1 {
        return Err("Error: only provide path when loading: LOAD ./file.rdb".to_string());
    }

    let path = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };

    db::read_db_from_file(&path)
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn bulk(s: &str) -> RESPResult {
        RESPResult::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[test]
    fn test_echo_command_valid() {
        let data: Vec<_> = vec![RESPResult::BulkString(Some(b"hello".to_vec()))];
        let result = echo_command(&data);
        assert_eq!(result, Ok("hello".to_string()));
    }
    #[test]
    fn test_echo_command_invalid_arg_count() {
        let data = vec![]; // no args
        let result = echo_command(&data);
        assert_eq!(result, Err("Incorrect number of arguments for echo".to_string()));
        let data = vec![
            RESPResult::BulkString(Some(b"hello".to_vec())),
            RESPResult::BulkString(Some(b"world".to_vec())),
        ]; // too many args
        let result = echo_command(&data);
        assert_eq!(result, Err("Incorrect number of arguments for echo".to_string()));
    }
    #[test]
    fn test_echo_command_not_bulk_string() {
        let data = vec![RESPResult::Integer(42)];
        let result = echo_command(&data);
        assert_eq!(result, Err("Error: Not bulk string".to_string()));
    }
    #[test]
    fn test_echo_command_invalid_utf8() {
        let data = vec![RESPResult::BulkString(Some(vec![0xff, 0xfe, 0xfd]))]; // Invalid UTF-8
        let result = echo_command(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().starts_with("Invalid UTF-8"));
    }

    #[test]
    fn test_ping_command_valid() {
        let data: Vec<_> = vec![RESPResult::BulkString(Some(b"PING".to_vec()))];
        let result = command_router("PING", &data);
        assert_eq!(result, Ok(RESPResult::SimpleString("PONG".to_string())));
    }

    #[test]
    fn test_set_and_get_command_success() {
        // Set a key
        let set_input = vec![bulk("foo"), bulk("bar")];
        let set_result = set_command(&set_input);
        assert_eq!(set_result, Ok("OK".to_string()));

        // Get the same key
        let get_input = vec![bulk("foo")];
        let get_result = get_command(&get_input);
        assert_eq!(get_result, Ok("bar".to_string()));
    }

    #[test]
    fn test_set_command_missing_args() {
        let input = vec![bulk("foo")]; // only one argument
        let result = set_command(&input);
        assert_eq!(result, Err("Missing key/value for SET".to_string()));
    }

    #[test]
    fn test_ex_expiry() {
        let data = vec![
            bulk("key_ex"),
            bulk("value"),
            bulk("EX"),
            bulk("1"), // 1 second expiry
        ];

        set_command(&data).unwrap();

        // Immediately get should return the value
        let get_result = get_command(&[bulk("key_ex")]).unwrap();
        assert_eq!(get_result, "value");

        // Wait for more than 1 second
        thread::sleep(Duration::from_millis(1100));

        // Should be expired now
        let get_result = get_command(&[bulk("key_ex")]).unwrap();
        assert_eq!(get_result, "");
    }

    #[test]
    fn test_px_expiry() {
        let data = vec![
            bulk("key_px"),
            bulk("value"),
            bulk("PX"),
            bulk("500"), // 0.5 second expiry
        ];

        set_command(&data).unwrap();
        thread::sleep(Duration::from_millis(600)); // Wait for expiry

        let get_result = get_command(&[bulk("key_px")]).unwrap();
        assert_eq!(get_result, "");
    }

    #[test]
    fn test_exat_expiry() {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let expire_at = now_secs + 1; // 1 second in the future

        let data = vec![
            bulk("key_exat"),
            bulk("value"),
            bulk("EXAT"),
            bulk(&expire_at.to_string()),
        ];

        set_command(&data).unwrap();
        thread::sleep(Duration::from_millis(1100));

        let get_result = get_command(&[bulk("key_exat")]).unwrap();
        assert_eq!(get_result, "");
    }

    #[test]
    fn test_pxat_expiry() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let expire_at = now_ms + 500; // expires in 500ms

        let data = vec![
            bulk("key_pxat"),
            bulk("value"),
            bulk("PXAT"),
            bulk(&expire_at.to_string()),
        ];

        set_command(&data).unwrap();
        thread::sleep(Duration::from_millis(600));

        let get_result = get_command(&[bulk("key_pxat")]).unwrap();
        assert_eq!(get_result, "");
    }

    #[test]
    fn test_set_command_invalid_arg() {
        let data = vec![
            RESPResult::BulkString(Some(b"key_invalid".to_vec())),
            RESPResult::BulkString(Some(b"value".to_vec())),
            RESPResult::BulkString(Some(b"BOGUS".to_vec())),
            RESPResult::BulkString(Some(b"9999".to_vec())),
        ];

        let result = set_command(&data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Optional argument not understood");
    }

    #[test]
    fn test_get_command_missing_args() {
        let input = vec![];
        let result = get_command(&input);
        assert_eq!(result, Err("Missing key/value for GET".to_string()));
    }

    #[test]
    fn test_get_command_nonexistent_key() {
        let input = vec![bulk("nonexistent")];
        let result = get_command(&input);
        assert_eq!(result, Ok(String::new())); // returns empty string for missing keys
    }

    #[test]
    fn test_set_command_invalid_type() {
        let input = vec![RESPResult::Integer(42), bulk("bar")];
        let result = set_command(&input);
        assert_eq!(result, Err("Error: Not bulk string".to_string()));
    }

    #[test]
    fn test_get_command_invalid_type() {
        let input = vec![RESPResult::Integer(42)];
        let result = get_command(&input);
        assert_eq!(result, Err("Error: Not bulk string".to_string()));
    }

    
    #[test]
    fn test_exists_command_existing_key() {
        db::set("exists_test".to_string(), DB_TYPE::Str("value".to_string()), 0).unwrap();
        let data = vec![bulk("exists_test")];
        let result = exists_command(&data).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_exists_command_missing_key() {
        let data = vec![bulk("nonexistent_key")];
        let result = exists_command(&data).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_delete_command_existing_key() {
        db::set("delete_test".to_string(), DB_TYPE::Str("value".to_string()), 0).unwrap();
        let data = vec![bulk("delete_test")];
        let result = delete_command(&data).unwrap();
        assert_eq!(result, 1);

        // Confirm deletion
        let exists = db::exists(vec!["delete_test".to_string()]);
        assert_eq!(exists, 0);
    }

    #[test]
    fn test_delete_command_missing_key() {
        let data = vec![bulk("nonexistent")];
        let result = delete_command(&data).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_increment_command_initial_value() {
        let data = vec![bulk("counter_test")];
        let result = increment_command(&data).unwrap();
        assert_eq!(result, "OK");

        let result2 = increment_command(&data).unwrap();
        assert_eq!(result2, "OK");

        let result3 = get_command(&data);
        assert_eq!(result3, Ok("2".to_string()));
    }

    #[test]
    fn test_decrement_command_initial_value() {
        let data = vec![bulk("dec_test")];
        let result = decrement_command(&data).unwrap();
        assert_eq!(result, "OK");

        let result2 = decrement_command(&data).unwrap();
        assert_eq!(result2, "OK");

        let result3 = get_command(&data);
        assert_eq!(result3, Ok("0".to_string()));
    }

    #[test]
    fn test_increment_after_set() {
        db::set("num_key".to_string(), DB_TYPE::Int(5), 0).unwrap();
        let data = vec![bulk("num_key")];
        let result = increment_command(&data).unwrap();
        assert_eq!(result, "OK");

        let result3 = get_command(&data);
        assert_eq!(result3, Ok("6".to_string()));
    }

    #[test]
    fn test_decrement_after_set() {
        db::set("dec_key".to_string(), DB_TYPE::Int(10), 0).unwrap();
        let data = vec![bulk("dec_key")];
        let result = decrement_command(&data).unwrap();
        println!("{:?}", result);
        assert_eq!(result, "OK");

        let result2 = get_command(&data);
        println!("{:?}", result2);
        assert_eq!(result2, Ok("9".to_string()));
    }

    #[test]
    fn test_increment_invalid_data() {
        db::set("bad_data".to_string(), DB_TYPE::Str("value".to_string()), 0).unwrap();
        let data = vec![bulk("bad_data")];
        let result = increment_command(&data);
        assert!(result.is_err());
    }

    
    #[test]
    fn test_lpush_valid_int_and_string() {
        let input = vec![
            bulk("mylist"),
            bulk("123"),
            bulk("hello"),
        ];

        let result = lpush_command(&input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "2");

    let stored = db::get("mylist").unwrap();

    match stored {
        DB_TYPE::Array(ref items) => {
            assert_eq!(items, &vec![
                DB_TYPE::Str("hello".to_string()),
                DB_TYPE::Int(123)
                ]);
            },
        _ => panic!("Expected DB_TYPE::Array"),
        }
    }

    #[test]
    fn test_lpush_missing_value() {
        let input = vec![
            bulk("mylist"),
        ];
        let result = lpush_command(&input);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Missing key/value for LPUSH");
    }

    #[test]
    fn test_lpush_invalid_type() {
        let input = vec![
            RESPResult::BulkString(None), // invalid key
            bulk("hello"),
        ];
        let result = lpush_command(&input);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Error: Not bulk string");
    }

    #[test]
    fn test_rpush_valid_mixed_types() {
        let input = vec![
            bulk("mylist1"),
            bulk("123"),
            bulk("hello"),
        ];

        let result = rpush_command(&input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "2");

        let stored = db::get("mylist1").unwrap();
        match stored {
            DB_TYPE::Array(ref items) => {
                assert_eq!(items, &vec![
                    DB_TYPE::Int(123),
                    DB_TYPE::Str("hello".to_string()),
                ]);
            }
            _ => panic!("Expected DB_TYPE::Array"),
        }
    }

    #[test]
    fn test_rpush_missing_values() {
        let input = vec![
            bulk("mylist"),
        ];

        let result = rpush_command(&input);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Missing key/value for RPUSH");
    }

    #[test]
    fn test_rpush_invalid_key_type() {
        let input = vec![
            RESPResult::BulkString(None),
            bulk("hello"),
        ];

        let result = rpush_command(&input);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Error: Not bulk string");
    }

    #[test]
    fn test_rpush_multiple_calls_appends_right() {
        let input1 = vec![bulk("mylist2"), bulk("a")];
        let input2 = vec![bulk("mylist2"), bulk("b"), bulk("c")];

        let _ = rpush_command(&input1);
        let _ = rpush_command(&input2);

        let stored = db::get("mylist2").unwrap();
        match stored {
            DB_TYPE::Array(ref items) => {
                assert_eq!(items, &vec![
                    DB_TYPE::Str("a".to_string()),
                    DB_TYPE::Str("b".to_string()),
                    DB_TYPE::Str("c".to_string()),
                ]);
            }
            _ => panic!("Expected DB_TYPE::Array"),
        }
    }
}