use crate::types::RESPResult;
use crate::db::REDIS_DB;

pub fn command_router(command: &str, data: &[RESPResult]) -> Result<String, String> {
    match command {
        "ECHO" =>  echo_command(data),
        "SET" => set_command(data),
        "GET" => get_command(data),
        _ => Err("panic".to_string())
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
    if data.len() != 2 {
        return Err("Missing key/value for SET".to_string());
    }

    let key = match &data[0] {
        RESPResult::BulkString(Some(message)) => String::from_utf8(message.clone()).unwrap(),
        _ => return Err("Error: Not bulk string".to_string()),
    };
    
    let value = match &data[1] {
        RESPResult::BulkString(Some(message)) => message.clone(),
        _ => return Err("Error: Not bulk string".to_string()),
    };

    let mut db = REDIS_DB.lock().unwrap();
    db.insert(key.clone(), value.clone());
    
    match String::from_utf8(value) {
        Ok(s) => Ok(s),
        Err(e) => Err("Error retrieving set string".to_string())
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
    
    let db = REDIS_DB.lock().unwrap();
    let value: Option<&Vec<u8>> = db.get(&key);
    
    match value {
        Some(val) => Ok(String::from_utf8(val.clone()).unwrap()),
        None => Ok(String::new())
    }
}