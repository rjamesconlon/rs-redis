use crate::types::RESPResult;
use crate::db::REDIS_DB;

pub fn command_router(command: &str, data: &[RESPResult]) -> Result<String, String> {
    match command {
        "ECHO" =>  echo_command(data),
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

fn set_command(key: &String, value: &String) -> RESPResult {
    let mut db = REDIS_DB.lock().unwrap();
    db.insert(String::from(key), value.as_bytes().to_vec());
    return RESPResult::SimpleString(String::from("OK"))
}

// fn get_command(key: &String) -> RESPResult {
//     let mut db = REDIS_DB.lock().unwrap();
//     let value: Option<&Vec<u8>> = db.get(key);
    
//     return RESPResult::SimpleString(String::from("OK"))
// }