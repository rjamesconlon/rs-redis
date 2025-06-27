use std::vec::Vec;
use shell_words;
use crate::types::RESPResult;

pub fn string_to_resp_message(message: &str) -> Result<Vec<u8>, String> {
    let mut msg_str: Vec<String>;

    match shell_words::split(message) {
        Ok(message) => msg_str = message,
        Err(e) => return Err(e.to_string())
    }

    
    println!("string_to_resp_message: {:?}", msg_str);

    let mut resp_string = String::from("*") + &msg_str.len().to_string() + "\r\n";

    for word in msg_str {
        resp_string += &format!("${}\r\n{}\r\n", &word.len(), word);
    }

    Ok(resp_string.into_bytes())
}

pub fn resp_message_to_string(respmessage: &RESPResult) -> String {
    match respmessage {
        RESPResult::SimpleString(s) => s.clone(),
        RESPResult::Error(e) => format!("(error) {}", e),
        RESPResult::Integer(i) => i.to_string(),
        RESPResult::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        RESPResult::BulkString(None) => "(nil)".to_string(),
        RESPResult::Array(elements) => {
            elements
                .iter()
                .enumerate()
                .map(|(i, elem)| format!("{}) {}", i + 1, resp_message_to_string(elem)))
                .collect::<Vec<String>>()
                .join("\n")
        },
    }
}

// parse RESP message
pub fn parse_resp_message(message: &[u8]) ->  Result<(RESPResult, usize), String> {

    println!("parse_resp_message - parsing: {:?}", String::from_utf8_lossy(&message));

    match message.first() {
        Some(b'+') => parse_simple_string(message),
        Some(b'-') => parse_error_string(message),
        Some(b':') => parse_integer_string(message),
        Some(b'$') => parse_bulk_string(message),
        Some(b'*') => parse_array(message),
        _ => Err("Invalid or empty message".to_string()),
        
    }
}

fn parse_simple_string(message: &[u8]) -> Result<(RESPResult, usize), String> {
    
    // get end of simple string
    let pos = message.windows( 2).position(|window: &[u8]| window == b"\r\n").unwrap();

    let bytes: &[u8] = &message[1..pos];

    println!("parse_simple_string: {:?}", bytes);

    Ok((
        RESPResult::SimpleString(String::from_utf8_lossy(bytes).to_string()),
        pos + 2, // Total bytes consumed
    ))
}

fn parse_error_string(message: &[u8]) -> Result<(RESPResult, usize), String>  {
    
    // get end of error string
    let pos = message.windows( 2).position(|window: &[u8]| window == b"\r\n").unwrap();

    let bytes: &[u8] = &message[1..pos];

    println!("parse_error_string: {:?}", bytes);

    Ok((
        RESPResult::Error(String::from_utf8_lossy(bytes).to_string()),
        pos + 2, // Total bytes consumed
    ))
}

fn parse_integer_string(message: &[u8]) -> Result<(RESPResult, usize), String>  {
    
    // get end of integer string
    let pos = message.windows( 2).position(|window: &[u8]| window == b"\r\n").unwrap();

    let bytes: &[u8] = &message[1..pos];

    println!("parse_integer_string: {:?}", bytes);

    let integer: i64 = match String::from_utf8_lossy(bytes).parse() {
        Ok(val) => val,
        Err(_) => 0,
    };

    Ok((
        RESPResult::Integer(integer),
        pos + 2, // Total bytes consumed
    ))
}

fn parse_bulk_string(message: &[u8]) -> Result<(RESPResult, usize), String> {
    // get end of bulk string
    let pos = message.windows( 2).position(|window: &[u8]| window == b"\r\n").unwrap();

    let bytes: &[u8] = &message[1..pos];

    let byte_str = String::from_utf8_lossy(bytes);
    let len: isize = byte_str.parse().unwrap();

    if len == -1 {
        return Ok((
            RESPResult::BulkString(None),
            pos + 2, // Total bytes consumed
        ))
    }

    let no_of_bytes_to_read: usize = len as usize + pos + 2;

    let bulk_string_bytes = &message[pos + 2..no_of_bytes_to_read];

    let bulk_string = bulk_string_bytes.to_vec();


    Ok((
        RESPResult::BulkString(Some(bulk_string)),
        no_of_bytes_to_read + 2, // Total bytes consumed
    ))
}

fn parse_array(message: &[u8]) -> Result<(RESPResult, usize), String> {
    println!("parse_array: {:?}", message);
    let mut elements: Vec<RESPResult> = Vec::new();
    
    let mut pos: usize = message.windows( 2).position(|window: &[u8]| window == b"\r\n").unwrap();

    let bytes: &[u8] = &message[1..pos];
    let byte_str = String::from_utf8_lossy(bytes);

    pos += 2;

    let mut len: isize = byte_str.parse().unwrap();

    while len > 0 {
        match parse_resp_message(&message[pos..]) {
            Ok((element, bytes_consumed)) => {
                elements.push(element);
                pos += bytes_consumed;
                len -= 1;
            }
            Err(e) => return Err(e),
        }
    }

    Ok((
        RESPResult::Array(elements),
        pos + 2, // Total bytes consumed
    ))
} 

pub fn bulk_string_to_string(message: &[u8]) {

}