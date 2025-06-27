use crate::{parser};
use crate::{types};

#[test]
fn test_parse_simple_string() {
    let input = b"+PONG\r\n";
    let (resp, _) = parser::parse_resp_message(input).expect("Parsing failed");
    assert_eq!(resp, types::RESPResult::SimpleString("PONG".to_string()));
}

#[test]
fn test_parse_simple_string_bytes() {
    let input = b"+PONG\r\n";
    let (resp, bytes) = parser::parse_resp_message(input).expect("Parsing failed");
    assert_eq!(
        (resp, bytes),
        (types::RESPResult::SimpleString("PONG".to_string()), 7 as usize)
    );
}

#[test]
fn test_parse_bulk_string() {
    let input = b"$5\r\nhello\r\n";
    let (resp, _) = parser::parse_resp_message(input).expect("Parsing failed");
    assert_eq!(resp, types::RESPResult::BulkString(Some(b"hello".to_vec())));
}

#[test]
fn test_parse_null_bulk_string() {
    let input = b"$-1\r\n";
    let (resp, _) = parser::parse_resp_message(input).expect("Parsing failed");
    assert_eq!(resp, types::RESPResult::BulkString(None));
}