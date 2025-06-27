#[derive(Debug, PartialEq)]
pub enum RESPResult {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Vec<RESPResult>),
}