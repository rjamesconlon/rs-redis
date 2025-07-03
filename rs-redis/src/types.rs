#[derive(Debug, PartialEq)]
pub enum RESPResult {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Vec<RESPResult>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum DB_TYPE {
    Int(i64),
    Str(String),
    Array(Vec<DB_TYPE>)
}

