use crate::command;
use crate::parser;
use crate::parser::parse_resp_message;
use crate::types::RESPResult;

pub fn read_cli_input(message: &str) -> Result<Vec<u8>, String> {

    // get the resp format of the command
    let resp_message: Vec<u8> = match parser::string_to_resp_message(message) {
        Ok(s) => s,
        Err(e) => return Err(e)
    };

    // get the result of the resp format message
    let message_result = match parse_resp_message(&resp_message) {
        Ok(result   ) => result.0,
        Err(e) => return Err(e)
    };

    // check that message result is an array, and above len 0
    let command_values = match message_result {
        RESPResult::Array(a) => {
            if a.len() == 0 {
                return Err("Empty array".to_string());
            }
            a
        },
        _ => return Err("Err".to_string())
    };

    // get the command from the array
    // make sure it is a bulk string
    let command: String = match &command_values[0] {
        RESPResult::BulkString(Some(message)) => match String::from_utf8(message.clone())
        {
            Ok(s) => s,
            Err(e) => return Err(e.to_string())
        },
        _ => return Err("Error: Not bulk string".to_string()),
    };

    let arguments = &command_values[1..];

    let result = match command::command_router(&command, arguments) {
        Ok(m) => m,
        Err(e) => return Err(e),
    };

    Ok(parser::respresult_to_resp_string(&result).unwrap().as_bytes().to_vec())
}