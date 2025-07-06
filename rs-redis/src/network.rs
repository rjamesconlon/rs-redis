use crate::{command, parser, network};
use crate::types::RESPResult;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};


pub async fn start_network() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = process_stream(socket).await {
                eprintln!("Error handling connection: {:?}", e);
            }
        });
    }

    // technically unreachable
    // Ok(())
}

async fn process_stream(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    // split the socket into read/write


    let (reader, mut writer) = socket.split();

    let mut reader = BufReader::new(reader);
    let mut buffer = String::new();

    loop {
        buffer.clear();

        // Try to read a full RESP message line (e.g., "*3\r\n")
        let bytes_read = reader.read_line(&mut buffer).await?;

        if bytes_read == 0 {
            // client closed connection
            break;
        }

        if buffer.starts_with('*') {
            let num_elements: usize = buffer[1..].trim().parse().unwrap();
            let mut command_parts = Vec::with_capacity(num_elements);     

            for _ in 0..num_elements {
                buffer.clear();
                reader.read_line(&mut buffer).await?; 
                let bulk_len: usize = buffer[1..].trim().parse().unwrap();

                let mut bulk = vec![0u8; bulk_len + 2]; 
                reader.read_exact(&mut bulk).await?;

                let clean = String::from_utf8_lossy(&bulk[..bulk_len]).to_string();
                command_parts.push(clean);
            }
            
            let response = match network::read_network_input(command_parts) {
                Ok(res) => res,
                Err(e) => format!("Error: {0}", e).as_bytes().to_vec()
            };

            writer.write_all(&response).await?;
        }
    }

    Ok(())
}

pub fn read_network_input(commands: Vec<String>) -> Result<Vec<u8>, String> {
    // check that commands is an array, and above len 0
    if commands.len() == 0 {
        return Err("Empty array".to_string());
    };

    let mut arguments = Vec::<RESPResult>::new();

    for args in &commands[1..] {
        arguments.push(RESPResult::BulkString(Some(args.as_bytes().to_vec())));
    }

    let result = match command::command_router(&commands[0], &arguments) {
        Ok(m) => m,
        Err(e) => return Err(e),
    };

    Ok(parser::respresult_to_resp_string(&result).unwrap().as_bytes().to_vec())
}