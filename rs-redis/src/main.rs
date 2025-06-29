use rs_redis::cli;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        // Spawn a new task for the connection
        tokio::spawn(async move {
            if let Err(e) = process(socket).await {
                //println!("Error handling connection: {:?}", e);
            }
        });
    }
}

// Function to process each TCP stream
async fn process(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = [0u8; 4096];

    loop {
        let bytes_read = socket.read(&mut buffer).await?;

        if bytes_read == 0 {
            // Connection closed
            break;
        }

        // Print received data
        let input = String::from_utf8_lossy(&buffer[..bytes_read]);
        //println!("Received: {}", input);

        let result: Vec<u8> = match cli::read_cli_input(&input) {
            Ok(s) => s,
            Err(e) => return Err(e.into()),
        };

        // Echo it back
        socket.write_all(&result).await?;
    }

    Ok(())
}