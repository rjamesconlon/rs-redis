#[cfg(test)] 
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use std::time::Duration;
    use rs_redis::network;
    use std::fs;

    #[tokio::test]
    async fn test_simple_set_and_get() {
        // Start server
        tokio::spawn(async {
            network::start_network().await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let (mut reader, mut writer) = stream.into_split();

        // Send SET command: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let msg = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        writer.write_all(msg).await.unwrap();

        let mut response = [0u8; 64];
        let n = reader.read(&mut response).await.unwrap();

        let reply = std::str::from_utf8(&response[..n]).unwrap();
        assert!(reply.contains("+OK"));

        // Now send GET command
        let msg = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        writer.write_all(msg).await.unwrap();

        let mut response = [0u8; 64];
        let n = reader.read(&mut response).await.unwrap();

        let reply = std::str::from_utf8(&response[..n]).unwrap();
        assert!(reply.contains("bar"));
    }

    #[tokio::test]
    async fn test_save_creates_rdb_file() {
        // Start server
        tokio::spawn(async {
            rs_redis::network::start_network().await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let (mut reader, mut writer) = stream.into_split();

        // Insert a String value
        writer.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$5\r\nhello\r\n").await.unwrap();
        reader.read(&mut [0u8; 64]).await.unwrap();

        // Insert an Int value
        writer.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nint\r\n$2\r\n42\r\n").await.unwrap();
        reader.read(&mut [0u8; 64]).await.unwrap();


        // Insert an array using LPUSH
        writer.write_all(b"*3\r\n$5\r\nLPUSH\r\n$5\r\narray\r\n$1\r\n1\r\n").await.unwrap();
        reader.read(&mut [0u8; 64]).await.unwrap();

        writer.write_all(b"*3\r\n$5\r\nLPUSH\r\n$5\r\narray\r\n$1\r\n2\r\n").await.unwrap();
        reader.read(&mut [0u8; 64]).await.unwrap();

        writer.write_all(b"*3\r\n$5\r\nLPUSH\r\n$5\r\narray\r\n$1\r\n3\r\n").await.unwrap();
        reader.read(&mut [0u8; 64]).await.unwrap();

        writer.write_all(b"*3\r\n$5\r\nLPUSH\r\n$5\r\narray\r\n$1\r\na\r\n").await.unwrap();
        reader.read(&mut [0u8; 64]).await.unwrap();

        // Send the SAVE command
        writer.write_all(b"*1\r\n$4\r\nSAVE\r\n").await.unwrap();

        let mut response = [0u8; 64];
        let n = reader.read(&mut response).await.unwrap();
        let reply = std::str::from_utf8(&response[..n]).unwrap();

        assert!(reply.contains("+OK"));

        // Give the server a moment to write the file
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check that the file exists
        let metadata = fs::metadata("REDIS.rdb").expect("REDIS.rdb file should exist");
        assert!(metadata.is_file());
        assert!(metadata.len() > 0);
    }
}