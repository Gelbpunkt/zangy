use async_std::net::TcpStream;
use async_std::prelude::*;
use std::io;
use std::net::Shutdown;

struct RedisConnection {
    connection: TcpStream,
}

impl RedisConnection {
    async fn from_address(address: String) -> Result<RedisConnection, io::Error> {
        let stream = TcpStream::connect(address).await?;
        Ok(RedisConnection { connection: stream })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), io::Error> {
        self.connection.write_all(data).await?;

        Ok(())
    }

    fn disconnect(&self) -> Result<(), io::Error> {
        self.connection.shutdown(Shutdown::Both)?;

        Ok(())
    }
}
