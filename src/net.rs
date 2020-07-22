use async_std::net::TcpStream;
use async_std::prelude::*;
use std::io;
use std::net::Shutdown;

#[derive(Clone)]
pub struct RedisConnection {
    connection: TcpStream,
}

impl RedisConnection {
    pub async fn from_address(address: &str) -> Result<RedisConnection, io::Error> {
        let stream = TcpStream::connect(address).await?;
        Ok(RedisConnection { connection: stream })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), io::Error> {
        self.connection.write_all(data).await?;

        Ok(())
    }

    async fn recv(&mut self, bytes: u8) -> Result<Vec<u8>, io::Error> {
        let mut buf = vec![0u8, bytes];
        self.connection.read(&mut buf).await?;

        Ok(buf)
    }

    pub fn disconnect(&self) -> Result<(), io::Error> {
        self.connection.shutdown(Shutdown::Both)?;

        Ok(())
    }

    pub async fn execute(&mut self, data: &[u8]) -> Result<Vec<u8>, io::Error> {
        self.send(data).await?;

        Ok(self.recv(255).await?)
    }
}
