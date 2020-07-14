// https://github.com/dpbriggs/redis-oxide/blob/master/src/asyncresp.rs
use bytes::{Bytes, BytesMut};
use memchr::memchr;
use std::convert::From;
use std::io;
use std::str;

/// These types are used by state and ops to actually perform useful work.
pub type Value = Vec<u8>;
/// Key is the standard type to index our structures
pub type Key = Vec<u8>;

pub const NULL_ARRAY: &str = "*-1\r\n";
pub const NULL_BULK_STRING: &str = "$-1\r\n";

/// RedisValueRef is the canonical type for values flowing
/// through the system. Inputs are converted into RedisValues,
/// and outputs are converted into RedisValues.
#[derive(PartialEq, Clone)]
pub enum RedisValueRef {
    BulkString(Bytes),
    SimpleString(Bytes),
    Error(Bytes),
    ErrorMsg(Vec<u8>),
    Int(i64),
    Array(Vec<RedisValueRef>),
    NullArray,
    NullBulkString,
}

impl std::fmt::Debug for RedisValueRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValueRef::BulkString(s) => write!(
                f,
                "RedisValueRef::BulkString({:?})",
                String::from_utf8_lossy(s)
            ),
            RedisValueRef::SimpleString(s) => write!(
                f,
                "RedisValueRef::SimpleString({:?})",
                String::from_utf8_lossy(s)
            ),
            RedisValueRef::Error(s) => {
                write!(f, "RedisValueRef::Error({:?})", String::from_utf8_lossy(s))
            }
            RedisValueRef::ErrorMsg(s) => write!(f, "RedisValueRef::ErrorMsg({:?})", s),

            RedisValueRef::Int(i) => write!(f, "RedisValueRef::Int({:?})", i),
            RedisValueRef::NullBulkString => write!(f, "RedisValueRef::NullBulkString"),
            RedisValueRef::NullArray => write!(f, "RedisValueRef::NullArray"),
            RedisValueRef::Array(arr) => {
                write!(f, "RedisValueRef::Array(")?;
                for item in arr {
                    write!(f, "{:?}", item)?;
                    write!(f, ",")?;
                }
                write!(f, ")")?;
                Ok(())
            }
        }
    }
}

/// Fundamental struct for viewing byte slices
///
/// Used for zero-copy redis values.
struct BufSplit(usize, usize);

impl BufSplit {
    /// Get a lifetime appropriate slice of the underlying buffer.
    ///
    /// Constant time.
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    /// Get a Bytes object representing the appropriate slice
    /// of bytes.
    ///
    /// Constant time.
    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

/// BufSplit based equivalent to our output type RedisValueRef
enum RedisBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Int(i64),
    Array(Vec<RedisBufSplit>),
    NullArray,
    NullBulkString,
}

impl RedisBufSplit {
    fn redis_value(self, buf: &Bytes) -> RedisValueRef {
        match self {
            RedisBufSplit::String(bfs) => RedisValueRef::BulkString(bfs.as_bytes(buf)),
            RedisBufSplit::Error(bfs) => RedisValueRef::Error(bfs.as_bytes(buf)),
            RedisBufSplit::Array(arr) => {
                RedisValueRef::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect())
            }
            RedisBufSplit::NullArray => RedisValueRef::NullArray,
            RedisBufSplit::NullBulkString => RedisValueRef::NullBulkString,
            RedisBufSplit::Int(i) => RedisValueRef::Int(i),
        }
    }
}

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
}

impl From<std::io::Error> for RESPError {
    fn from(e: std::io::Error) -> RESPError {
        RESPError::IOError(e)
    }
}

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;

#[inline]
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }
    memchr(b'\r', &buf[pos..]).and_then(|end| {
        if end + 1 < buf.len() {
            Some((pos + end + 2, BufSplit(pos, pos + end)))
        } else {
            None
        }
    })
}

fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullBulkString))),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                let bb = RedisBufSplit::String(BufSplit(pos, total_size));
                Ok(Some((total_size + 2, bb)))
            }
        }
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::String(word))))
}

fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::Error(word))))
}

fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(int(buf, pos)?.map(|(pos, int)| (pos, RedisBufSplit::Int(int))))
}

fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        None => Ok(None),
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullArray))),
        Some((pos, num_elements)) if num_elements >= 0 => {
            let mut values = Vec::with_capacity(num_elements as usize);
            let mut curr_pos = pos;
            for _ in 0..num_elements {
                match parse(buf, curr_pos)? {
                    Some((new_pos, value)) => {
                        curr_pos = new_pos;
                        values.push(value);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((curr_pos, RedisBufSplit::Array(values))))
        }
        Some((_pos, bad_num_elements)) => Err(RESPError::BadArraySize(bad_num_elements)),
    }
}

fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b':' => resp_int(buf, pos + 1),
        b'*' => array(buf, pos + 1),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

/// The struct we're using. We don't need to store anything in the struct.
/// Later on we can expand this struct for optimization purposes.
#[derive(Default)]
pub struct RespParser;

impl RespParser {
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<RedisValueRef>, RESPError> {
        if buf.is_empty() {
            return Ok(None);
        }

        match parse(buf, 0)? {
            Some((pos, value)) => {
                // We parsed a value! Shave off the bytes so tokio can continue filling the buffer.
                let our_data = buf.split_to(pos);
                // Use `redis_value` defined above to get the correct type
                Ok(Some(value.redis_value(&our_data.freeze())))
            }
            None => Ok(None),
        }
    }

    pub fn encode(&mut self, item: RedisValueRef, dst: &mut BytesMut) -> io::Result<()> {
        write_redis_value(item, dst);
        Ok(())
    }
}

fn write_redis_value(item: RedisValueRef, dst: &mut BytesMut) {
    match item {
        RedisValueRef::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::ErrorMsg(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::BulkString(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::Array(array) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(array.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for redis_value in array {
                write_redis_value(redis_value, dst);
            }
        }
        RedisValueRef::Int(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::NullArray => dst.extend_from_slice(NULL_ARRAY.as_bytes()),
        RedisValueRef::NullBulkString => dst.extend_from_slice(NULL_BULK_STRING.as_bytes()),
    }
}
