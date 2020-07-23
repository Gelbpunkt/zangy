use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr_iter;

const NULL_BULK_STRING: &[u8] = b"$-1\r\n";
const NULL_ARRAY: &[u8] = b"*-1\r\n";
const CRLF: &[u8] = b"\r\n";

pub enum ParserError {
    UnknownStartingByte,
    Eof,
}

#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(Bytes),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RedisType>),
    NullBulkString,
    NullArray,
}

fn simple_string(data: &mut BytesMut) -> Result<(&mut BytesMut, BytesMut), ParserError> {
    // We know that [0] is a +
    // Search for the first occurence of \r\n
    let mut actual_index: usize = 0;
    let data_len = data.len();
    for occ in memchr_iter(b'\r', data) {
        if data_len < occ + 2 {
            return Err(ParserError::Eof);
        }
        if data[occ + 1] == b'\n' {
            actual_index = occ;
            break;
        }
    }
    if actual_index == 0 {
        return Err(ParserError::Eof);
    }
    data.advance(1);
    let actual = data.split_to(actual_index - 1);
    // Skip the \r\n
    data.advance(2);

    Ok((data, actual))
}

fn error(data: &mut BytesMut) -> Result<(&mut BytesMut, BytesMut), ParserError> {
    // We know that [0] is a -
    // Search for the first occurence of \r\n
    let mut actual_index: usize = 0;
    let data_len = data.len();
    for occ in memchr_iter(b'\r', data) {
        if data_len < occ + 2 {
            return Err(ParserError::Eof);
        }
        if data[occ + 1] == b'\n' {
            actual_index = occ;
            break;
        }
    }
    if actual_index == 0 {
        return Err(ParserError::Eof);
    }
    data.advance(1);
    let actual = data.split_to(actual_index - 1);
    // Skip the \r\n
    data.advance(2);

    Ok((data, actual))
}

fn int(data: &mut BytesMut) -> Result<(&mut BytesMut, i64), ParserError> {
    // We know that [0] is a :
    // Search for the first occurence of \r\n
    let mut actual_index: usize = 0;
    let data_len = data.len();
    for occ in memchr_iter(b'\r', data) {
        if data_len < occ + 2 {
            return Err(ParserError::Eof);
        }
        if data[occ + 1] == b'\n' {
            actual_index = occ;
            break;
        }
    }
    if actual_index == 0 {
        return Err(ParserError::Eof);
    }
    data.advance(1);
    let actual_data = data.split_to(actual_index - 1);
    let int_str = String::from_utf8(actual_data.to_vec()).unwrap();
    let actual_int = int_str.parse::<i64>().unwrap();
    // Skip the \r\n
    data.advance(2);

    Ok((data, actual_int))
}

fn bulk_string(data: &mut BytesMut) -> Result<(&mut BytesMut, BytesMut), ParserError> {
    // We know that [0] is a $
    // Search for the first occurence of \r\n
    let mut actual_index: usize = 0;
    let data_len = data.len();
    for occ in memchr_iter(b'\r', data) {
        if data_len < occ + 2 {
            return Err(ParserError::Eof);
        }
        if data[occ + 1] == b'\n' {
            actual_index = occ;
            break;
        }
    }
    if actual_index == 0 {
        return Err(ParserError::Eof);
    }
    // Skip the $
    data.advance(1);
    // Read the number of upcoming bytes
    let actual_data = data.split_to(actual_index - 1);
    let int_str = String::from_utf8(actual_data.to_vec()).unwrap();
    let actual_int = int_str.parse::<usize>().unwrap();
    // Skip the \r\n
    data.advance(2);
    // Split to those bytes
    let real_data = data.split_to(actual_int);
    if data.len() < 2 {
        return Err(ParserError::Eof);
    }
    let crlf = data.split_to(2);
    if crlf != CRLF {
        return Err(ParserError::Eof);
    }

    Ok((data, real_data))
}

fn array(data: &mut BytesMut) -> Result<(&mut BytesMut, Vec<RedisType>), ParserError> {
    // We know that [0] is a *
    // Search for the first occurence of \r\n
    let mut actual_index: usize = 0;
    let data_len = data.len();
    for occ in memchr_iter(b'\r', data) {
        if data_len < occ + 2 {
            return Err(ParserError::Eof);
        }
        if data[occ + 1] == b'\n' {
            actual_index = occ;
            break;
        }
    }
    if actual_index == 0 {
        return Err(ParserError::Eof);
    }
    // Skip the *
    data.advance(1);
    // Read the number of upcoming elements
    let actual_data = data.split_to(actual_index - 1);
    let int_str = String::from_utf8(actual_data.to_vec()).unwrap();
    let actual_int = int_str.parse::<i64>().unwrap();
    // Skip the \r\n
    data.advance(2);
    let mut elems = Vec::new();
    for _ in 0..actual_int {
        let elem = parse(data)?;
        elems.push(elem);
    }

    Ok((data, elems))
}

pub fn parse(data: &mut BytesMut) -> Result<RedisType, ParserError> {
    if data == NULL_BULK_STRING {
        Ok(RedisType::NullBulkString)
    } else if data == NULL_ARRAY {
        Ok(RedisType::NullArray)
    } else {
        match data[0] {
            b'+' => match simple_string(data) {
                Ok(res) => {
                    let obj = RedisType::SimpleString(res.1.clone().freeze());
                    Ok(obj)
                }
                Err(e) => Err(e),
            },
            b'-' => match error(data) {
                Ok(res) => {
                    let obj = RedisType::Error(String::from_utf8((&res.1).to_vec()).unwrap());
                    Ok(obj)
                }
                Err(e) => Err(e),
            },
            b':' => match int(data) {
                Ok(res) => {
                    let obj = RedisType::Integer(res.1);
                    Ok(obj)
                }
                Err(e) => Err(e),
            },
            b'$' => match bulk_string(data) {
                Ok(res) => {
                    let obj = RedisType::BulkString(res.1.clone().freeze());
                    Ok(obj)
                }
                Err(e) => Err(e),
            },
            b'*' => match array(data) {
                Ok(res) => {
                    let obj = RedisType::Array(res.1);
                    Ok(obj)
                }
                Err(e) => Err(e),
            },
            _ => Err(ParserError::UnknownStartingByte),
        }
    }
}
