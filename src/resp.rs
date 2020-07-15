use bytes::{Buf, Bytes};
use memchr::memchr;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::str;

create_exception!(zangy, ProtocolError, pyo3::exceptions::Exception);
create_exception!(zangy, RedisError, pyo3::exceptions::Exception);

#[derive(Debug)]
struct RustRedisError {
    error_message: String,
}

#[derive(Debug)]
pub enum RedisType {
    SimpleString(Bytes),
    Error(RustRedisError),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RedisType>>),
    ParserError(String),
}

const NULL_BULK_STRING: &[u8] = b"$-1\r\n";
const EMPTY_ARRAY: &[u8] = b"*0\r\n";
const NULL_ARRAY: &[u8] = b"*-1\r\n";

fn parse_simple_string(data: Bytes) -> Bytes {
    // Simple Strings are encoded in the following way:
    // a plus character, followed by a string that cannot contain a CR or LF character (no newlines are allowed),
    // terminated by CRLF (that is "\r\n").
    //
    // b"+OK\r\n" -> b"OK"
    data.slice(1..data.len() - 2)
}

fn parse_error(data: Bytes) -> String {
    // RESP has a specific data type for errors.
    // Actually errors are exactly like RESP Simple Strings,
    // but the first character is a minus '-' character instead of a plus.
    // The real difference between Simple Strings and Errors in RESP is that errors
    // are treated by clients as exceptions, and the string that composes the Error type is the error message itself.
    //
    // b"-Error message\r\n" -> RedisError { error_message: "Error message" }
    let message = parse_simple_string(data);
    String::from_utf8(message.to_vec()).unwrap_or("".to_string())
}

fn parse_integer(data: Bytes) -> i64 {
    // This type is just a CRLF terminated string representing an integer,
    // prefixed by a ":" byte. For example ":0\r\n", or ":1000\r\n" are integer replies.
    //
    // b":1581958\r\n" -> 1581958_i64
    let parsed_string = parse_simple_string(data);
    let int_string = str::from_utf8(&parsed_string).unwrap();
    int_string.parse::<i64>().unwrap()
}

fn parse_bulk_string(data: Bytes) -> Option<Bytes> {
    // Bulk Strings are used in order to represent a single binary safe string up to 512 MB in length.
    // Bulk Strings are encoded in the following way:
    // A "$" byte followed by the number of bytes composing the string (a prefixed length), terminated by CRLF.
    // The actual string data.
    // A final CRLF.
    // So the string "foobar" is encoded as follows:
    // "$6\r\nfoobar\r\n"
    // When an empty string is just:
    // "$0\r\n\r\n"
    // RESP Bulk Strings can also be used in order to signal non-existence of a value
    // using a special format that is used to represent a Null value. In this special format the length is -1,
    // and there is no data, so a Null is represented as:
    // "$-1\r\n"
    if data == NULL_BULK_STRING {
        None
    } else {
        let first_nl = memchr(b'\n', &data).unwrap();
        let actual_str = data.slice(first_nl + 1..data.len() - 2);
        Some(actual_str)
    }
}

fn parse_array_rust(mut data: Bytes) -> Option<Vec<RedisType>> {
    // Arrays are starting with a *, follow by the number of elements and those
    // on seperate lines
    // Empty array: "*0\r\n"
    // Two strings: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    // We could use an array but ignoring the length should be faster
    //
    // The current impl is not ideal
    // *2\r\n:1234\r\n5678\r\n gets parsed properly to [1234, 5678]
    // but anything with \r\n in the actual element does not work
    // should probably get all until \n[+|-|:|$|*] instead of the next \n
    if data == EMPTY_ARRAY {
        Some(Vec::new())
    } else if data == NULL_ARRAY {
        None
    } else {
        let mut res = Vec::new();
        let first_nl = memchr(b'\n', &data).unwrap();
        // Remove beginning
        data.advance(first_nl + 1);
        while let Some(next_nl) = memchr(b'\n', &data) {
            let element = data.split_to(next_nl + 1);
            let new_element = parse(element);
            res.push(new_element);
        }
        Some(res)
    }
}

fn parse_array_py(mut data: Bytes, py: Python) -> Option<Vec<PyObject>> {
    // Arrays are starting with a *, follow by the number of elements and those
    // on seperate lines
    // Empty array: "*0\r\n"
    // Two strings: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    // We could use an array but ignoring the length should be faster
    //
    // The current impl is not ideal
    // *2\r\n:1234\r\n5678\r\n gets parsed properly to [1234, 5678]
    // but anything with \r\n in the actual element does not work
    // should probably get all until \n[+|-|:|$|*] instead of the next \n
    if data == EMPTY_ARRAY {
        Some(Vec::new())
    } else if data == NULL_ARRAY {
        None
    } else {
        let mut res = Vec::new();
        let first_nl = memchr(b'\n', &data).unwrap();
        // Remove beginning
        data.advance(first_nl + 1);
        while let Some(next_nl) = memchr(b'\n', &data) {
            let element = data.split_to(next_nl + 1);
            let new_element = parse_py(element, py).unwrap();
            res.push(new_element);
        }
        Some(res)
    }
}

pub fn parse(data: Bytes) -> RedisType {
    match data[0] {
        b'+' => RedisType::SimpleString(parse_simple_string(data)),
        b'-' => RedisType::Error(RustRedisError {
            error_message: parse_error(data),
        }),
        b':' => RedisType::Integer(parse_integer(data)),
        b'$' => RedisType::BulkString(parse_bulk_string(data)),
        b'*' => RedisType::Array(parse_array_rust(data)),
        _ => RedisType::ParserError(String::from("unknown data type")),
    }
}

pub fn parse_py(data: Bytes, py: Python) -> PyResult<PyObject> {
    match data[0] {
        b'+' => Ok(PyBytes::new(py, &parse_simple_string(data)).to_object(py)),
        b'-' => Err(RedisError::py_err(parse_error(data))),
        b':' => Ok(parse_integer(data).to_object(py)),
        b'$' => match parse_bulk_string(data) {
            Some(bytes) => Ok(PyBytes::new(py, &bytes).to_object(py)),
            None => Ok(py.None()),
        },
        b'*' => match parse_array_py(data, py) {
            Some(arr) => Ok(arr.to_object(py)),
            None => Ok(py.None()),
        },
        _ => Err(ProtocolError::py_err("unknown data type")),
    }
}
