use nom::{
    bytes::complete::{take_until, take_while1},
    character::{
        complete::{char, crlf},
        is_digit,
    },
    combinator::cut,
    error::{context, ContextError, ErrorKind, ParseError},
    sequence::{preceded, terminated},
    Err, IResult,
};
use std::convert::TryInto;

// A generic Redis datatype
#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RedisType>),
    NullBulkString,
    NullArray,
}

// nom's take only works with usizes
//fn take<'a, E: ParseError<&'a [u8]>>(
//    count: i64,
//) -> impl Fn(&'a [u8]) -> IResult<&'a [u8], &'a [u8], E> {
//    move |i| {
//        if count > i.len().try_into().unwrap() {
//            Err(Err::Error(ParseError::from_error_kind(i, ErrorKind::Eof)))
//        } else {
//            Ok(i[..count], i[count..])
//        }
//    }
//}

// Parses an integer
fn parse_int<'a, E: ParseError<&'a [u8]>>(i: &'a [u8]) -> IResult<&'a [u8], i64, E> {
    let neg: std::result::Result<(&[u8], char), Err<E>> = char('-')(i);
    let (i, digits) = take_while1(is_digit)(i)?;
    let int_str = String::from_utf8(digits.to_vec()).unwrap();
    let actual_int = int_str.parse::<i64>().unwrap();
    if neg.is_ok() {
        Ok((i, -actual_int))
    } else {
        Ok((i, actual_int))
    }
}

// Parses "+abcdef\r\n"
fn simple_string<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], &'a [u8], E> {
    context(
        "simplestring",
        preceded(char('+'), cut(terminated(take_until("\r\n"), crlf))),
    )(i)
}

// Parses "-error message\r\n"
fn error<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], &'a [u8], E> {
    context(
        "error",
        preceded(char('-'), cut(terminated(take_until("\r\n"), crlf))),
    )(i)
}

// Parses ":123456\r\n"
fn int<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], i64, E> {
    context(
        "integer",
        preceded(char(':'), cut(terminated(parse_int, crlf))),
    )(i)
}

// Parses "$5\r\nabcde\r\n"
fn bulk_string<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], &'a [u8], E> {
    let (i, _) = char('$')(i)?;
    let (i, num_bytes) = parse_int(i)?;
    let (i, _) = crlf(i)?;
    context("bulkstring", cut(terminated(take_until("\r\n"), crlf)))(i)
}

// Parses "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
fn array<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], Vec<RedisType>, E> {
    let (i, _) = char('*')(i)?;
    let (i, mut num_elements) = parse_int(i)?;
    let (mut i, _) = crlf(i)?;
    let mut types = Vec::new();
    while num_elements > 0 {
        let (new_i, new) = parse(i)?;
        i = new_i;
        types.push(new);
        num_elements = num_elements - 1;
    }
    Ok((i, types))
}

pub fn parse<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    data: &'a [u8],
) -> Result<(&'a [u8], RedisType), Err<E>> {
    if data == b"$-1\r\n" {
        Ok((b"", RedisType::NullBulkString))
    } else if data == b"*-1\r\n" {
        Ok((b"", RedisType::NullArray))
    } else {
        match data[0] {
            b'+' => match simple_string::<'a, E>(data) {
                Ok(res) => {
                    let obj = RedisType::SimpleString(String::from_utf8_lossy(res.1).to_string());
                    Ok((res.0, obj))
                }
                Err(e) => Err(e),
            },
            b':' => match int::<'a, E>(data) {
                Ok(res) => {
                    let obj = RedisType::Integer(res.1);
                    Ok((res.0, obj))
                }
                Err(e) => Err(e),
            },
            b'$' => match bulk_string::<'a, E>(data) {
                Ok(res) => {
                    let obj = RedisType::BulkString(String::from_utf8_lossy(res.1).to_string());
                    Ok((res.0, obj))
                }
                Err(e) => Err(e),
            },
            b'*' => match array::<'a, E>(data) {
                Ok(res) => {
                    let obj = RedisType::Array(res.1);
                    Ok((res.0, obj))
                }
                Err(e) => Err(e),
            },
            b'-' => match error::<'a, E>(data) {
                Ok(res) => {
                    let obj = RedisType::Error(String::from_utf8_lossy(res.1).to_string());
                    Ok((res.0, obj))
                }
                Err(e) => Err(e),
            },
            _ => Err(Err::Incomplete(nom::Needed::Unknown)),
        }
    }
}
