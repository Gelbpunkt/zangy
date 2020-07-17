use nom::{
    branch::alt,
    bytes::complete::{escaped, is_not, tag},
    character::complete::{alphanumeric1 as alphanumeric, char, one_of},
    combinator::{cut, map},
    error::{context, ContextError, ParseError},
    number::complete::be_i64,
    sequence::{preceded, terminated},
    Err, IResult,
};

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

fn parse_simple_string<'a, E: ParseError<&'a [u8]>>(i: &'a [u8]) -> IResult<&'a [u8], &'a [u8], E> {
    escaped(alphanumeric, '\\', one_of("\"n\\"))(i)
}

fn simple_string<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], &'a [u8], E> {
    context(
        "simplestring",
        preceded(char('+'), cut(terminated(parse_simple_string, tag("\r\n")))),
    )(i)
}

fn error<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], &'a [u8], E> {
    context(
        "error",
        preceded(char('-'), cut(terminated(parse_simple_string, tag("\r\n")))),
    )(i)
}

fn int<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], i64, E> {
    context(
        "integer",
        preceded(char(':'), cut(terminated(be_i64, tag("\r\n")))),
    )(i)
}

fn bulk_string<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], &'a [u8], E> {
    let (i, _) = tag("$")(i)?;
    let (i, _num_bytes) = be_i64(i)?;
    let (i, _) = tag("\r\n")(i)?;
    context("bulkstring", cut(terminated(is_not("\r\n"), tag("\r\n"))))(i)
}

fn array<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], Vec<RedisType>, E> {
    let (i, _) = tag("*")(i)?;
    let (mut i, mut num_elements) = be_i64(i)?;
    let mut types = Vec::new();
    while num_elements > 0 {
        let (new_i, new) = redis_value(i)?;
        i = new_i;
        types.push(new);
        num_elements = num_elements - 1;
    }
    Ok((i, types))
}

fn redis_value<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    i: &'a [u8],
) -> IResult<&'a [u8], RedisType, E> {
    alt((
        map(int, RedisType::Integer),
        map(simple_string, |s| {
            RedisType::SimpleString(String::from_utf8_lossy(s).to_string())
        }),
        map(bulk_string, |s| {
            RedisType::BulkString(String::from_utf8_lossy(s).to_string())
        }),
        map(array, RedisType::Array),
        map(error, |s| {
            RedisType::Error(String::from_utf8_lossy(s).to_string())
        }),
    ))(i)
}

pub fn parse<'a, E: ParseError<&'a [u8]> + ContextError<&'a [u8]>>(
    data: &'a [u8],
) -> Result<RedisType, Err<E>> {
    if data == b"$-1\r\n" {
        Ok(RedisType::NullBulkString)
    } else if data == b"*-1\r\n" {
        Ok(RedisType::NullArray)
    } else {
        match redis_value(data) {
            Ok((_, value)) => Ok(value),
            Err(e) => Err(e),
        }
    }
}
