import pytest

from zangy import ProtocolError, Reader, RedisError


def test_reader_init_works():
    assert isinstance(Reader(), Reader)


def test_reader_feed_str():
    r = Reader()
    r.feed(b"$5\r\nhello\r\n")
    assert r.gets() == b"hello"


def test_reader_feed_arr():
    r = Reader()
    r.feed(b"*2\r\n$5\r\nhello\r\n")
    r.feed(b"$5\r\nworld\r\n")
    assert r.gets() == [b"hello", b"world"]


def test_reader_feed_snowman():
    r = Reader()
    snowman = b"\xe2\x98\x83"
    r.feed(b"$3\r\n" + snowman + b"\r\n")
    assert r.gets().decode() == "☃"


def test_null_multi_bulk():
    r = Reader()
    r.feed(b"*-1\r\n")
    assert r.gets() == None


def test_empty_multi_bulk():
    r = Reader()
    r.feed(b"*0\r\n")
    assert r.gets() == []


def test_multi_bulk():
    r = Reader()
    r.feed(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
    assert r.gets() == [b"hello", b"world"]


def test_nested_multi_bulk():
    r = Reader()
    r.feed(b"*1\r\n*1\r\n*1\r\n*1\r\n$1\r\n!\r\n")
    assert r.gets() == [[[[b"!"]]]]


def test_reader_feed_error_string():
    r = Reader()
    r.feed(b"-error\r\n")
    with pytest.raises(RedisError):
        r.gets()


def test_reader_len():
    r = Reader()
    r.feed(b"*0\r\n")
    assert len(r) == 4


def test_empty_is_false():
    r = Reader()
    assert r.gets() is False
