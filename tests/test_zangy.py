import pytest

from zangy import Reader, RedisError


def test_reader_init_works():
    assert isinstance(Reader(), Reader)

def test_reader_feed_str():
    r = Reader()
    r.feed(b"$5\r\nhello\r\n")
    assert r.gets() == "hello"

def test_reader_feed_arr():
    r = Reader()
    r.feed(b"*2\r\n$5\r\nhello\r\n")
    r.feed(b"$5\r\nworld\r\n")
    assert r.gets() == ["hello", "world"]

def test_reader_feed_error_string(self):
    r = Reader()
    r.feed(b"-error\r\n")
    with pytest.raises(RedisError):
        r.gets()
