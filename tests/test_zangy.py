import pytest

from zangy import ConnectionPool


@pytest.fixture()
async def client():
    return await ConnectionPool.connect("redis://localhost:6379", 2)


def test_size(client):
    assert client.pool_size == 2

@pytest.mark.asyncio
async def test_ping(client):
    assert await client.execute("PING") == "PONG"


@pytest.mark.asyncio
async def test_set(client):
    # Note that there are a few situations
    # in which redis actually returns a string for an integer which
    # is why this library generally treats integers and strings
    # the same for all numeric responses.
    assert await client.set("hello", 1) is None


@pytest.mark.asyncio
async def test_get(client):
    assert await client.get("hello") == b"1"


@pytest.mark.asyncio
async def test_set_bool(client):
    assert await client.set("hello", True) is None


@pytest.mark.asyncio
async def test_get_bool(client):
    assert await client.get("hello") == b"true"
