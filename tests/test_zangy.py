import pytest

from zangy import ConnectionPool


@pytest.fixture()
def client():
    return ConnectionPool("redis://localhost:6379")


@pytest.fixture()
async def connection(client):
    return await client.get()


@pytest.mark.asyncio
async def test_ping(connection):
    assert await connection.execute("PING") == "PONG"


@pytest.mark.asyncio
async def test_set(connection):
    # Note that there are a few situations
    # in which redis actually returns a string for an integer which
    # is why this library generally treats integers and strings
    # the same for all numeric responses.
    assert await connection.execute("SET", "hello", 1) is True


@pytest.mark.asyncio
async def test_get(connection):
    assert await connection.execute("GET", "hello") == b"1"


@pytest.mark.asyncio
async def test_set_bool(connection):
    assert await connection.execute("SET", "hello", True) is True


@pytest.mark.asyncio
async def test_get_bool(connection):
    assert await connection.execute("GET", "hello") == b"true"
