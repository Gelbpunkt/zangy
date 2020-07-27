import pytest

from zangy import Client, Connection


@pytest.fixture()
def client():
    return Client("redis://localhost:6379")

@pytest.fixture()
async def connection(client):
    return await client.get_connection()

@pytest.mark.asyncio
async def test_ping(connection):
    assert await connection.execute("PING") == "PONG"
