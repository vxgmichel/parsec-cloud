from parsec.types import DeviceID
from parsec.backend.ping import BasePingComponent
from parsec.backend.drivers.postgresql.handler import send_signal, PGHandler


class PGPingComponent(BasePingComponent):
    def __init__(self, dbh: PGHandler):
        self.dbh = dbh

    async def ping(self, author: DeviceID, ping: str) -> None:
        if not author:
            return
        async with self.dbh.pool.acquire() as conn:
            await send_signal(conn, "pinged", author=author, ping=ping)
