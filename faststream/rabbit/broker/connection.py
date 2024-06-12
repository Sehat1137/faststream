from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Iterator, Optional, cast

from aio_pika import connect_robust
from aio_pika.pool import Pool

if TYPE_CHECKING:
    from ssl import SSLContext

    from aio_pika import (
        RobustChannel,
        RobustConnection,
    )
    from aio_pika.abc import TimeoutType


class ConnectionManager:
    def __init__(
        self,
        *,
        url: str,
        timeout: "TimeoutType",
        ssl_context: Optional["SSLContext"],
        connection_pool_size: Optional[int],
        channel_pool_size: Optional[int]
    ) -> None:
        self._connection_pool = Pool(
            lambda: connect_robust(
                url=url,
                timeout=timeout,
                ssl_context=ssl_context,
            ),
            max_size=connection_pool_size,
        )

        self._channel_pool = Pool(
            self._get_channel,
            max_size=channel_pool_size,
        )

    async def get_connection(self) -> "RobustConnection":
        return await self._connection_pool._get()

    @asynccontextmanager
    async def acquire_connection(self) -> Iterator["RobustConnection"]:
        async with self._connection_pool.acquire() as connection:
            yield connection

    async def get_channel(self) -> "RobustChannel":
        return await self._channel_pool._get()

    @asynccontextmanager
    async def acquire_channel(self) -> Iterator["RobustChannel"]:
        async with self._channel_pool.acquire() as channel:
            yield channel

    async def _get_channel(self) -> "RobustChannel":
        async with self.acquire_connection() as connection:
            return cast(
                "RobustChannel",
                await connection.channel(),
            )

    async def close(self) -> None:
        if not self._channel_pool.is_closed:
            await self._channel_pool.close()
        self._channel_pool = None

        if not self._connection_pool.is_closed:
            await self._connection_pool.close()
        self._connection_pool = None