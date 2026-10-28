"""Replay permission ends when an internal gateway accepts a streaming request."""

import time
from contextlib import asynccontextmanager

import httpx

from execution_engine.drain import GATEWAY_DRAIN_RETRY_SECONDS, is_trusted_drain_response, wait_retry


@asynccontextmanager
async def generation_stream(client, url, payload):
    deadline = time.monotonic() + GATEWAY_DRAIN_RETRY_SECONDS
    while True:
        retry = False
        async with client.stream("POST", url, json=payload, follow_redirects=False) as response:
            if response.status_code == 503:
                # Only a complete small error envelope can authorize replay. A
                # truncated body, transport failure or started stream never does.
                chunks = []
                received = 0
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if received > 65536:
                        response.raise_for_status()
                    chunks.append(chunk)
                response = httpx.Response(
                    response.status_code,
                    headers=response.headers,
                    content=b"".join(chunks),
                    request=response.request,
                    history=response.history,
                )
                retry = is_trusted_drain_response(response)
            if not retry:
                yield response
                return
        if not await wait_retry(deadline):
            yield response
            return
