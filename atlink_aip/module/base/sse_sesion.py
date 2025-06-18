import asyncio
import logging
import aiohttp
from aiohttp_sse_client import client as sse_client
from urllib.parse import urlparse, urljoin


logger = logging.getLogger(__name__)


async def sse_reader(
        url: str,
        session: aiohttp.ClientSession,
        timeout_obj: aiohttp.ClientTimeout,
        endpoint_future: asyncio.Future,
        read_queue: asyncio.Queue
):
    """Read and handle events from the SSE stream."""
    try:
        async with sse_client.EventSource(
                url,
                session=session,
                timeout=timeout_obj,
                raise_for_status=True
        ) as event_source:
            logger.debug("SSE connection established")
            async for event in event_source:
                if event.type == "endpoint":
                    endpoint_url = urljoin(url, event.data)
                    logger.debug(f"Received endpoint URL: {endpoint_url}")

                    url_parsed = urlparse(url)
                    endpoint_parsed = urlparse(endpoint_url)
                    if (url_parsed.netloc != endpoint_parsed.netloc or
                            url_parsed.scheme != endpoint_parsed.scheme):
                        error_msg = f"Endpoint origin mismatch: {endpoint_url}"
                        logger.error(error_msg)
                        raise ValueError(error_msg)

                    if not endpoint_future.done():
                        endpoint_future.set_result(endpoint_url)

                elif event.type == "message":
                    await read_queue.put(event.data)

                else:
                    logger.warning(f"Unknown SSE event: {event.type}")

    except Exception as e:
        logger.error(f"SSE read error: {str(e)}")
        if not endpoint_future.done():
            endpoint_future.set_exception(e)
    finally:
        while not read_queue.empty():
            read_queue.get_nowait()

async def post_writer(
    session: aiohttp.ClientSession,
    endpoint_future: asyncio.Future,
    write_queue: asyncio.Queue,
    timeout: float = 5
):
    """Send a message to the endpoint URL"""
    try:
        # Waiting for the endpoint URL to be available.
        endpoint_url = await asyncio.wait_for(endpoint_future, timeout)

        while True:
            message = await write_queue.get()
            try:
                logger.debug(f"Sending client message: {message}")
                async with session.post(
                        endpoint_url,
                        json=message,
                        timeout=timeout
                ) as resp:
                    resp.raise_for_status()
                    logger.debug(f"Message sent successfully: {resp.status}")
            except Exception as e:
                logger.error(f"Post error: {str(e)}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Post writer failed: {str(e)}")
    finally:
        while not write_queue.empty():
            write_queue.get_nowait()