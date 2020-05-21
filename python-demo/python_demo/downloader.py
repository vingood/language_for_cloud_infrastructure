import asyncio
import os
import tempfile
import uuid
from typing import Optional

import click
import aiohttp
import time
import uvloop
from aiohttp import TCPConnector
from attr import dataclass

HOST_URL = os.environ.get("API_HOST_URL")

FILES = [
    "0cf50f1c99234954b00340471538ce9d.MOV",
    "0db9a58b669048dc999eb8f11f7ba424.MOV",
    "0d38ceda70b14ccfaf6960514615757f.MOV",
    "0CB55372-0173-49F7-9EAF-6CF1A40382C5.MOV",
    "0f132134b2474cbd858559ed979835a3.MOV",
]


@dataclass
class Result:
    content: Optional[bytes] = None
    error: Optional[Exception] = None


async def download_video(file_name: str, connector: TCPConnector) -> Result:
    """
    Download a content
    """
    click.secho(f"Begin downloading {file_name}", fg="yellow")
    url = f"{HOST_URL}{file_name}"
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url) as resp:
                return Result(content=await resp.read())
        except asyncio.TimeoutError as err:
            return Result(error=err)


async def write_to_file(tmpdirname: str, content: bytes) -> None:
    """
    Save a content to the localstorage
    """
    filename = os.path.join(tmpdirname, f"async_{str(uuid.uuid4())}.mov")
    with open(filename, "wb") as video_file:
        video_file.write(content)
        click.secho(f"Finished writing {filename}", fg="green")


async def web_scrape_task(
    file_name: str, tmpdirname: str, connector: TCPConnector
) -> None:
    resp = await download_video(file_name, connector)
    if resp.error is None:
        await write_to_file(tmpdirname, resp.content)
    else:
        click.secho(f"Error download a {file_name}", fg="red")


@click.command()
def main() -> None:
    """
    This is the simple web scraper.
    The scraper gets data from sources and saves them to our local machine
    for further analysis
    """
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(async_main())


async def async_main() -> None:
    s = time.perf_counter()
    conn = aiohttp.TCPConnector(limit=3)
    with tempfile.TemporaryDirectory() as tmpdirname:
        await asyncio.gather(
            *[web_scrape_task(file_name, tmpdirname, conn) for file_name in FILES]
        )
    elapsed = time.perf_counter() - s
    click.secho(f"Execution time: {elapsed:0.2f} seconds.", fg="bright_blue")


if __name__ == "__main__":
    main()
