#
#   Copyright 2025 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING

from fastmcp import (
    Context,  # noqa: TC002
)
from fastmcp.server.dependencies import get_context
from fastmcp.server.http import _current_http_request
from filelock import AsyncFileLock
from hopsworks.mcp.utils.tags import TAGS
from pydantic import BaseModel


if TYPE_CHECKING:
    from fastmcp import (
        FastMCP,
    )


class ExecutionResult(BaseModel):
    output: str = ""
    returncode: int | None = None


class BrewerTools:
    def __init__(self, mcp: FastMCP):
        """Initialize the BrewerTools with the MCP server instance.

        Parameters:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.BREWER])(self.execute)

    # TODO: Use on_notification Middleware to handle cancellation requests, add process manager

    async def execute(  # docsig: disable
        self,
        chat_id: int,
        script_path: Path,
        ctx: Context,
    ) -> ExecutionResult:
        """Execute a Python script in a conda environment for a specific chat."""
        await ctx.info("Locking the chat environment for execution...\n")
        if not isinstance(chat_id, int) or chat_id < 0:
            raise ValueError("chat_id must be a non-negative integer.")
        chatdir = Path(f"/hopsfs/Brewer/{chat_id}").resolve()
        async with AsyncFileLock(f"{chatdir}/.lock"):
            await extract_hopsworks_credentials(chatdir)

            envname = await get_chat_env(chat_id, chatdir)

            await ctx.info("Executing the script...\n")
            # Confine the script to the chat directory: reject absolute paths
            # and any ".." escape so a client cannot run an arbitrary file
            # elsewhere on the server.
            resolved = (chatdir / script_path).resolve()
            if not _is_within(resolved, chatdir):
                raise ValueError(
                    "script_path must resolve to a file inside the chat directory."
                )
            script_path = resolved

            envcopy = os.environ.copy()
            envcopy["SECRETS_DIR"] = str(chatdir)

            proc = await asyncio.create_subprocess_exec(
                "conda",
                "run",
                "-n",
                envname,
                "python",
                script_path.name,
                cwd=script_path.parent,
                env=envcopy,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            if proc.stdout is None:
                raise Exception("Unable to create env: stdout is None")
            output = ""
            while proc.returncode is None:
                buf = await proc.stdout.readline()
                if not buf:
                    break
                b = buf.decode()
                output += b
                await ctx.info(b)

            return ExecutionResult(
                output=output,
                returncode=proc.returncode,
            )


async def extract_hopsworks_credentials(chatdir: Path):
    ctx = get_context()
    await ctx.info("Setting up authentication credentials...\n")
    request = _current_http_request.get()
    if not request:
        raise Exception("No HTTP request found")
    auth = next(
        (
            request.headers.get(key)
            for key in request.headers
            if key.lower() == "authorization"
        ),
        None,
    )
    if not auth:
        raise Exception("No authentication header found")
    if auth.startswith("Bearer"):
        _write_secret(chatdir / "token.jwt", auth.removeprefix("Bearer").strip())
    elif auth.startswith("ApiKey"):
        _write_secret(chatdir / "api.key", auth.removeprefix("ApiKey").strip())
    else:
        raise Exception("Unknown auth type")


def _is_within(path: Path, parent: Path) -> bool:
    """Return True iff ``path`` is ``parent`` or nested under it."""
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _write_secret(path: Path, value: str) -> None:
    """Write a credential to ``path`` with owner-only (0o600) permissions.

    Opened O_CREAT|O_WRONLY|O_TRUNC at 0o600 so the secret is never
    world-readable, even momentarily between create and chmod.
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(value)


async def get_chat_env(chat_id: int, chatdir: Path) -> str:
    ctx = get_context()
    await ctx.info("Checking if the chat environment is ready to be used...\n")
    proc = await asyncio.create_subprocess_exec(
        "conda",
        "env",
        "list",
        cwd=chatdir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    envlist, _ = await proc.communicate()
    envname = f"chat{chat_id}"
    if envname not in envlist.decode():
        await ctx.info("Creating a new chat environment...\n")
        proc = await asyncio.create_subprocess_exec(
            "conda",
            "create",
            "--clone",
            "hopsworks_environment",
            "-n",
            envname,
            cwd=chatdir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        if proc.stdout is None:
            raise Exception("Unable to create env: stdout is None")
        output = ""
        while proc.returncode is None:
            buf = await proc.stdout.readline()
            if not buf:
                break
            b = buf.decode()
            output += b
            await ctx.info(b)
        if proc.returncode:
            raise Exception(f"Unable to create env ({proc.returncode}):\n{output}")
    return envname
