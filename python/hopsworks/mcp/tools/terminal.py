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

import json
import secrets
import subprocess
import tempfile
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import TYPE_CHECKING

from hopsworks.mcp.utils.tags import TAGS


if TYPE_CHECKING:
    from fastmcp import FastMCP


def enqueue_output(out, queue):
    # TODO: obtain output byte-by-byte
    for line in iter(out.readline, b""):
        queue.put(line)
    out.close()


class TerminalTools:
    """Tools implementing shell and terminal access to Hopsworks."""

    def __init__(self, mcp: FastMCP):
        """Initialize the TerminalTools with the MCP server instance.

        Parameters:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.TERMINAL])(self.start_session)
        self.mcp.tool(tags=[TAGS.TERMINAL])(self.add_input)
        self.mcp.tool(tags=[TAGS.TERMINAL])(self.get_output)

        # Keyed by an unguessable session token (not the OS pid, which is
        # enumerable) so one client cannot drive or read another client's
        # shell by guessing an identifier.
        self.sessions: dict[str, tuple[subprocess.Popen, Queue, str, str]] = {}

    def _session(self, session_id: str) -> tuple[subprocess.Popen, Queue, str, str]:
        """Look up a session by token, raising a clean error when unknown."""
        session = self.sessions.get(session_id)
        if session is None:
            raise ValueError("Unknown or expired terminal session.")
        return session

    def start_session(self, cwd: str) -> str:
        """Start a terminal session in the specified directory.

        Parameters:
            cwd: The directory path to start the session in. Must be an
                existing directory.

        Returns:
            An opaque session token. Pass it to ``add_input``/``get_output``;
            it is the only handle to the session and is not derivable from the
            process table.
        """
        # TODO: delete processes which were used longer than 5 min ago

        cwd_path = Path(cwd).expanduser()
        if not cwd_path.is_dir():
            raise ValueError(f"cwd is not an existing directory: {cwd}")

        envdir = tempfile.mkdtemp()

        proc = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(cwd_path),
            text=True,
            bufsize=1,
            # Capture only PWD on each prompt — never the full environment,
            # which would write the server's secrets (API key, JWT) to disk.
            env={
                "PROMPT_COMMAND": f'python -c "import json, os; print(json.dumps({{\\"PWD\\": os.environ.get(\\"PWD\\", \\"\\")}}))" > {envdir}/env.json'
            },
        )

        output_queue = Queue()
        t = Thread(target=enqueue_output, args=(proc.stdout, output_queue))
        t.daemon = True  # thread dies with the program
        t.start()

        session_id = secrets.token_urlsafe(32)
        self.sessions[session_id] = (proc, output_queue, "", envdir)
        # TODO: check for errors

        return session_id

    def add_input(self, session_id: str, addon: str):
        r"""Add input to the terminal session.

        Note that if you want to simulate pressing Enter, you need to include a newline character.
        So, to run a command, you should set addon to something like "ls -la\n".

        Parameters:
            session_id: The token returned by ``start_session``.
            addon: The input string to add to the session.
        """
        proc, _, _, _ = self._session(session_id)
        if not proc.stdin:
            raise Exception("Process stdin is not available")
        proc.stdin.write(addon)
        proc.stdin.flush()

    def get_output(self, session_id: str, offset: int = 0) -> str:
        r"""Get output from the terminal session.

        Set offset to the number of characters you have already retrieved, so that only new output is returned.

        Parameters:
            session_id: The token returned by ``start_session``.
            offset: The offset from which to retrieve new output.

        Returns:
            The output string from the session starting from the specified offset.
        """
        proc, output_queue, output, envdir = self._session(session_id)
        while True:
            try:
                output += output_queue.get_nowait()
            except Empty:
                self.sessions[session_id] = (proc, output_queue, output, envdir)
                return output[offset:]

    def get_environ(self, session_id: str) -> dict[str, str]:
        r"""Get the captured environment variables of the terminal session.

        Only ``PWD`` is captured (so an agent can track the working directory);
        the full environment is deliberately not exposed.

        Parameters:
            session_id: The token returned by ``start_session``.

        Returns:
            A dictionary of the captured environment variables.
        """
        _, _, _, envdir = self._session(session_id)
        return json.loads((Path(envdir) / "env.json").read_text())
