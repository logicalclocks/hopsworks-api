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
import subprocess
import tempfile
from pathlib import Path
from queue import Empty, Queue
from threading import Thread

from hopsworks.mcp.utils.tags import TAGS


def enqueue_output(out, queue):
    # TODO: obtain output byte-by-byte
    for line in iter(out.readline, b""):
        queue.put(line)
    out.close()


class TerminalTools:
    """Tools implementing shell and terminal access to Hopsworks."""

    def __init__(self, mcp):
        """Initialize the TerminalTools with the MCP server instance.

        Parameters:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.TERMINAL])(self.start_session)
        self.mcp.tool(tags=[TAGS.TERMINAL])(self.add_input)
        self.mcp.tool(tags=[TAGS.TERMINAL])(self.get_output)

        self.sessions: dict[int, tuple[subprocess.Popen, Queue, str, str]] = {}

    def start_session(self, cwd: str) -> int:
        """Start a terminal session in the specified directory.

        Parameters:
            cwd: The directory path to start the session in.

        Returns:
            PID: The session process identifier.
        """
        # TODO: delete processes which were used longer than 5 min ago

        envdir = tempfile.mkdtemp()

        proc = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            text=True,
            bufsize=1,
            env={"PROMPT_COMMAND": f'python -c "import json, os; print(json.dumps(dict(os.environ)))" > {envdir}/env.json'},
        )

        output_queue = Queue()
        t = Thread(target=enqueue_output, args=(proc.stdout, output_queue))
        t.daemon = True  # thread dies with the program
        t.start()

        self.sessions[proc.pid] = (proc, output_queue, "", envdir)
        # TODO: check for errors

        return proc.pid

    def add_input(self, pid: int, addon: str):
        proc, _, _, _ = self.sessions[pid]
        if not proc.stdin:
            raise Exception("Process stdin is not available")
        proc.stdin.write(addon)
        proc.stdin.flush()

    def get_output(self, pid: int, offset: int = 0):
        proc, output_queue, output, envdir = self.sessions[pid]
        while True:
            try:
                output += output_queue.get_nowait()
            except Empty:
                self.sessions[pid] = (proc, output_queue, output, envdir)
                return output[offset:]

    def get_environ(self, pid: int) -> dict[str, str]:
        _, _, _, envdir = self.sessions[pid]
        return json.loads((Path(envdir) / "env.json").read_text())
