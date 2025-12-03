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

import subprocess
import hopsworks
import multiprocessing
import uuid
from fastmcp import Context
from hopsworks.mcp.models.job import Jobs, to_base_model_job
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client
import os
from queue import Empty, Queue
from threading import Thread


def enqueue_output(out, queue):
    # TODO: obtain output byte-by-byte
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()


class UnixTools:
    """Tools for managing jobs in Hopsworks and executing Unix/Git commands."""

    def __init__(self, mcp):
        """
        Initialize the UnixTools with the MCP server instance.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.UNIX])(self.start_session)
        self.mcp.tool(tags=[TAGS.UNIX])(self.add_input)
        self.mcp.tool(tags=[TAGS.UNIX])(self.get_output)

        self.sessions = {}

    def start_session(self, cwd):
        """
        Start a Unix session in the specified directory.

        Args:
            pwd (str): The directory path to start the session in.
        Returns:
            UnixTools: An instance of UnixTools for the specified directory.
        """
        # TODO: delete processes which were used longer than 5 min ago

        proc = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            text=True,
            bufsize=1
        )

        output_queue = Queue()
        t = Thread(target=enqueue_output, args=(proc.stdout, output_queue))
        t.daemon = True # thread dies with the program
        t.start()

        self.sessions[proc.pid] = (proc, output_queue, "")
        # TODO: check for errors

        print(proc.pid)
        return proc.pid

    def add_input(self, pid: int, addon: str):
        proc, _, _ = self.sessions[pid]
        proc.stdin.write(addon)
        proc.stdin.flush()

    def get_output(self, pid: int, offset: int = 0):
        proc, output_queue, output = self.sessions[pid]
        while True:
            try:
                output += output_queue.get_nowait()
            except Empty:
                self.sessions[pid] = (proc, output_queue, output)
                return output[offset:]
