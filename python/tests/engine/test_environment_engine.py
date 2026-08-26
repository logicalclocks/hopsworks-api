#
#   Copyright 2026 Hopsworks AB
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

import pytest
import requests
from hopsworks_common import command
from hopsworks_common.client.exceptions import EnvironmentException
from hopsworks_common.engine import environment_engine


class Artifact:
    """Stand-in for a library or environment carrying commands."""

    def __init__(self, *statuses):
        self._commands = [command.Command(status=s) for s in statuses]


class CommandlessArtifact:
    """Stand-in for the response shape where no command is attached.

    Both `Library` and `Environment` leave `_commands` as `None` rather than empty in that case.
    """

    _commands = None


def clock(*readings):
    """A `time.monotonic` stand-in that returns the given readings, then holds the last one.

    Holding rather than exhausting keeps the tests from depending on exactly how many times the loop reads the clock.
    """
    values = iter(readings)
    last = [0.0]

    def monotonic():
        last[0] = next(values, last[0])
        return last[0]

    return monotonic


class TestEnvironmentEngine:
    def test_returns_when_the_command_succeeds(self, mocker):
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        poll = mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("SUCCESS")
        )

        engine._await_library_command("myenv", "matplotlib")

        assert poll.call_count == 1

    def test_raises_when_the_command_fails(self, mocker):
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("FAILED")
        )

        with pytest.raises(EnvironmentException):
            engine._await_library_command("myenv", "matplotlib")

    def test_returns_when_the_artifact_is_gone(self, mocker):
        """A missing artifact ends the wait rather than failing it.

        Uninstalling a library that is already absent is done, not broken, and this case was the one the original loop did handle.
        Kept under test so bounding the wait does not turn a benign outcome into a timeout.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        poll = mocker.patch.object(engine, "_poll_commands_library", return_value=None)

        engine._await_library_command("myenv", "matplotlib")

        # It polled, saw the artifact was gone, and returned: no exception and no second poll.
        assert poll.call_count == 1

    def test_returns_when_the_response_carries_no_commands(self, mocker):
        """An artifact with no commands means nothing is in flight, so the wait is over.

        `_commands` is `None` and not `[]` in that case, which `len()` cannot take, so the value has to be normalised before the loop condition sees it.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        poll = mocker.patch.object(
            engine, "_poll_commands_library", return_value=CommandlessArtifact()
        )

        engine._await_library_command("myenv", "matplotlib")

        assert poll.call_count == 1

    def test_raises_instead_of_polling_forever_on_a_stalled_command(self, mocker):
        """The regression this exists for.

        A command whose installer is gone stays ONGOING for good.
        Before the deadline the caller polled it every five seconds with no output and no end, which cost two full loadtest runs before anyone knew where the process was.

        The clock is faked rather than slept through, so the test proves the deadline is enforced without taking the timeout to prove it.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=clock(0, 1, 10_000)
        )
        poll = mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException, match="Timed out"):
            engine._await_library_command("myenv", "tmp3zwmpner.txt")

        # It gave up rather than spinning: a bounded number of polls, not an endless stream.
        assert poll.call_count < 5

    def test_the_timeout_message_names_the_artifact_and_last_status(self, mocker):
        """A bare timeout does not tell an operator where to look.

        The environment, the library and the status it was stuck in are the three facts that turn this from a mystery hang into a thing to go and inspect.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=clock(0, 1, 10_000)
        )
        mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException) as raised:
            engine._await_library_command("env_ge_0_18_12", "tmp3zwmpner.txt")

        message = str(raised.value)
        assert "env_ge_0_18_12" in message
        assert "tmp3zwmpner.txt" in message
        assert "ONGOING" in message

    def test_an_explicit_timeout_overrides_the_default(self, mocker):
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=clock(0, 1, 10_000)
        )
        mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException, match="7s"):
            engine._await_library_command("myenv", "matplotlib", timeout=7)

    def test_a_fractional_timeout_is_reported_as_given(self, mocker):
        """Rounding the duration to whole seconds reports a half-second wait as `0s`, which reads as a bug in the client rather than a stalled command."""
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=clock(0, 0.1, 10_000)
        )
        mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException, match="0.5s"):
            engine._await_library_command("myenv", "matplotlib", timeout=0.5)

    def test_environment_command_is_bounded_too(self, mocker):
        """The environment wait had the same unbounded shape, so it gets the same deadline."""
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=clock(0, 1, 10_000)
        )
        mocker.patch.object(
            engine, "_poll_commands_environment", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException, match="Timed out"):
            engine._await_environment_command("myenv")

    def test_each_poll_request_is_bounded(self, mocker):
        """The deadline is only enforceable if the request the loop makes comes back.

        A GET that stalls blocks in the socket, where the loop cannot see it, so the poll carries its own request timeout.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        library_poll = mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("SUCCESS")
        )
        environment_poll = mocker.patch.object(
            engine, "_poll_commands_environment", return_value=Artifact("SUCCESS")
        )

        engine._await_library_command("myenv", "matplotlib")
        engine._await_environment_command("myenv")

        library_poll.assert_called_once_with(
            "myenv", "matplotlib", engine.POLL_REQUEST_TIMEOUT
        )
        environment_poll.assert_called_once_with("myenv", engine.POLL_REQUEST_TIMEOUT)

    def test_a_timed_out_poll_request_does_not_end_the_wait(self, mocker):
        """One request giving up is not the command giving up.

        A slow or dropped poll leaves the status unknown, so the loop keeps the last one it saw and carries on until the deadline decides.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        poll = mocker.patch.object(
            engine,
            "_poll_commands_library",
            side_effect=[requests.exceptions.ReadTimeout(), Artifact("SUCCESS")],
        )

        engine._await_library_command("myenv", "matplotlib")

        assert poll.call_count == 2

    def test_the_request_timeout_reaches_the_backend_call(self, mocker):
        """The bound is worth nothing unless `requests` is the one holding it."""
        engine = environment_engine.EnvironmentEngine()
        mock_client = mocker.MagicMock()
        mock_client._project_id = 1
        mock_client._send_request.return_value = {
            "channel": "pip",
            "packageSource": "PIP",
            "library": "matplotlib",
            "version": "3.1.3",
        }
        mocker.patch("hopsworks_common.client._get_instance", return_value=mock_client)

        engine._poll_commands_library("myenv", "matplotlib", 30)

        assert mock_client._send_request.call_args.kwargs["timeout"] == 30
