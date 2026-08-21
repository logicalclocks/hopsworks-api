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
from hopsworks_common import command
from hopsworks_common.client.exceptions import EnvironmentException
from hopsworks_common.engine import environment_engine


class Artifact:
    """Stand-in for a library or environment carrying commands."""

    def __init__(self, *statuses):
        self._commands = [command.Command(status=s) for s in statuses]


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

        Uninstalling a library that is already absent is done, not broken, and this case was
        the one the original loop did handle. Kept under test so bounding the wait does not
        turn a benign outcome into a timeout.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(engine, "_poll_commands_library", return_value=None)

        engine._await_library_command("myenv", "matplotlib")

    def test_raises_instead_of_polling_forever_on_a_stalled_command(self, mocker):
        """The regression this exists for.

        A command whose installer is gone stays ONGOING for good. Before the deadline the
        caller polled it every five seconds with no output and no end, which cost two full
        loadtest runs before anyone knew where the process was.

        The clock is faked rather than slept through, so the test proves the deadline is
        enforced without taking the timeout to prove it.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        # monotonic() is read once for the deadline, then once per loop; walk it past the
        # timeout so the second check trips.
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=[0, 1, 10_000, 10_000]
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

        The environment, the library and the status it was stuck in are the three facts that
        turn this from a mystery hang into a thing to go and inspect.
        """
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=[0, 1, 10_000, 10_000]
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
            environment_engine.time, "monotonic", side_effect=[0, 1, 10_000, 10_000]
        )
        mocker.patch.object(
            engine, "_poll_commands_library", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException, match="7s"):
            engine._await_library_command("myenv", "matplotlib", timeout=7)

    def test_environment_command_is_bounded_too(self, mocker):
        """The environment wait had the same unbounded shape, so it gets the same deadline."""
        engine = environment_engine.EnvironmentEngine()
        mocker.patch.object(environment_engine.time, "sleep")
        mocker.patch.object(
            environment_engine.time, "monotonic", side_effect=[0, 1, 10_000, 10_000]
        )
        mocker.patch.object(
            engine, "_poll_commands_environment", return_value=Artifact("ONGOING")
        )

        with pytest.raises(EnvironmentException, match="Timed out"):
            engine._await_environment_command("myenv")
