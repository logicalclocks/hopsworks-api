#
#   Copyright 2024 Hopsworks AB
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

import pytest
from hsml.decorators import (
    HopsworksConnectionError,
    NoHopsworksConnectionError,
    connected,
    not_connected,
)


class TestDecorators:
    # test not connected

    def test_not_connected_valid(self, mocker):
        # Arrange
        mock_instance = mocker.MagicMock()
        mock_instance._connected = False

        @not_connected
        def assert_not_connected(inst, arg, key_arg):
            assert not inst._connected
            assert arg == "arg"
            assert key_arg == "key_arg"

        # Act
        assert_not_connected(mock_instance, "arg", key_arg="key_arg")

    def test_not_connected_invalid(self, mocker):
        # Arrange
        mock_instance = mocker.MagicMock()
        mock_instance._connected = True

        @not_connected
        def assert_not_connected(inst, arg, key_arg):
            pass

        # Act
        with pytest.raises(HopsworksConnectionError):
            assert_not_connected(mock_instance, "arg", key_arg="key_arg")

    # test connected

    def test_connected_valid(self, mocker):
        # Arrange
        mock_instance = mocker.MagicMock()
        mock_instance._connected = True

        @connected
        def assert_connected(inst, arg, key_arg):
            assert inst._connected
            assert arg == "arg"
            assert key_arg == "key_arg"

        # Act
        assert_connected(mock_instance, "arg", key_arg="key_arg")

    def test_connected_invalid(self, mocker):
        # Arrange
        mock_instance = mocker.MagicMock()
        mock_instance._connected = False

        @connected
        def assert_connected(inst, arg, key_arg):
            pass

        # Act
        with pytest.raises(NoHopsworksConnectionError):
            assert_connected(mock_instance, "arg", key_arg="key_arg")
