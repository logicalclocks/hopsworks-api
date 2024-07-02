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

from hsml.utils.schema import tensor


class TestTensor:
    def test_constructor_default(self):
        # Arrange
        _type = 1234
        shape = 4321

        # Act
        t = tensor.Tensor(_type, shape)

        # Assert
        assert t.type == str(_type)
        assert t.shape == str(shape)
        assert not hasattr(t, "name")
        assert not hasattr(t, "description")

    def test_constructor(self):
        # Arrange
        _type = 1234
        shape = 4321
        name = 1111111
        description = 2222222

        # Act
        t = tensor.Tensor(_type, shape, name, description)

        # Assert
        assert t.type == str(_type)
        assert t.shape == str(shape)
        assert t.name == str(name)
        assert t.description == str(description)
