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

import numpy as np
import pytest
from hopsworks.internal.ml.utils.schema import tensor, tensor_schema


class TestTensorSchema:
    # constructor

    def test_constructor_default(self):
        # Act
        with pytest.raises(TypeError) as e_info:
            _ = tensor_schema.TensorSchema()

        # Assert
        assert "is not supported in a tensor schema" in str(e_info.value)

    def test_constructor_invalid(self):
        # Act
        with pytest.raises(TypeError) as e_info:
            _ = tensor_schema.TensorSchema("invalid")

        # Assert
        assert "is not supported in a tensor schema" in str(e_info.value)

    def test_constructor_list(self, mocker):
        # Arrange
        tensor_obj = [1234, 4321, 1111111, 2222222]
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.tensor_schema.TensorSchema._convert_list_to_schema",
            return_value="list_to_schema",
        )
        mock_convert_tensor_to_schema = mocker.patch(
            "hsml.utils.schema.tensor_schema.TensorSchema._convert_tensor_to_schema",
            return_value="tensor_to_schema",
        )

        # Act
        ts = tensor_schema.TensorSchema(tensor_obj)

        # Assert
        assert ts.tensors == "list_to_schema"
        mock_convert_list_to_schema.assert_called_once_with(tensor_obj)
        mock_convert_tensor_to_schema.assert_not_called()

    def test_constructor_ndarray(self, mocker):
        # Arrange
        tensor_obj = np.array([1234, 4321, 1111111, 2222222])
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.tensor_schema.TensorSchema._convert_list_to_schema",
            return_value="list_to_schema",
        )
        mock_convert_tensor_to_schema = mocker.patch(
            "hsml.utils.schema.tensor_schema.TensorSchema._convert_tensor_to_schema",
            return_value="tensor_to_schema",
        )

        # Act
        ts = tensor_schema.TensorSchema(tensor_obj)

        # Assert
        assert ts.tensors == "tensor_to_schema"
        mock_convert_tensor_to_schema.assert_called_once_with(tensor_obj)
        mock_convert_list_to_schema.assert_not_called()

    # convert tensor to schema

    def test_convert_tensor_to_schema(self, mocker):
        # Arrange
        tensor_obj = mocker.MagicMock()
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._convert_tensor_to_schema = (
            tensor_schema.TensorSchema._convert_tensor_to_schema
        )
        mock_tensor_init = mocker.patch(
            "hsml.utils.schema.tensor.Tensor.__init__", return_value=None
        )

        # Act
        t = mock_tensor_schema._convert_tensor_to_schema(mock_tensor_schema, tensor_obj)

        # Assert
        assert isinstance(t, tensor.Tensor)
        mock_tensor_init.assert_called_once_with(tensor_obj.dtype, tensor_obj.shape)

    # convert list to schema

    def test_convert_list_to_schema_singleton(self, mocker):
        # Arrange
        tensor_obj = [1234]
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._convert_list_to_schema = (
            tensor_schema.TensorSchema._convert_list_to_schema
        )

        # Act
        t = mock_tensor_schema._convert_list_to_schema(mock_tensor_schema, tensor_obj)

        # Assert
        assert isinstance(t, list)
        assert len(t) == len(tensor_obj)
        mock_tensor_schema._build_tensor.assert_called_once_with(1234)

    def test_convert_list_to_schema_list(self, mocker):
        # Arrange
        tensor_obj = np.array([1234, 4321, 1111111, 2222222])
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._convert_list_to_schema = (
            tensor_schema.TensorSchema._convert_list_to_schema
        )

        # Act
        t = mock_tensor_schema._convert_list_to_schema(mock_tensor_schema, tensor_obj)

        # Assert
        assert isinstance(t, list)
        assert len(t) == len(tensor_obj)
        assert mock_tensor_schema._build_tensor.call_count == len(tensor_obj)

    # build tensor

    def test_build_tensor_type_and_shape_only(self, mocker):
        # Arrange
        tensor_obj = {"type": "tensor_type", "shape": "tensor_shape"}
        mock_tensor_init = mocker.patch(
            "hsml.utils.schema.tensor.Tensor.__init__", return_value=None
        )
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._build_tensor = tensor_schema.TensorSchema._build_tensor

        # Act
        t = mock_tensor_schema._build_tensor(mock_tensor_schema, tensor_obj)

        # Assert
        assert isinstance(t, tensor.Tensor)
        mock_tensor_init.assert_called_once_with(
            tensor_obj["type"], tensor_obj["shape"], name=None, description=None
        )

    def test_build_tensor_invalid_missing_type(self, mocker):
        # Arrange
        tensor_obj = {"shape": "tensor_shape"}
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._build_tensor = tensor_schema.TensorSchema._build_tensor

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = mock_tensor_schema._build_tensor(mock_tensor_schema, tensor_obj)

        # Assert
        assert "Mandatory 'type' key missing from entry" in str(e_info.value)

    def test_build_tensor_invalid_missing_shape(self, mocker):
        # Arrange
        tensor_obj = {"type": "tensor_type"}
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._build_tensor = tensor_schema.TensorSchema._build_tensor

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = mock_tensor_schema._build_tensor(mock_tensor_schema, tensor_obj)

        # Assert
        assert "Mandatory 'shape' key missing from entry" in str(e_info.value)

    def test_build_tensor_type_shape_name_and_description(self, mocker):
        # Arrange
        tensor_obj = {
            "type": "tensor_type",
            "shape": "tensor_shape",
            "name": "tensor_name",
            "description": "tensor_description",
        }
        mock_tensor_init = mocker.patch(
            "hsml.utils.schema.tensor.Tensor.__init__", return_value=None
        )
        mock_tensor_schema = mocker.MagicMock()
        mock_tensor_schema._build_tensor = tensor_schema.TensorSchema._build_tensor

        # Act
        t = mock_tensor_schema._build_tensor(mock_tensor_schema, tensor_obj)

        # Assert
        assert isinstance(t, tensor.Tensor)
        mock_tensor_init.assert_called_once_with(
            tensor_obj["type"],
            tensor_obj["shape"],
            name=tensor_obj["name"],
            description=tensor_obj["description"],
        )
