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
#

import numpy as np
from hsml import schema


class TestSchema:
    # constructor

    def test_constructor_default(self, mocker):
        # Arrange
        mock_tensor = mocker.MagicMock()
        mock_tensor.tensors = mocker.MagicMock(return_value="tensor_schema")
        mock_columnar = mocker.MagicMock()
        mock_columnar.columns = mocker.MagicMock(return_value="columnar_schema")
        mock_convert_tensor_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_tensor_to_schema", return_value=mock_tensor
        )
        mock_convert_columnar_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_columnar_to_schema",
            return_value=mock_columnar,
        )

        # Act
        s = schema.Schema()

        # Assert
        assert s.columnar_schema == mock_columnar.columns
        assert not hasattr(s, "tensor_schema")
        mock_convert_tensor_to_schema.assert_not_called()
        mock_convert_columnar_to_schema.assert_called_once_with(None)

    def test_constructor_numpy(self, mocker):
        # Arrange
        obj = np.array([])
        mock_tensor = mocker.MagicMock()
        mock_tensor.tensors = mocker.MagicMock(return_value="tensor_schema")
        mock_columnar = mocker.MagicMock()
        mock_columnar.columns = mocker.MagicMock(return_value="columnar_schema")
        mock_convert_tensor_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_tensor_to_schema", return_value=mock_tensor
        )
        mock_convert_columnar_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_columnar_to_schema",
            return_value=mock_columnar,
        )

        # Act
        s = schema.Schema(obj)

        # Assert
        assert s.tensor_schema == mock_tensor.tensors
        assert not hasattr(s, "columnar_schema")
        mock_convert_columnar_to_schema.assert_not_called()
        mock_convert_tensor_to_schema.assert_called_once_with(obj)

    def test_constructor_tensor_list(self, mocker):
        # Arrange
        obj = [{"shape": "some_shape"}]
        mock_tensor = mocker.MagicMock()
        mock_tensor.tensors = mocker.MagicMock(return_value="tensor_schema")
        mock_columnar = mocker.MagicMock()
        mock_columnar.columns = mocker.MagicMock(return_value="columnar_schema")
        mock_convert_tensor_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_tensor_to_schema", return_value=mock_tensor
        )
        mock_convert_columnar_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_columnar_to_schema",
            return_value=mock_columnar,
        )

        # Act
        s = schema.Schema(obj)

        # Assert
        assert s.tensor_schema == mock_tensor.tensors
        assert not hasattr(s, "columnar_schema")
        mock_convert_columnar_to_schema.assert_not_called()
        mock_convert_tensor_to_schema.assert_called_once_with(obj)

    def test_constructor_column_list(self, mocker):
        # Arrange
        obj = [{"no_shape": "nothing"}]
        mock_tensor = mocker.MagicMock()
        mock_tensor.tensors = mocker.MagicMock(return_value="tensor_schema")
        mock_columnar = mocker.MagicMock()
        mock_columnar.columns = mocker.MagicMock(return_value="columnar_schema")
        mock_convert_tensor_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_tensor_to_schema", return_value=mock_tensor
        )
        mock_convert_columnar_to_schema = mocker.patch(
            "hsml.schema.Schema._convert_columnar_to_schema",
            return_value=mock_columnar,
        )

        # Act
        s = schema.Schema(obj)

        # Assert
        assert s.columnar_schema == mock_columnar.columns
        assert not hasattr(s, "tensor_schema")
        mock_convert_tensor_to_schema.assert_not_called()
        mock_convert_columnar_to_schema.assert_called_once_with(obj)

    # convert to schema

    def test_convert_columnar_to_schema(self, mocker):
        # Arrange
        obj = {"key": "value"}
        mock_columnar_schema_init = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema.__init__",
            return_value=None,
        )
        mock_schema = mocker.MagicMock()
        mock_schema._convert_columnar_to_schema = (
            schema.Schema._convert_columnar_to_schema
        )

        # Act
        ret = mock_schema._convert_columnar_to_schema(mock_schema, obj)

        # Assert
        assert isinstance(ret, schema.ColumnarSchema)
        mock_columnar_schema_init.assert_called_once_with(obj)

    def test_convert_tensor_to_schema(self, mocker):
        # Arrange
        obj = {"key": "value"}
        mock_tensor_schema_init = mocker.patch(
            "hsml.utils.schema.tensor_schema.TensorSchema.__init__",
            return_value=None,
        )
        mock_schema = mocker.MagicMock()
        mock_schema._convert_tensor_to_schema = schema.Schema._convert_tensor_to_schema

        # Act
        ret = mock_schema._convert_tensor_to_schema(mock_schema, obj)

        # Assert
        assert isinstance(ret, schema.TensorSchema)
        mock_tensor_schema_init.assert_called_once_with(obj)

    # get type

    def test_get_type_none(self, mocker):
        # Arrange
        class MockSchema:
            pass

        mock_schema = MockSchema()
        mock_schema._get_type = schema.Schema._get_type

        # Act
        t = mock_schema._get_type(mock_schema)

        # Assert
        assert t is None

    def test_get_type_tensor(self, mocker):
        # Arrange
        class MockSchema:
            tensor_schema = {}

        mock_schema = MockSchema()
        mock_schema._get_type = schema.Schema._get_type

        # Act
        t = mock_schema._get_type(mock_schema)

        # Assert
        assert t == "tensor"

    def test_get_type_columnar(self, mocker):
        # Arrange
        class MockSchema:
            columnar_schema = {}

        mock_schema = MockSchema()
        mock_schema._get_type = schema.Schema._get_type

        # Act
        t = mock_schema._get_type(mock_schema)

        # Assert
        assert t == "columnar"
