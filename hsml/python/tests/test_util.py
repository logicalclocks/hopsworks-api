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

import os
from urllib.parse import ParseResult

import pytest
from hsml import util
from hsml.constants import MODEL
from hsml.model import Model as BaseModel
from hsml.predictor import Predictor as BasePredictor
from hsml.python.model import Model as PythonModel
from hsml.python.predictor import Predictor as PyPredictor
from hsml.sklearn.model import Model as SklearnModel
from hsml.sklearn.predictor import Predictor as SkLearnPredictor
from hsml.tensorflow.model import Model as TensorflowModel
from hsml.tensorflow.predictor import Predictor as TFPredictor
from hsml.torch.model import Model as TorchModel
from hsml.torch.predictor import Predictor as TorchPredictor


class TestUtil:
    # schema and types

    # - set_model_class

    def test_set_model_class_base(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_base"]["response"]["items"][0]

        # Act
        model = util.set_model_class(json)

        # Assert
        assert isinstance(model, BaseModel)
        assert model.framework is None

    def test_set_model_class_python(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_python"]["response"]["items"][0]

        # Act
        model = util.set_model_class(json)

        # Assert
        assert isinstance(model, PythonModel)
        assert model.framework == MODEL.FRAMEWORK_PYTHON

    def test_set_model_class_sklearn(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_sklearn"]["response"]["items"][0]

        # Act
        model = util.set_model_class(json)

        # Assert
        assert isinstance(model, SklearnModel)
        assert model.framework == MODEL.FRAMEWORK_SKLEARN

    def test_set_model_class_tensorflow(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_tensorflow"]["response"]["items"][0]

        # Act
        model = util.set_model_class(json)

        # Assert
        assert isinstance(model, TensorflowModel)
        assert model.framework == MODEL.FRAMEWORK_TENSORFLOW

    def test_set_model_class_torch(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_torch"]["response"]["items"][0]

        # Act
        model = util.set_model_class(json)

        # Assert
        assert isinstance(model, TorchModel)
        assert model.framework == MODEL.FRAMEWORK_TORCH

    def test_set_model_class_unsupported(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_base"]["response"]["items"][0]
        json["framework"] = "UNSUPPORTED"

        # Act
        with pytest.raises(ValueError) as e_info:
            util.set_model_class(json)

        # Assert
        assert "is not a supported framework" in str(e_info.value)

    # - input_example_to_json

    def test_input_example_to_json_from_numpy(self, mocker, input_example_numpy):
        # Arrange
        mock_handle_tensor_input = mocker.patch("hsml.util._handle_tensor_input")
        mock_handle_dataframe_input = mocker.patch("hsml.util._handle_dataframe_input")
        mock_handle_dict_input = mocker.patch("hsml.util._handle_dict_input")

        # Act
        util.input_example_to_json(input_example_numpy)

        # Assert
        mock_handle_tensor_input.assert_called_once()
        mock_handle_dict_input.assert_not_called()
        mock_handle_dataframe_input.assert_not_called()

    def test_input_example_to_json_from_dict(self, mocker, input_example_dict):
        # Arrange
        mock_handle_tensor_input = mocker.patch("hsml.util._handle_tensor_input")
        mock_handle_dataframe_input = mocker.patch("hsml.util._handle_dataframe_input")
        mock_handle_dict_input = mocker.patch("hsml.util._handle_dict_input")

        # Act
        util.input_example_to_json(input_example_dict)

        # Assert
        mock_handle_tensor_input.assert_not_called()
        mock_handle_dict_input.assert_called_once()
        mock_handle_dataframe_input.assert_not_called()

    def test_input_example_to_json_from_dataframe(
        self, mocker, input_example_dataframe_pandas_dataframe
    ):
        # Arrange
        mock_handle_tensor_input = mocker.patch("hsml.util._handle_tensor_input")
        mock_handle_dataframe_input = mocker.patch("hsml.util._handle_dataframe_input")
        mock_handle_dict_input = mocker.patch("hsml.util._handle_dict_input")

        # Act
        util.input_example_to_json(input_example_dataframe_pandas_dataframe)

        # Assert
        mock_handle_tensor_input.assert_not_called()
        mock_handle_dict_input.assert_not_called()
        mock_handle_dataframe_input.assert_called_once()  # default

    def test_input_example_to_json_unsupported(self, mocker):
        # Arrange
        mock_handle_tensor_input = mocker.patch("hsml.util._handle_tensor_input")
        mock_handle_dataframe_input = mocker.patch("hsml.util._handle_dataframe_input")
        mock_handle_dict_input = mocker.patch("hsml.util._handle_dict_input")

        # Act
        util.input_example_to_json(lambda unsupported_type: None)

        # Assert
        mock_handle_tensor_input.assert_not_called()
        mock_handle_dict_input.assert_not_called()
        mock_handle_dataframe_input.assert_called_once()  # default

    # - handle input examples

    def test_handle_dataframe_input_pandas_dataframe(
        self,
        input_example_dataframe_pandas_dataframe,
        input_example_dataframe_pandas_dataframe_empty,
        input_example_dataframe_list,
    ):
        # Act
        json = util._handle_dataframe_input(input_example_dataframe_pandas_dataframe)
        with pytest.raises(ValueError) as e_info:
            util._handle_dataframe_input(input_example_dataframe_pandas_dataframe_empty)

        # Assert
        assert isinstance(json, list)
        assert json == input_example_dataframe_list
        assert "can not be empty" in str(e_info.value)

    def test_handle_dataframe_input_pandas_dataframe_series(
        self,
        input_example_dataframe_pandas_series,
        input_example_dataframe_pandas_series_empty,
        input_example_dataframe_list,
    ):
        # Act
        json = util._handle_dataframe_input(input_example_dataframe_pandas_series)
        with pytest.raises(ValueError) as e_info:
            util._handle_dataframe_input(input_example_dataframe_pandas_series_empty)

        # Assert
        assert isinstance(json, list)
        assert json == input_example_dataframe_list
        assert "can not be empty" in str(e_info.value)

    def test_handle_dataframe_input_list(self, input_example_dataframe_list):
        # Act
        json = util._handle_dataframe_input(input_example_dataframe_list)

        # Assert
        assert isinstance(json, list)
        assert json == input_example_dataframe_list

    def test_handle_dataframe_input_unsupported(self):
        # Act
        with pytest.raises(TypeError) as e_info:
            util._handle_dataframe_input(lambda unsupported: None)

        # Assert
        assert "is not a supported input example type" in str(e_info.value)

    def test_handle_tensor_input(
        self, input_example_numpy, input_example_dataframe_list
    ):
        # Act
        json = util._handle_tensor_input(input_example_numpy)

        # Assert
        assert isinstance(json, list)
        assert json == input_example_dataframe_list

    def test_handle_dict_input(self, input_example_dict):
        # Act
        json = util._handle_dict_input(input_example_dict)

        # Assert
        assert isinstance(json, dict)
        assert json == input_example_dict

    # artifacts

    def test_compress_dir(self, mocker):
        # Arrange
        archive_name = "archive_name"
        path_to_archive = os.path.join("this", "is", "the", "path", "to", "archive")
        archive_out_path = os.path.join(
            "this", "is", "the", "output", "path", "to", "archive"
        )
        full_archive_out_path = os.path.join(archive_out_path, archive_name)
        mock_isdir = mocker.patch("os.path.isdir", return_value=True)
        mock_shutil_make_archive = mocker.patch(
            "shutil.make_archive", return_value="resulting_path"
        )

        # Act
        path = util.compress(archive_out_path, archive_name, path_to_archive)

        # Assert
        assert path == "resulting_path"
        mock_isdir.assert_called_once_with(path_to_archive)
        mock_shutil_make_archive.assert_called_once_with(
            full_archive_out_path, "gztar", path_to_archive
        )

    def test_compress_file(self, mocker):
        # Arrange
        archive_name = "archive_name"
        path_to_archive = os.path.join("path", "to", "archive")
        archive_out_path = os.path.join("output", "path", "to", "archive")
        full_archive_out_path = os.path.join(archive_out_path, archive_name)
        archive_path_dirname = os.path.join("path", "to")
        archive_path_basename = "archive"
        mock_isdir = mocker.patch("os.path.isdir", return_value=False)
        mock_shutil_make_archive = mocker.patch(
            "shutil.make_archive", return_value="resulting_path"
        )

        # Act
        path = util.compress(archive_out_path, archive_name, path_to_archive)

        # Assert
        assert path == "resulting_path"
        mock_isdir.assert_called_once_with(path_to_archive)
        mock_shutil_make_archive.assert_called_once_with(
            full_archive_out_path, "gztar", archive_path_dirname, archive_path_basename
        )

    def test_decompress(self, mocker):
        # Arrange
        archive_file_path = os.path.join("path", "to", "archive", "file")
        extract_dir = False
        mock_shutil_unpack_archive = mocker.patch(
            "shutil.unpack_archive", return_value="resulting_path"
        )

        # Act
        path = util.decompress(archive_file_path, extract_dir)

        # Assert
        assert path == "resulting_path"
        mock_shutil_unpack_archive.assert_called_once_with(
            archive_file_path, extract_dir=extract_dir
        )

    # export models

    def test_validate_metrics(self, model_metrics):
        # Act
        util.validate_metrics(model_metrics)

        # Assert
        # noop

    def test_validate_metrics_unsupported_type(self, model_metrics_wrong_type):
        # Act
        with pytest.raises(TypeError) as e_info:
            util.validate_metrics(model_metrics_wrong_type)

        # Assert
        assert "expected a dict" in str(e_info.value)

    def test_validate_metrics_unsupported_metric_type(
        self, model_metrics_wrong_metric_type
    ):
        # Act
        with pytest.raises(TypeError) as e_info:
            util.validate_metrics(model_metrics_wrong_metric_type)

        # Assert
        assert "expected a string" in str(e_info.value)

    def test_validate_metrics_unsupported_metric_value(
        self, model_metrics_wrong_metric_value
    ):
        # Act
        with pytest.raises(ValueError) as e_info:
            util.validate_metrics(model_metrics_wrong_metric_value)

        # Assert
        assert "is not a number" in str(e_info.value)

    # model serving

    def test_get_predictor_for_model_base(self, mocker, model_base):
        # Arrange
        def pred_base_spec(model_framework, model_server):
            pass

        pred_base = mocker.patch(
            "hsml.predictor.Predictor.__init__", return_value=None, spec=pred_base_spec
        )
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_base)

        # Assert
        assert isinstance(predictor, BasePredictor)
        pred_base.assert_called_once_with(
            model_framework=MODEL.FRAMEWORK_PYTHON, model_server=MODEL.FRAMEWORK_PYTHON
        )
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()

    def test_get_predictor_for_model_python(self, mocker, model_python):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch(
            "hsml.python.predictor.Predictor.__init__", return_value=None
        )
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_python)

        # Assert
        assert isinstance(predictor, PyPredictor)
        pred_base.assert_not_called()
        pred_python.assert_called_once()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()

    def test_get_predictor_for_model_sklearn(self, mocker, model_sklearn):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch(
            "hsml.sklearn.predictor.Predictor.__init__", return_value=None
        )
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_sklearn)

        # Assert
        assert isinstance(predictor, SkLearnPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_called_once()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()

    def test_get_predictor_for_model_tensorflow(self, mocker, model_tensorflow):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch(
            "hsml.tensorflow.predictor.Predictor.__init__", return_value=None
        )
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_tensorflow)

        # Assert
        assert isinstance(predictor, TFPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_called_once()
        pred_torch.assert_not_called()

    def test_get_predictor_for_model_torch(self, mocker, model_torch):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch(
            "hsml.torch.predictor.Predictor.__init__", return_value=None
        )

        # Act
        predictor = util.get_predictor_for_model(model_torch)

        # Assert
        assert isinstance(predictor, TorchPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_called_once()

    def test_get_predictor_for_model_non_base(self, mocker):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")

        class NonBaseModel:
            pass

        # Act
        with pytest.raises(ValueError) as e_info:
            util.get_predictor_for_model(NonBaseModel())

        assert "an instance of {} class is expected".format(BaseModel) in str(
            e_info.value
        )
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()

    def test_get_hostname_replaced_url(self, mocker):
        # Arrange
        sub_path = "this/is/a/sub_path"
        base_url = "/hopsworks/api/base/"
        urlparse_href_arg = ParseResult(
            scheme="",
            netloc="",
            path=base_url + sub_path,
            params="",
            query="",
            fragment="",
        )
        geturl_return = "final_url"
        mock_url_parsed = mocker.MagicMock()
        mock_url_parsed.geturl = mocker.MagicMock(return_value=geturl_return)
        mock_client = mocker.MagicMock()
        mock_client._base_url = base_url + "url"
        mock_client._replace_public_host = mocker.MagicMock(
            return_value=mock_url_parsed
        )
        mocker.patch("hsml.client.get_instance", return_value=mock_client)

        # Act
        url = util.get_hostname_replaced_url(sub_path)

        # Assert
        mock_client._replace_public_host.assert_called_once_with(urlparse_href_arg)
        mock_url_parsed.geturl.assert_called_once()
        assert url == geturl_return

    # general

    def test_get_members(self):
        # Arrange
        class TEST:
            TEST_1 = 1
            TEST_2 = "two"
            TEST_3 = "3"

        # Act
        members = list(util.get_members(TEST))

        # Assert
        assert members == [1, "two", "3"]

    def test_get_members_with_prefix(self):
        # Arrange
        class TEST:
            TEST_1 = 1
            TEST_2 = "two"
            RES_3 = "3"
            NONE = None

        # Act
        members = list(util.get_members(TEST, prefix="TEST"))

        # Assert
        assert members == [1, "two"]

    # json

    def test_extract_field_from_json(self, mocker):
        # Arrange
        json = {"a": "1", "b": "2"}
        get_obj_from_json = mocker.patch("hsml.util.get_obj_from_json")

        # Act
        b = util.extract_field_from_json(json, "b")

        # Assert
        assert b == "2"
        assert get_obj_from_json.call_count == 0

    def test_extract_field_from_json_fields(self, mocker):
        # Arrange
        json = {"a": "1", "b": "2"}
        get_obj_from_json = mocker.patch("hsml.util.get_obj_from_json")

        # Act
        b = util.extract_field_from_json(json, ["B", "b"])  # alternative fields

        # Assert
        assert b == "2"
        assert get_obj_from_json.call_count == 0

    def test_extract_field_from_json_as_instance_of_str(self, mocker):
        # Arrange
        json = {"a": "1", "b": "2"}
        get_obj_from_json = mocker.patch(
            "hsml.util.get_obj_from_json", return_value="2"
        )

        # Act
        b = util.extract_field_from_json(json, "b", as_instance_of=str)

        # Assert
        assert b == "2"
        get_obj_from_json.assert_called_once_with(obj="2", cls=str)

    def test_extract_field_from_json_as_instance_of_list_str(self, mocker):
        # Arrange
        json = {"a": "1", "b": ["2", "2", "2"]}
        get_obj_from_json = mocker.patch(
            "hsml.util.get_obj_from_json", return_value="2"
        )

        # Act
        b = util.extract_field_from_json(json, "b", as_instance_of=str)

        # Assert
        assert b == ["2", "2", "2"]
        assert get_obj_from_json.call_count == 3
        assert get_obj_from_json.call_args[1]["obj"] == "2"
        assert get_obj_from_json.call_args[1]["cls"] == str

    def test_get_obj_from_json_cls(self, mocker):
        # Arrange
        class Test:
            def __init__(self):
                self.a = "1"

        # Act
        obj = util.get_obj_from_json(Test(), Test)

        # Assert
        assert isinstance(obj, Test)
        assert obj.a == "1"

    def test_get_obj_from_json_dict(self, mocker):
        # Arrange
        class Test:
            def __init__(self, a):
                self.a = a

            @classmethod
            def from_json(cls, json):
                return cls(**json)

        # Act
        obj = util.get_obj_from_json({"a": "1"}, Test)

        # Assert
        assert isinstance(obj, Test)
        assert obj.a == "1"

    def test_get_obj_from_json_dict_default(self, mocker):
        # Arrange
        class Test:
            def __init__(self, a="11"):
                self.a = "11"

            @classmethod
            def from_json(cls, json):
                return cls(**json)

        # Act
        obj = util.get_obj_from_json({}, Test)

        # Assert
        assert isinstance(obj, Test)
        assert obj.a == "11"

    def test_get_obj_from_json_unsupported(self, mocker):
        # Arrange
        class Test:
            pass

        # Act
        with pytest.raises(ValueError) as e_info:
            util.get_obj_from_json("UNSUPPORTED", Test)

        # Assert
        assert "cannot be converted to class" in str(e_info.value)
