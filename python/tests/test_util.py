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

import asyncio
import os
from datetime import date, datetime
from urllib.parse import ParseResult

import hopsworks_common.util
import pytest
import pytz
from hopsworks_common import util
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.constants import MODEL
from hopsworks_common.core.constants import HAS_AIOMYSQL, HAS_SQLALCHEMY
from hsfs.embedding import EmbeddingFeature, EmbeddingIndex
from hsfs.feature import Feature
from hsml.llm.model import Model as LLMModel
from hsml.llm.predictor import Predictor as LLMPredictor
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
from mock import patch


if HAS_SQLALCHEMY and HAS_AIOMYSQL:
    from hsfs.core import util_sql


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

    def test_set_model_class_llm(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_llm"]["response"]["items"][0]

        # Act
        model = util.set_model_class(json)

        # Assert
        assert isinstance(model, LLMModel)
        assert model.framework == MODEL.FRAMEWORK_LLM

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
        mock_handle_tensor_input = mocker.patch(
            "hopsworks_common.util._handle_tensor_input"
        )
        mock_handle_dataframe_input = mocker.patch(
            "hopsworks_common.util._handle_dataframe_input"
        )
        mock_handle_dict_input = mocker.patch(
            "hopsworks_common.util._handle_dict_input"
        )

        # Act
        util.input_example_to_json(input_example_numpy)

        # Assert
        mock_handle_tensor_input.assert_called_once()
        mock_handle_dict_input.assert_not_called()
        mock_handle_dataframe_input.assert_not_called()

    def test_input_example_to_json_from_dict(self, mocker, input_example_dict):
        # Arrange
        mock_handle_tensor_input = mocker.patch(
            "hopsworks_common.util._handle_tensor_input"
        )
        mock_handle_dataframe_input = mocker.patch(
            "hopsworks_common.util._handle_dataframe_input"
        )
        mock_handle_dict_input = mocker.patch(
            "hopsworks_common.util._handle_dict_input"
        )

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
        mock_handle_tensor_input = mocker.patch(
            "hopsworks_common.util._handle_tensor_input"
        )
        mock_handle_dataframe_input = mocker.patch(
            "hopsworks_common.util._handle_dataframe_input"
        )
        mock_handle_dict_input = mocker.patch(
            "hopsworks_common.util._handle_dict_input"
        )

        # Act
        util.input_example_to_json(input_example_dataframe_pandas_dataframe)

        # Assert
        mock_handle_tensor_input.assert_not_called()
        mock_handle_dict_input.assert_not_called()
        mock_handle_dataframe_input.assert_called_once()  # default

    def test_input_example_to_json_unsupported(self, mocker):
        # Arrange
        mock_handle_tensor_input = mocker.patch(
            "hopsworks_common.util._handle_tensor_input"
        )
        mock_handle_dataframe_input = mocker.patch(
            "hopsworks_common.util._handle_dataframe_input"
        )
        mock_handle_dict_input = mocker.patch(
            "hopsworks_common.util._handle_dict_input"
        )

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
        pred_llm = mocker.patch("hsml.llm.predictor.Predictor.__init__")

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
        pred_llm.assert_not_called()

    def test_get_predictor_for_model_python(self, mocker, model_python):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch(
            "hsml.python.predictor.Predictor.__init__", return_value=None
        )
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")
        pred_llm = mocker.patch("hsml.llm.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_python)

        # Assert
        assert isinstance(predictor, PyPredictor)
        pred_base.assert_not_called()
        pred_python.assert_called_once()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()
        pred_llm.assert_not_called()

    def test_get_predictor_for_model_sklearn(self, mocker, model_sklearn):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch(
            "hsml.sklearn.predictor.Predictor.__init__", return_value=None
        )
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")
        pred_llm = mocker.patch("hsml.llm.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_sklearn)

        # Assert
        assert isinstance(predictor, SkLearnPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_called_once()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()
        pred_llm.assert_not_called()

    def test_get_predictor_for_model_tensorflow(self, mocker, model_tensorflow):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch(
            "hsml.tensorflow.predictor.Predictor.__init__", return_value=None
        )
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")
        pred_llm = mocker.patch("hsml.llm.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_tensorflow)

        # Assert
        assert isinstance(predictor, TFPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_called_once()
        pred_torch.assert_not_called()
        pred_llm.assert_not_called()

    def test_get_predictor_for_model_torch(self, mocker, model_torch):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch(
            "hsml.torch.predictor.Predictor.__init__", return_value=None
        )
        pred_llm = mocker.patch("hsml.llm.predictor.Predictor.__init__")

        # Act
        predictor = util.get_predictor_for_model(model_torch)

        # Assert
        assert isinstance(predictor, TorchPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_called_once()
        pred_llm.assert_not_called()

    def test_get_predictor_for_model_llm(self, mocker, model_llm):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")
        pred_llm = mocker.patch(
            "hsml.llm.predictor.Predictor.__init__", return_value=None
        )

        # Act
        predictor = util.get_predictor_for_model(model_llm)

        # Assert
        assert isinstance(predictor, LLMPredictor)
        pred_base.assert_not_called()
        pred_python.assert_not_called()
        pred_sklearn.assert_not_called()
        pred_tensorflow.assert_not_called()
        pred_torch.assert_not_called()
        pred_llm.assert_called_once()

    def test_get_predictor_for_model_non_base(self, mocker):
        # Arrange
        pred_base = mocker.patch("hsml.predictor.Predictor.__init__")
        pred_python = mocker.patch("hsml.python.predictor.Predictor.__init__")
        pred_sklearn = mocker.patch("hsml.sklearn.predictor.Predictor.__init__")
        pred_tensorflow = mocker.patch("hsml.tensorflow.predictor.Predictor.__init__")
        pred_torch = mocker.patch("hsml.torch.predictor.Predictor.__init__")
        pred_llm = mocker.patch("hsml.llm.predictor.Predictor.__init__")

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
        pred_llm.assert_not_called()

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
        mock_client.replace_public_host = mocker.MagicMock(return_value=mock_url_parsed)
        mocker.patch("hopsworks_common.client.get_instance", return_value=mock_client)

        # Act
        url = util.get_hostname_replaced_url(sub_path)

        # Assert
        mock_client.replace_public_host.assert_called_once_with(urlparse_href_arg)
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
        get_obj_from_json = mocker.patch("hopsworks_common.util.get_obj_from_json")

        # Act
        b = util.extract_field_from_json(json, "b")

        # Assert
        assert b == "2"
        assert get_obj_from_json.call_count == 0

    def test_extract_field_from_json_fields(self, mocker):
        # Arrange
        json = {"a": "1", "b": "2"}
        get_obj_from_json = mocker.patch("hopsworks_common.util.get_obj_from_json")

        # Act
        b = util.extract_field_from_json(json, ["B", "b"])  # alternative fields

        # Assert
        assert b == "2"
        assert get_obj_from_json.call_count == 0

    def test_extract_field_from_json_as_instance_of_str(self, mocker):
        # Arrange
        json = {"a": "1", "b": "2"}
        get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value="2"
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
            "hopsworks_common.util.get_obj_from_json", return_value="2"
        )

        # Act
        b = util.extract_field_from_json(json, "b", as_instance_of=str)

        # Assert
        assert b == ["2", "2", "2"]
        assert get_obj_from_json.call_count == 3
        assert get_obj_from_json.call_args[1]["obj"] == "2"
        assert get_obj_from_json.call_args[1]["cls"] is str

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

    def test_get_hudi_datestr_from_timestamp(self):
        dt = hopsworks_common.util.get_hudi_datestr_from_timestamp(1640995200000)
        assert dt == "20220101000000000"

    def test_convert_event_time_to_timestamp_timestamp(self):
        dt = hopsworks_common.util.convert_event_time_to_timestamp(1640995200)
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_datetime(self):
        dt = hopsworks_common.util.convert_event_time_to_timestamp(
            datetime(2022, 1, 1, 0, 0, 0)
        )
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_datetime_tz(self):
        dt = hopsworks_common.util.convert_event_time_to_timestamp(
            pytz.timezone("US/Pacific").localize(datetime(2021, 12, 31, 16, 0, 0))
        )
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_date(self):
        dt = hopsworks_common.util.convert_event_time_to_timestamp(date(2022, 1, 1))
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_string(self):
        dt = hopsworks_common.util.convert_event_time_to_timestamp(
            "2022-01-01 00:00:00"
        )
        assert dt == 1640995200000

    def test_convert_iso_event_time_to_timestamp_string(self):
        dt = hopsworks_common.util.convert_event_time_to_timestamp(
            "2022-01-01T00:00:00.000000Z"
        )
        assert dt == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd(self):
        timestamp = hopsworks_common.util.get_timestamp_from_date_string("2022-01-01")
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh(self):
        timestamp = hopsworks_common.util.get_timestamp_from_date_string(
            "2022-01-01 00"
        )
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm(self):
        timestamp = hopsworks_common.util.get_timestamp_from_date_string(
            "2022-01-01 00:00"
        )
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss(self):
        timestamp = hopsworks_common.util.get_timestamp_from_date_string(
            "2022-01-01 00:00:00"
        )
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_f(self):
        timestamp = hopsworks_common.util.get_timestamp_from_date_string(
            "2022-01-01 00:00:00.000"
        )
        assert timestamp == 1640995200000

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_error(self):
        with pytest.raises(ValueError):
            hopsworks_common.util.get_timestamp_from_date_string("2022-13-01 00:00:00")

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_error2(self):
        with pytest.raises(ValueError):
            hopsworks_common.util.get_timestamp_from_date_string("202-13-01 00:00:00")

    def test_convert_event_time_to_timestamp_yyyy_mm_dd_hh_mm_ss_error3(self):
        with pytest.raises(ValueError):
            hopsworks_common.util.get_timestamp_from_date_string("00:00:00 2022-01-01")

    def test_convert_hudi_commit_time_to_timestamp(self):
        timestamp = hopsworks_common.util.get_timestamp_from_date_string(
            "20221118095233099"
        )
        assert timestamp == 1668765153099

    def test_get_dataset_type_HIVEDB(self):
        db_type = hopsworks_common.util.get_dataset_type(
            "/apps/hive/warehouse/temp_featurestore.db/storage_connector_resources/kafka__tstore.jks"
        )
        assert db_type == "HIVEDB"

    def test_get_dataset_type_HIVEDB_with_dfs(self):
        db_type = hopsworks_common.util.get_dataset_type(
            "hdfs:///apps/hive/warehouse/temp_featurestore.db/storage_connector_resources/kafka__tstore.jks"
        )
        assert db_type == "HIVEDB"

    def test_get_dataset_type_DATASET(self):
        db_type = hopsworks_common.util.get_dataset_type(
            "/Projects/temp/Resources/kafka__tstore.jks"
        )
        assert db_type == "DATASET"

    def test_get_dataset_type_DATASET_with_dfs(self):
        db_type = hopsworks_common.util.get_dataset_type(
            "hdfs:///Projects/temp/Resources/kafka__tstore.jks"
        )
        assert db_type == "DATASET"

    def test_get_job_url(self, mocker):
        # Arrange
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")

        # Act
        hopsworks_common.util.get_job_url(href="1/2/3/4/5/6/7/8")

        # Assert
        assert (
            mock_client_get_instance.return_value.replace_public_host.call_args[0][
                0
            ].path
            == "p/5/jobs/named/7/executions"
        )

    def test_get_feature_group_url(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hopsworks_common.util.get_hostname_replaced_url"
        )
        mock_client_get_instance.return_value._project_id = 50

        # Act
        hopsworks_common.util.get_feature_group_url(
            feature_group_id=feature_group_id, feature_store_id=feature_store_id
        )

        # Assert
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args[0][0] == "/p/50/fs/99/fg/10"
        )

    def test_valid_embedding_type(self):
        embedding_index = EmbeddingIndex(
            features=[
                EmbeddingFeature("feature1", 3),
                EmbeddingFeature("feature2", 3),
                EmbeddingFeature("feature3", 3),
                EmbeddingFeature("feature4", 3),
            ]
        )
        # Define a schema with valid feature types
        schema = [
            Feature(name="feature1", type="array<int>"),
            Feature(name="feature2", type="array<bigint>"),
            Feature(name="feature3", type="array<float>"),
            Feature(name="feature4", type="array<double>"),
        ]
        # Call the method and expect no exceptions
        hopsworks_common.util.validate_embedding_feature_type(embedding_index, schema)

    def test_invalid_embedding_type(self):
        embedding_index = EmbeddingIndex(
            features=[
                EmbeddingFeature("feature1", 3),
                EmbeddingFeature("feature2", 3),
            ]
        )
        # Define a schema with an invalid feature type
        schema = [
            Feature(name="feature1", type="array<int>"),
            Feature(name="feature2", type="array<string>"),  # Invalid type
        ]
        # Call the method and expect a FeatureStoreException
        with pytest.raises(FeatureStoreException):
            hopsworks_common.util.validate_embedding_feature_type(
                embedding_index, schema
            )

    def test_missing_embedding_index(self):
        # Define a schema without an embedding index
        schema = [
            Feature(name="feature1", type="array<int>"),
            Feature(name="feature2", type="array<bigint>"),
        ]
        # Call the method with an empty feature_group (no embedding index)
        hopsworks_common.util.validate_embedding_feature_type(None, schema)
        # No exception should be raised

    def test_empty_schema(self):
        embedding_index = EmbeddingIndex(
            features=[
                EmbeddingFeature("feature1", 3),
                EmbeddingFeature("feature2", 3),
            ]
        )
        # Define an empty schema
        schema = []
        # Call the method with an empty schema
        hopsworks_common.util.validate_embedding_feature_type(embedding_index, schema)
        # No exception should be raised

    @pytest.mark.skipif(
        not HAS_SQLALCHEMY or not HAS_AIOMYSQL,
        reason="SQLAlchemy or aiomysql is not installed",
    )
    def test_create_async_engine(self, mocker):
        # Test when get_running_loop() raises a RuntimeError
        with patch("asyncio.get_running_loop", side_effect=RuntimeError):
            # mock storage connector
            online_connector = patch.object(
                hopsworks_common.util, "get_online_connector"
            )
            with pytest.raises(
                RuntimeError,
                match="Event loop is not running. Please invoke this co-routine from a running loop or provide an event loop.",
            ):
                asyncio.run(util_sql.create_async_engine(online_connector, True, 1))
