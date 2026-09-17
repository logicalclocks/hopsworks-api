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
from __future__ import annotations

import os
import threading
from types import SimpleNamespace

import pandas as pd
import pytest
from hopsworks_common.client.exceptions import (
    FeatureStoreException,
    ModelServingException,
)
from hsml import default_predictor as dp
from hsml.deployment_schema import DeploymentSchema


def _status(err):
    return getattr(err, "status_code", None)


def _detail(err):
    return getattr(err, "detail", None)


class FakeFeatureView:
    def __init__(
        self, logging_enabled=False, extra_columns=(), transformation_functions=()
    ):
        self.name = "fv"
        self.version = 1
        self.logging_enabled = logging_enabled
        self.feature_logging = SimpleNamespace(
            extra_logging_columns=[
                SimpleNamespace(name=n, type="string") for n in extra_columns
            ]
        )
        self.transformation_functions = list(transformation_functions)
        self.init_serving_calls = []
        self.feature_logger = None
        self.log_calls = []
        self.log_gate = None  # a test sets it to hold the logging worker inside log()
        self.log_started = threading.Event()
        self.lookup_calls = []
        self.transform_calls = []
        self.on_demand_calls = []
        self.missing_rows = set()
        self.lookup_error = None
        self.log_error = None

    def init_serving(self, **kwargs):
        self.init_serving_calls.append(kwargs)

    def init_feature_logger(self, feature_logger):
        self.feature_logger = feature_logger

    def get_training_dataset_schema(self, version):
        return [
            SimpleNamespace(
                name="cc_num",
                type="bigint",
                label=False,
                inference_helper_column=False,
                training_helper_column=False,
            ),
            SimpleNamespace(
                name="amount",
                type="double",
                label=False,
                inference_helper_column=False,
                training_helper_column=False,
            ),
            SimpleNamespace(
                name="amount_scaled",
                type="double",
                label=False,
                inference_helper_column=False,
                training_helper_column=False,
            ),
            SimpleNamespace(
                name="fraud",
                type="int",
                label=True,
                inference_helper_column=False,
                training_helper_column=False,
            ),
        ]

    def compute_on_demand_features(self, frame, request_parameters=None, **kwargs):
        self.on_demand_calls.append(request_parameters)
        return frame

    def transform(self, frame, **kwargs):
        self.transform_calls.append(frame)
        out = frame.copy()
        out["amount_scaled"] = 0.5
        return out

    def get_feature_vectors(self, entry, allow_missing=False, **kwargs):
        self.lookup_calls.append(
            {"entry": entry, "allow_missing": allow_missing, **kwargs}
        )
        if self.lookup_error is not None and not allow_missing:
            raise self.lookup_error
        rows = []
        for index, e in enumerate(entry):
            if index in self.missing_rows:
                rows.append({"cc_num": None, "amount": None, "amount_scaled": None})
            else:
                rows.append(
                    {"cc_num": e["cc_num"], "amount": 1.0, "amount_scaled": 0.5}
                )
        return pd.DataFrame(rows, columns=["cc_num", "amount", "amount_scaled"])

    def log(self, *args, **kwargs):
        self.log_started.set()
        if self.log_gate is not None:
            self.log_gate.wait(5)
        if self.log_error is not None:
            raise self.log_error
        self.log_calls.append((args, kwargs))


class FakeModel:
    def __init__(self, model_schema, training_dataset_version=3):
        self.name = "fraud"
        self.version = 2
        self.model_schema = model_schema
        self.training_dataset_version = training_dataset_version
        self.feature_view = None
        self.get_feature_view_calls = []

    def get_feature_view(self, init=True, online=False):
        """Like hsml.Model.get_feature_view in a pod: init_serving on the model's training dataset."""
        self.get_feature_view_calls.append({"init": init, "online": online})
        if init and self.feature_view is not None:
            self.feature_view.init_serving(
                training_dataset_version=self.training_dataset_version
            )
        return self.feature_view


class FakeDeployment:
    def __init__(self, schema, feature_view, model=None, monitoring=()):
        self.name = "dep"
        self._schema = schema
        self._feature_view = feature_view
        self._model = model
        if model is not None:
            model.feature_view = (
                feature_view  # a model deployment's view is the model's
            )
        self.training_dataset_version = 3
        self._monitoring = list(monitoring)

    @property
    def schema(self):
        return self._schema

    def get_model(self):
        return self._model

    def get_feature_view(self, init=False):
        return self._feature_view

    def get_monitoring_configs(self):
        return self._monitoring


class Estimator:
    def __init__(self, fail=False):
        self.fail = fail
        self.seen = None

    def predict(self, x):
        if self.fail:
            raise RuntimeError("boom")
        self.seen = x
        return pd.Series([1] * len(x)).to_numpy()


COLUMNAR = {
    "input_schema": {
        "columnar_schema": [
            {"name": "amount_scaled", "type": "float64"},
            {"name": "cc_num", "type": "int64"},
        ]
    }
}


def _schema(**kwargs):
    defaults = {
        "serving_keys": [{"name": "cc_num", "type": "bigint"}],
        "request_parameters": [],
        "feature_view": {"name": "fv", "version": 1},
        "training_dataset_version": 3,
    }
    defaults.update(kwargs)
    return DeploymentSchema(**defaults)


@pytest.fixture
def model_files(tmp_path):
    import pickle

    (tmp_path / "model.pkl").write_bytes(pickle.dumps(Estimator()))
    return tmp_path


@pytest.fixture
def pod_env(monkeypatch, model_files):
    monkeypatch.setenv("MODEL_FILES_PATH", str(model_files))
    monkeypatch.setenv("DEPLOYMENT_NAME", "dep")
    monkeypatch.setenv("DEPLOYMENT_VERSION", "4")


class TestStartup:
    def test_model_deployment_ready(self, pod_env):
        fv = FakeFeatureView()
        model = FakeModel(COLUMNAR)
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, model), async_logger=object()
        )

        assert predictor.model is not None
        assert predictor.model_input_columns == ["amount_scaled", "cc_num"]
        # the view arrives initialised from the model, as in a hand-written predictor
        assert model.get_feature_view_calls == [{"init": True, "online": False}]
        assert fv.init_serving_calls == [{"training_dataset_version": 3}]
        assert fv.feature_logger is None
        assert predictor.logging_enabled is False

    def test_feature_view_deployment_skips_model(self, pod_env):
        predictor = dp.DefaultPredict(FakeDeployment(_schema(), FakeFeatureView()))

        assert predictor.model is None
        assert predictor.hopsworks_model is None

    def test_missing_schema_and_feature_view(self, pod_env):
        with pytest.raises(ModelServingException, match="no deployment schema"):
            dp.DefaultPredict(FakeDeployment(None, FakeFeatureView()))
        with pytest.raises(ModelServingException, match="No feature view is linked"):
            dp.DefaultPredict(FakeDeployment(_schema(), None))

    def test_schema_for_other_feature_view_fails(self, pod_env):
        schema = _schema(feature_view={"name": "other", "version": 1})

        with pytest.raises(
            ModelServingException, match="published for feature view 'other'"
        ):
            dp.DefaultPredict(FakeDeployment(schema, FakeFeatureView()))

    def test_statistics_check_at_startup(self, pod_env):
        tf = SimpleNamespace(
            transformation_type=SimpleNamespace(value="model_dependent"),
            hopsworks_udf=SimpleNamespace(
                function_name="min_max_scaler",
                transformation_features=["amount"],
                statistics_required=True,
            ),
        )
        deployment = FakeDeployment(
            _schema(), FakeFeatureView(transformation_functions=[tf])
        )
        deployment.training_dataset_version = None

        with pytest.raises(ValueError, match="min_max_scaler\\(amount\\)"):
            dp.DefaultPredict(deployment)

    def test_revision_env_vars_win_over_the_current_deployment(
        self, pod_env, monkeypatch
    ):
        """An older revision's pod serves that revision's training dataset.

        The deployment's current value must not leak into it.
        """
        fv = FakeFeatureView()
        deployment = FakeDeployment(_schema(training_dataset_version=3), fv)
        deployment.training_dataset_version = 4  # the deployment moved on
        monkeypatch.setenv("SERVING_TRAINING_DATASET_VERSION", "3")

        predictor = dp.DefaultPredict(deployment)

        assert predictor.training_dataset_version == 3
        assert fv.init_serving_calls[-1]["training_dataset_version"] == 3

    def test_revision_model_fetched_by_the_pods_model_env_vars(
        self, pod_env, monkeypatch
    ):
        current = FakeModel(COLUMNAR, training_dataset_version=4)
        older = FakeModel(COLUMNAR, training_dataset_version=3)
        older.version = 1
        deployment = FakeDeployment(
            _schema(training_dataset_version=3), FakeFeatureView(), current
        )
        deployment.model_name = "fraud"
        deployment.model_version = 2
        deployment.model_registry_id = 9
        deployment._model_api = SimpleNamespace(_get=lambda name, version, mr: older)
        older.get_feature_view = lambda init=False: deployment.get_feature_view()
        monkeypatch.setenv("MODEL_NAME", "fraud")
        monkeypatch.setenv("MODEL_VERSION", "1")

        predictor = dp.DefaultPredict(deployment)

        assert predictor.hopsworks_model is older
        assert predictor.training_dataset_version == 3

    def test_schema_and_statistics_must_agree(self, pod_env, monkeypatch):
        deployment = FakeDeployment(
            _schema(training_dataset_version=3), FakeFeatureView()
        )
        monkeypatch.setenv("SERVING_TRAINING_DATASET_VERSION", "4")

        with pytest.raises(
            ModelServingException,
            match="training dataset v3 but this revision serves v4",
        ):
            dp.DefaultPredict(deployment)

        model = FakeModel(COLUMNAR, training_dataset_version=4)
        with pytest.raises(
            ModelServingException, match="v3 but this revision serves v4"
        ):
            dp.DefaultPredict(
                FakeDeployment(
                    _schema(training_dataset_version=3), FakeFeatureView(), model
                )
            )

    def test_async_logger_attached_when_logging_enabled(self, pod_env, caplog):
        logger = object()
        fv = FakeFeatureView(logging_enabled=True)
        dp.DefaultPredict(FakeDeployment(_schema(), fv), async_logger=logger)
        assert fv.init_serving_calls == [{"training_dataset_version": 3}]
        assert fv.feature_logger is logger

        # no logger from the wrapper: nothing is logged, and the pod says so
        fv = FakeFeatureView(logging_enabled=True)
        predictor = dp.DefaultPredict(FakeDeployment(_schema(), fv))
        assert predictor.logging_enabled is False
        assert fv.feature_logger is None
        assert "provided no feature logger" in caplog.text

        fv = FakeFeatureView(logging_enabled=True)
        dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=logger
        )
        assert fv.init_serving_calls == [{"training_dataset_version": 3}]
        assert fv.feature_logger is logger

    def test_reserved_columns_detected_and_undeclared_warned(self, pod_env, caplog):
        fv = FakeFeatureView(
            logging_enabled=True,
            extra_columns=("deployment_name", "request_row", "channel"),
        )
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv), async_logger=object()
        )

        assert predictor.reserved_columns == ["deployment_name", "request_row"]
        assert "channel" in caplog.text

    def test_monitoring_without_logging_warns_and_serves(self, pod_env, caplog):
        config = SimpleNamespace(name="psi")
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), FakeFeatureView(), monitoring=[config])
        )

        assert predictor.monitoring_configs == [config]
        assert "feature logging is disabled" in caplog.text


class TestLoadModel:
    def test_zero_or_many_candidates_fail(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MODEL_FILES_PATH", str(tmp_path))
        with pytest.raises(ModelServingException, match="found: none"):
            dp.DefaultPredict(
                FakeDeployment(_schema(), FakeFeatureView(), FakeModel(COLUMNAR))
            )

        (tmp_path / "a.pkl").write_bytes(b"x")
        (tmp_path / "b.joblib").write_bytes(b"x")
        with pytest.raises(ModelServingException, match="a.pkl"):
            dp.DefaultPredict(
                FakeDeployment(_schema(), FakeFeatureView(), FakeModel(COLUMNAR))
            )


class TestModelInputContract:
    def test_cardinality_is_enforced_before_logging(self, pod_env):
        fv = FakeFeatureView(logging_enabled=True)
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=object()
        )
        original = fv.get_feature_vectors
        fv.get_feature_vectors = lambda entry, **kw: original(entry[:1], **kw)

        with pytest.raises(Exception) as info:
            predictor.predict([{"cc_num": 1}, {"cc_num": 2}])

        assert _status(info.value) == 500
        assert _detail(info.value)["code"] == "CONTRACT_VIOLATION"
        assert fv.log_calls == []

        fv.get_feature_vectors = original
        predictor.model.predict = lambda x: pd.Series([1]).to_numpy()
        with pytest.raises(Exception) as info:
            predictor.predict([{"cc_num": 1}, {"cc_num": 2}])
        assert _detail(info.value)["code"] == "CONTRACT_VIOLATION"
        assert "1 predictions for 2 rows" in _detail(info.value)["message"]
        assert fv.log_calls == []

    def test_feature_view_output_columns_checked_against_schema(self, pod_env):
        schema = _schema(
            output={
                "kind": "feature_vectors",
                "columns": [
                    {"name": "cc_num", "type": "bigint"},
                    {"name": "other", "type": "double"},
                ],
            }
        )
        predictor = dp.DefaultPredict(FakeDeployment(schema, FakeFeatureView()))

        with pytest.raises(Exception) as info:
            predictor.predict([{"cc_num": 1}])

        assert _detail(info.value)["code"] == "CONTRACT_VIOLATION"

    def test_every_error_carries_the_request_id(self, pod_env):
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), FakeFeatureView(), FakeModel(COLUMNAR))
        )

        with pytest.raises(Exception) as info:
            predictor.predict([{"cc_num": 1, "extra": 2}], request_id="req-7")

        assert _detail(info.value)["request_id"] == "req-7"

    def _predictor(self, pod_env, model_schema):
        return dp.DefaultPredict(
            FakeDeployment(_schema(), FakeFeatureView(), FakeModel(model_schema))
        )

    def test_missing_schema(self, pod_env):
        with pytest.raises(ModelServingException, match="no model schema"):
            self._predictor(pod_env, None)

    def test_missing_column(self, pod_env):
        schema = {
            "input_schema": {"columnar_schema": [{"name": "nope", "type": "float64"}]}
        }
        with pytest.raises(
            ModelServingException, match=r"\['nope'\] are not in the transformed"
        ):
            self._predictor(pod_env, schema)

    def test_incompatible_type(self, pod_env):
        schema = {
            "input_schema": {
                "columnar_schema": [
                    {"name": "amount_scaled", "type": "object"},
                    {"name": "cc_num", "type": "bool"},
                ]
            }
        }
        with pytest.raises(
            ModelServingException, match="cc_num: model bool, feature view bigint"
        ):
            self._predictor(pod_env, schema)

    def test_tensor_shapes(self, pod_env):
        ok = {
            "input_schema": {"tensor_schema": [{"shape": [-1, 3], "type": "float32"}]}
        }
        predictor = self._predictor(pod_env, ok)
        assert predictor.model_input_is_tensor is True
        assert predictor.model_input_columns == ["cc_num", "amount", "amount_scaled"]

        bad = {
            "input_schema": {"tensor_schema": [{"shape": [-1, 7], "type": "float32"}]}
        }
        with pytest.raises(ModelServingException, match="has 3 columns"):
            self._predictor(pod_env, bad)

    def test_neither(self, pod_env):
        with pytest.raises(
            ModelServingException, match="neither a columnar nor a tensor"
        ):
            self._predictor(pod_env, {"input_schema": {}})


class TestPredict:
    def _model_predictor(self, pod_env, fv=None):
        fv = fv or FakeFeatureView()
        deployment = FakeDeployment(_schema(), fv, FakeModel(COLUMNAR))
        return dp.DefaultPredict(deployment, async_logger=object()), fv

    def test_predictions_and_column_selection(self, pod_env):
        predictor, fv = self._model_predictor(pod_env)

        result = predictor.predict([{"cc_num": 1}, {"cc_num": 2}])

        assert result == [1, 1]
        assert list(predictor.model.seen.columns) == ["amount_scaled", "cc_num"]
        assert fv.lookup_calls[0]["entry"] == [{"cc_num": 1}, {"cc_num": 2}]
        assert fv.lookup_calls[0]["passed_features"] is None
        assert fv.log_calls == []

    def test_feature_view_deployment_returns_vectors(self, pod_env):
        predictor = dp.DefaultPredict(FakeDeployment(_schema(), FakeFeatureView()))

        result = predictor.predict([{"cc_num": 7}])

        assert result == {
            "predictions": [[7, 1.0, 0.5]],
            "columns": ["cc_num", "amount", "amount_scaled"],
        }

    def test_schema_validation_400(self, pod_env):
        predictor, _ = self._model_predictor(pod_env)

        with pytest.raises(Exception) as info:
            predictor.predict([{"cc_num": 1, "extra": 2}])

        assert _status(info.value) == 400
        assert _detail(info.value)["code"] == "SCHEMA_VALIDATION"
        assert _detail(info.value)["errors"] == [
            {"row": 0, "field": "extra", "reason": "unknown field"}
        ]

    def test_batch_too_large_413(self, pod_env):
        deployment = FakeDeployment(
            _schema()._with_max_batch_rows(2), FakeFeatureView(), FakeModel(COLUMNAR)
        )
        predictor = dp.DefaultPredict(deployment, async_logger=object())
        assert predictor.max_batch_rows == 2  # the limit travels with the schema

        with pytest.raises(Exception) as info:
            predictor.predict([[1], [2], [3]])

        assert _status(info.value) == 413

    def test_entity_not_found_404_with_rows(self, pod_env):
        fv = FakeFeatureView()
        fv.lookup_error = FeatureStoreException("primary key entry cannot be found")
        fv.missing_rows = {1}
        predictor, _ = self._model_predictor(pod_env, fv)

        with pytest.raises(Exception) as info:
            predictor.predict([[1], [2], [3]])

        assert _status(info.value) == 404
        assert _detail(info.value)["code"] == "ENTITY_NOT_FOUND"
        assert [e["row"] for e in _detail(info.value)["errors"]] == [1]

    def test_lookup_failure_400_when_no_row_missing(self, pod_env):
        fv = FakeFeatureView()
        fv.lookup_error = FeatureStoreException("request parameter missing")
        predictor, _ = self._model_predictor(pod_env, fv)

        with pytest.raises(Exception) as info:
            predictor.predict([[1]])

        assert _status(info.value) == 400
        assert _detail(info.value)["code"] == "FEATURE_LOOKUP_FAILED"

    def test_transformation_and_store_failures(self, pod_env):
        fv = FakeFeatureView()
        fv.lookup_error = ZeroDivisionError("udf")
        predictor, _ = self._model_predictor(pod_env, fv)
        with pytest.raises(Exception) as info:
            predictor.predict([[1]])
        assert _status(info.value) == 422
        assert "ZeroDivisionError" in _detail(info.value)["message"]
        assert "udf" not in _detail(info.value)["message"]
        assert "recorded type" not in _detail(info.value)["message"]
        assert _detail(info.value)["request_id"]

        fv.lookup_error = ConnectionError("down")
        with pytest.raises(Exception) as info:
            predictor.predict([[1]])
        assert _status(info.value) == 503
        assert _detail(info.value)["code"] == "FEATURE_STORE_UNAVAILABLE"

    def test_transformation_failure_names_untyped_request_parameters(self, pod_env):
        fv = FakeFeatureView()
        fv.lookup_error = TypeError("unsupported operand: 'float' and 'str'")
        deployment = FakeDeployment(
            _schema(request_parameters=["budget"]), fv, FakeModel(COLUMNAR)
        )
        predictor = dp.DefaultPredict(deployment, async_logger=object())

        with pytest.raises(Exception) as info:
            predictor.predict([{"cc_num": 1, "budget": "value"}])

        message = _detail(info.value)["message"]
        assert _status(info.value) == 422
        assert _detail(info.value)["code"] == "TRANSFORMATION_FAILED"
        assert "TypeError" in message
        assert "budget" in message and "no recorded type" in message
        # the exception text may carry values: only its type is reported
        assert "unsupported operand" not in message

    def test_model_failure_500(self, pod_env, model_files):
        import pickle

        (model_files / "model.pkl").write_bytes(pickle.dumps(Estimator(fail=True)))
        predictor, _ = self._model_predictor(pod_env)

        with pytest.raises(Exception) as info:
            predictor.predict([[1]])

        assert _status(info.value) == 500
        assert _detail(info.value)["code"] == "MODEL_FAILED"
        # the exception text may carry feature values: only its type is reported
        assert "boom" not in _detail(info.value)["message"]
        assert "RuntimeError" in _detail(info.value)["message"]

    def test_logging_async_forwards_everything_and_swallows_failures(self, pod_env):
        fv = FakeFeatureView(
            logging_enabled=True,
            extra_columns=(
                "deployment_name",
                "deployment_version",
                "deployment_schema_id",
                "request_row",
                "channel",
            ),
        )
        schema = _schema(extra_logging_features=[{"name": "channel", "type": "string"}])
        deployment = FakeDeployment(schema, fv, FakeModel(COLUMNAR))
        predictor = dp.DefaultPredict(deployment, async_logger=object())

        predictor.predict(
            [{"cc_num": 1, "channel": "web"}, {"cc_num": 2}], request_id="req-1"
        )

        assert predictor._log_worker._wait(5)
        args, kwargs = fv.log_calls[0]
        assert kwargs["predictions"] == [1, 1]
        assert kwargs["request_id"] == "req-1"
        assert kwargs["training_dataset_version"] == 3
        assert kwargs["model"] is deployment._model
        assert kwargs["extra_logging_features"] == [
            {
                "channel": "web",
                "deployment_name": "dep",
                "deployment_version": 4,
                "deployment_schema_id": schema.schema_id,
                "request_row": 0,
            },
            {
                "channel": None,
                "deployment_name": "dep",
                "deployment_version": 4,
                "deployment_schema_id": schema.schema_id,
                "request_row": 1,
            },
        ]

        fv.log_error = RuntimeError("kafka")
        assert predictor.predict([[1, None]]) == [1]
        assert predictor._log_worker._wait(5)
        assert predictor._log_worker.failed == 1

    def test_feature_view_deployment_logs_without_model(self, pod_env):
        fv = FakeFeatureView(logging_enabled=True)
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv), async_logger=object()
        )

        predictor.predict([[1]])

        assert predictor._log_worker._wait(5)
        _, kwargs = fv.log_calls[0]
        assert kwargs["predictions"] is None
        assert kwargs["model"] is None
        assert kwargs["request_id"]

    def test_log_runs_off_the_request_thread(self, pod_env):
        fv = FakeFeatureView(logging_enabled=True)
        fv.log_gate = threading.Event()
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=object()
        )

        assert predictor.predict([[1]]) == [1]
        assert fv.log_started.wait(5)
        # the worker is inside log() while the request has already returned
        assert fv.log_calls == []

        fv.log_gate.set()
        assert predictor._log_worker._wait(5)
        assert len(fv.log_calls) == 1

    def test_full_logging_queue_drops_and_counts(self, pod_env, monkeypatch):
        monkeypatch.setenv("FEATURE_LOGGER_QUEUE_SIZE", "2")
        fv = FakeFeatureView(logging_enabled=True)
        fv.log_gate = threading.Event()
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=object()
        )

        predictor.predict([[1]])
        assert fv.log_started.wait(5)  # the worker holds the first request's row
        predictor.predict([[2]])  # fills the budget of two rows
        predictor.predict([[3]])  # dropped
        assert predictor._log_worker.dropped == 1

        fv.log_gate.set()
        assert predictor.close() is True
        assert len(fv.log_calls) == 2

    def test_the_logging_backlog_is_bounded_by_rows_not_requests(
        self, pod_env, monkeypatch
    ):
        monkeypatch.setenv("FEATURE_LOGGER_QUEUE_SIZE", "4")
        fv = FakeFeatureView(logging_enabled=True)
        fv.log_gate = threading.Event()
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=object()
        )

        predictor.predict([[1], [2], [3]])  # three rows, held by the worker
        assert fv.log_started.wait(5)
        predictor.predict([[4], [5]])  # 3 + 2 rows exceed the budget: dropped
        assert predictor._log_worker.dropped == 1
        predictor.predict([[6]])  # 3 + 1 rows fit

        fv.log_gate.set()
        assert predictor.close() is True
        assert [len(call[0][0]) for call in fv.log_calls] == [3, 1]

    def test_a_batch_larger_than_the_whole_budget_is_still_logged(
        self, pod_env, monkeypatch
    ):
        monkeypatch.setenv("FEATURE_LOGGER_QUEUE_SIZE", "1")
        fv = FakeFeatureView(logging_enabled=True)
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=object()
        )

        fv.log_gate = threading.Event()
        predictor.predict([[1], [2], [3]])  # three rows against a budget of one
        assert fv.log_started.wait(5)
        # the peak is that one batch: the next request finds the backlog
        # non-empty and is refused, so nothing accumulates behind it
        predictor.predict([[4]])
        assert predictor._log_worker.dropped == 1

        fv.log_gate.set()
        assert predictor.close() is True
        assert [len(call[0][0]) for call in fv.log_calls] == [3]

    def test_close_drains_the_queue(self, pod_env):
        fv = FakeFeatureView(logging_enabled=True)
        predictor = dp.DefaultPredict(
            FakeDeployment(_schema(), fv, FakeModel(COLUMNAR)), async_logger=object()
        )
        for _ in range(5):
            predictor.predict([[1]])

        assert predictor.close() is True
        assert len(fv.log_calls) == 5
        assert predictor._log_worker.failed == 0


class TestWrapperHandover:
    def test_execs_launcher_with_deployment_name(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_NAME", "dep")
        monkeypatch.setattr(dp.shutil, "which", lambda name: "/usr/bin/" + name)
        calls = []
        monkeypatch.setattr(
            dp.os, "execv", lambda path, argv: calls.append((path, argv))
        )

        dp.run_kserve_wrapper()

        assert calls == [
            (
                "/usr/bin/kserve_server_launcher.sh",
                [
                    "/usr/bin/kserve_server_launcher.sh",
                    "--model_name",
                    "dep",
                    "--api_protocol",
                    "v1",
                ],
            )
        ]

    def test_missing_launcher_or_name(self, monkeypatch):
        monkeypatch.setattr(dp.shutil, "which", lambda name: None)
        with pytest.raises(ModelServingException, match="not on PATH"):
            dp.run_kserve_wrapper()

        monkeypatch.setattr(dp.shutil, "which", lambda name: "/x/" + name)
        monkeypatch.delenv("DEPLOYMENT_NAME", raising=False)
        with pytest.raises(ModelServingException, match="DEPLOYMENT_NAME"):
            dp.run_kserve_wrapper()

    def test_stub_imports_without_handing_over(self, tmp_path, monkeypatch):
        import importlib.util

        stub = tmp_path / "default_predictor.py"
        stub.write_text(dp.STUB_SCRIPT)
        monkeypatch.setattr(
            dp.os, "execv", lambda *a: pytest.fail("hand-over ran on import")
        )
        spec = importlib.util.spec_from_file_location("default_predictor_stub", stub)
        module = importlib.util.module_from_spec(spec)

        spec.loader.exec_module(module)

        assert module.Predict is dp.DefaultPredict
        assert "run_kserve_wrapper" in dp.STUB_SCRIPT
        assert os.path.basename(str(stub)) == "default_predictor.py"


def test_frame_to_rows_keeps_collections_distinct_from_null():
    import pandas as pd

    frame = pd.DataFrame(
        {
            "arr": [[], [None], [1.0, 2.0]],
            "scalar": [None, float("nan"), "x"],
        }
    )

    assert dp._frame_to_rows(frame) == [
        [[], None],
        [[None], None],
        [[1.0, 2.0], "x"],
    ]


class TestNoLookup:
    def test_all_features_passed_transforms_without_lookup(self, pod_env):
        fv = FakeFeatureView(logging_enabled=True)
        schema = _schema(
            serving_keys=[],
            passed_features=[
                {"name": "cc_num", "type": "bigint"},
                {"name": "amount", "type": "double"},
            ],
            request_parameters=[{"name": "rate", "type": "double"}],
        )
        predictor = dp.DefaultPredict(
            FakeDeployment(schema, fv, FakeModel(COLUMNAR)), async_logger=object()
        )

        result = predictor.predict(
            [
                {"cc_num": 1, "amount": 2.0, "rate": 0.1},
                {"cc_num": 2, "amount": 3.0, "rate": 0.2},
            ]
        )

        assert result == [1, 1]
        assert fv.lookup_calls == []
        assert fv.on_demand_calls == [[{"rate": 0.1}, {"rate": 0.2}]]
        assert list(fv.transform_calls[0].columns) == ["cc_num", "amount"]
        assert predictor._log_worker._wait(5)
        args, kwargs = fv.log_calls[0]
        assert args == (None,)
        assert list(kwargs["untransformed_features"].columns) == ["cc_num", "amount"]
        assert "amount_scaled" in kwargs["transformed_features"].columns

    def test_model_without_feature_view_predicts_on_passed_features(self, pod_env):
        schema = DeploymentSchema(
            passed_features=[
                {"name": "amount_scaled", "type": "double"},
                {"name": "cc_num", "type": "bigint"},
            ]
        )
        predictor = dp.DefaultPredict(
            FakeDeployment(schema, None, FakeModel(COLUMNAR)), async_logger=object()
        )

        assert predictor.feature_view is None
        assert predictor.logging_enabled is False
        assert predictor.model_input_columns == ["amount_scaled", "cc_num"]
        result = predictor.predict([{"amount_scaled": 0.5, "cc_num": 1}])
        assert result == [1]
        assert list(predictor.model.seen.columns) == ["amount_scaled", "cc_num"]

    def test_model_without_any_schema_takes_the_passed_features_in_order(self, pod_env):
        schema = DeploymentSchema(
            passed_features=[
                {"name": "cc_num", "type": None},
                {"name": "amount_scaled", "type": None},
            ]
        )
        predictor = dp.DefaultPredict(
            FakeDeployment(schema, None, FakeModel(None)), async_logger=object()
        )

        assert predictor.model_input_columns == ["cc_num", "amount_scaled"]
        assert predictor.predict([{"cc_num": 1, "amount_scaled": 0.5}]) == [1]
        assert list(predictor.model.seen.columns) == ["cc_num", "amount_scaled"]

    def test_model_without_feature_view_needs_every_input_passed(self, pod_env):
        schema = DeploymentSchema(
            passed_features=[{"name": "cc_num", "type": "bigint"}]
        )
        with pytest.raises(ModelServingException, match="not passed features"):
            dp.DefaultPredict(
                FakeDeployment(schema, None, FakeModel(COLUMNAR)), async_logger=object()
            )
