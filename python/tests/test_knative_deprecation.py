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

import subprocess
import sys
import textwrap
import warnings

import pytest
from hopsworks_common import util
from hopsworks_common.constants import MODEL, PREDICTOR
from hsml import deployment, model, model_serving, predictor


RECIPE = "deployment.knative_mode = False"


def _knative_warnings(record):
    return [
        w
        for w in record
        if issubclass(w.category, (DeprecationWarning, util.KnativeModeWarning))
        and "Knative" in str(w.message)
    ]


@pytest.fixture
def serving_variables(mocker):
    mocker.patch(
        "hopsworks_common.client._get_serving_num_instances_limits", return_value=[-1]
    )
    mocker.patch(
        "hopsworks_common.client._is_scale_to_zero_required", return_value=False
    )
    mocker.patch("hopsworks_common.client._is_saas_connection", return_value=False)
    mocker.patch("hopsworks_common.client._is_kserve_installed", return_value=True)


@pytest.fixture
def make_predictor(serving_variables):
    def _make(knative_mode=None):
        return predictor.Predictor(
            name="my_model",
            model_server=PREDICTOR.MODEL_SERVER_PYTHON,
            model_name="my_model",
            model_version=1,
            model_framework=MODEL.FRAMEWORK_SKLEARN,
            knative_mode=knative_mode,
        )

    return _make


@pytest.fixture
def ms():
    return model_serving.ModelServing(project_name="proj", project_id=99)


class TestHelper:
    def test_attributed_to_caller_of_public_method(self):
        def public_method():
            util._warn_knative_mode_deprecated()

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            public_method()  # line attributed to the warning

        (w,) = _knative_warnings(record)
        assert w.filename == __file__
        assert issubclass(w.category, DeprecationWarning)
        assert RECIPE in str(w.message)

    def test_skips_usage_wrapper_frame(self):
        def public_method():
            util._warn_knative_mode_deprecated()

        # Defined in the usage module's namespace to stand for its logging wrapper.
        wrapper = eval(  # noqa: S307
            "lambda f: f()", {"__name__": "hopsworks_common.usage"}
        )

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            wrapper(public_method)

        (w,) = _knative_warnings(record)
        assert w.filename != "<string>"


class TestEntryPoints:
    def _model(self, mocker):
        m = mocker.Mock()
        m._get_default_serving_name.return_value = "my_model"
        return m

    @pytest.mark.parametrize("value", [False, None])
    def test_no_warning_without_knative(self, mocker, ms, value):
        mocker.patch("hsml.model_serving.Predictor.for_model")
        mocker.patch("hsml.model_serving.Predictor.for_feature_view")
        mocker.patch("hsml.model_serving.Predictor.for_server")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            ms.create_predictor(self._model(mocker), knative_mode=value)
            ms.create_feature_view_predictor(mocker.Mock(), knative_mode=value)
            ms.create_endpoint(name="e", script_file="s.py", knative_mode=value)

        assert _knative_warnings(record) == []

    def test_model_deploy_warns_once(self, mocker):
        mocker.patch("hsml.model.Predictor.for_model")
        m = model.Model(id="1", name="my_model", version=1)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            m.deploy(knative_mode=True)

        (w,) = _knative_warnings(record)
        assert w.filename == __file__

    def test_model_deploy_no_warning(self, mocker):
        mocker.patch("hsml.model.Predictor.for_model")
        m = model.Model(id="1", name="my_model", version=1)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            m.deploy(knative_mode=False)
            m.deploy()

        assert _knative_warnings(record) == []

    def test_create_predictor_warns_once(self, mocker, ms):
        mocker.patch("hsml.model_serving.Predictor.for_model")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            ms.create_predictor(self._model(mocker), knative_mode=True)

        (w,) = _knative_warnings(record)
        assert w.filename == __file__

    def test_create_feature_view_predictor_warns_once(self, mocker, ms):
        mocker.patch("hsml.model_serving.Predictor.for_feature_view")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            ms.create_feature_view_predictor(mocker.Mock(), knative_mode=True)

        (w,) = _knative_warnings(record)
        assert w.filename == __file__

    def test_create_endpoint_warns_once(self, mocker, ms):
        mocker.patch("hsml.model_serving.Predictor.for_server")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            ms.create_endpoint(name="e", script_file="s.py", knative_mode=True)

        (w,) = _knative_warnings(record)
        assert w.filename == __file__

    def test_deploy_agent_warns_once(self, mocker, ms, tmp_path):
        script = tmp_path / "agent.py"
        script.write_text("print('hi')")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving._dataset_api.DatasetApi")
        mocker.patch("hsml.model_serving._environment_api.EnvironmentApi")
        mocker.patch("hsml.model_serving.Predictor.for_server")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            ms.deploy_agent(entry=str(script), name="agent", knative_mode=True)

        (w,) = _knative_warnings(record)
        assert w.filename == __file__


class TestSetters:
    def test_predictor_setter_true_warns(self, make_predictor):
        p = make_predictor()

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            p.knative_mode = True

        (w,) = _knative_warnings(record)
        assert w.filename == __file__
        assert p.knative_mode is True

    @pytest.mark.parametrize("value", [False, None])
    def test_predictor_setter_other_values_silent(self, make_predictor, value):
        p = make_predictor(knative_mode=True)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            p.knative_mode = value

        assert _knative_warnings(record) == []
        assert p.knative_mode is value

    def test_deployment_setter_true_warns_once(self, make_predictor):
        d = deployment.Deployment(predictor=make_predictor())

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            d.knative_mode = True

        (w,) = _knative_warnings(record)
        assert w.filename == __file__
        assert d.knative_mode is True

    @pytest.mark.parametrize("value", [False, None])
    def test_deployment_setter_other_values_silent(self, make_predictor, value):
        d = deployment.Deployment(predictor=make_predictor(knative_mode=True))

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            d.knative_mode = value

        assert _knative_warnings(record) == []
        assert d.knative_mode is value

    def test_hydration_is_silent(self, make_predictor):
        p = make_predictor(knative_mode=True)
        json_dict = p.to_dict()

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            p2 = predictor.Predictor.from_response_json(json_dict)
            p.update_from_response_json(json_dict)
            d = deployment.Deployment.from_response_json(json_dict)

        assert _knative_warnings(record) == []
        assert p2.knative_mode is True
        assert p.knative_mode is True
        assert d.knative_mode is True


class TestLoadTimeNotice:
    @pytest.fixture
    def knative_deployment(self, make_predictor):
        return deployment.Deployment(
            predictor=make_predictor(knative_mode=True), name="kn"
        )

    @pytest.fixture
    def standard_deployment(self, make_predictor):
        return deployment.Deployment(
            predictor=make_predictor(knative_mode=False), name="std"
        )

    def test_get_deployment_notices_knative_only(
        self, mocker, ms, knative_deployment, standard_deployment
    ):
        mocker.patch("hsml.model_serving.util._check_missing_mandatory_tags")
        api = mocker.patch.object(ms, "_serving_api")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            api._get.return_value = standard_deployment
            ms.get_deployment("std")
            assert _knative_warnings(record) == []
            api._get.return_value = knative_deployment
            ms.get_deployment("kn")

        (w,) = _knative_warnings(record)
        assert issubclass(w.category, util.KnativeModeWarning)
        assert "'kn'" in str(w.message)
        assert RECIPE in str(w.message)

    def test_get_deployment_by_id_notices_knative_only(
        self, mocker, ms, knative_deployment, standard_deployment
    ):
        api = mocker.patch.object(ms, "_serving_api")

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            api._get_by_id.return_value = standard_deployment
            ms.get_deployment_by_id(1)
            assert _knative_warnings(record) == []
            api._get_by_id.return_value = knative_deployment
            ms.get_deployment_by_id(2)

        (w,) = _knative_warnings(record)
        assert "'kn'" in str(w.message)

    def test_get_deployments_one_notice_per_knative_deployment(
        self, mocker, ms, make_predictor, standard_deployment
    ):
        deployments = [
            deployment.Deployment(predictor=make_predictor(True), name="kn1"),
            standard_deployment,
            deployment.Deployment(predictor=make_predictor(True), name="kn2"),
        ]
        mocker.patch.object(ms, "_serving_api")._get_all.return_value = deployments

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            result = ms.get_deployments()

        assert result == deployments
        notices = _knative_warnings(record)
        assert len(notices) == 2
        assert all(issubclass(n.category, util.KnativeModeWarning) for n in notices)
        assert [
            "'kn1'" in str(notices[0].message),
            "'kn2'" in str(notices[1].message),
        ] == [
            True,
            True,
        ]

    def test_saving_does_not_repeat_the_notice(self, make_predictor):
        p = make_predictor(knative_mode=True)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            p.update_from_response_json(p.to_dict())

        assert _knative_warnings(record) == []

    def test_visible_under_default_filters(self):
        # A fresh interpreter has the stock warning filters, so only what the
        # SDK itself registers makes the notice visible.
        code = textwrap.dedent(
            """
            import hsml
            from hopsworks_common import util
            import warnings

            def sdk_internal():
                util._warn_knative_deployment_loaded("kn")

            sdk_internal()
            """
        )
        result = subprocess.run(
            [sys.executable, "-W", "default", "-c", code],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "KnativeModeWarning" in result.stderr
        assert "Deployment 'kn'" in result.stderr

    def test_repeated_notice_not_deduplicated(self):
        code = textwrap.dedent(
            """
            import hsml
            from hopsworks_common import util

            for _ in range(2):
                util._warn_knative_deployment_loaded("kn")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )
        assert result.stderr.count("KnativeModeWarning") == 2
