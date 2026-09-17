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

import copy

from hopsworks_common.constants import SCALING_CONFIG, Default
from hsml import resources, transformer
from hsml.constants import RESOURCES


SERVING_NUM_INSTANCES_NO_LIMIT = [-1]
SERVING_NUM_INSTANCES_SCALE_TO_ZERO = [0]
SERVING_NUM_INSTANCES_ONE = [0]


class TestTransformer:
    # from response json

    def test_from_response_json_with_transformer_field(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_deployment_with_transformer"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert isinstance(t, transformer.Transformer)
        assert t.script_file == json["transformer"]

        tr_resources = json["transformer_resources"]
        assert (
            t.resources.num_instances == tr_resources["requested_transformer_instances"]
        )
        assert t.resources.requests.cores == tr_resources["requests"]["cores"]
        assert t.resources.requests.memory == tr_resources["requests"]["memory"]
        assert t.resources.requests.gpus == tr_resources["requests"]["gpus"]
        assert t.resources.limits.cores == tr_resources["limits"]["cores"]
        assert t.resources.limits.memory == tr_resources["limits"]["memory"]
        assert t.resources.limits.gpus == tr_resources["limits"]["gpus"]

        assert t.scaling_configuration is not None
        assert isinstance(t.scaling_configuration, transformer.TransformerScalingConfig)
        assert (
            t.scaling_configuration.min_instances
            == json["transformer_scaling_config"]["min_instances"]
        )
        assert (
            t.scaling_configuration.scale_metric.value
            == json["transformer_scaling_config"]["scale_metric"]
        )
        assert (
            t.scaling_configuration.target
            == json["transformer_scaling_config"]["target"]
        )

    def test_from_response_json_with_script_file_field(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_transformer_with_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert isinstance(t, transformer.Transformer)
        assert t.script_file == json["script_file"]

        tr_resources = json["resources"]
        assert t.resources.num_instances == tr_resources["num_instances"]
        assert t.resources.requests.cores == tr_resources["requests"]["cores"]
        assert t.resources.requests.memory == tr_resources["requests"]["memory"]
        assert t.resources.requests.gpus == tr_resources["requests"]["gpus"]
        assert t.resources.limits.cores == tr_resources["limits"]["cores"]
        assert t.resources.limits.memory == tr_resources["limits"]["memory"]
        assert t.resources.limits.gpus == tr_resources["limits"]["gpus"]

        assert t.scaling_configuration is not None
        assert isinstance(t.scaling_configuration, transformer.TransformerScalingConfig)
        assert (
            t.scaling_configuration.min_instances
            == json["transformer_scaling_config"]["min_instances"]
        )
        assert (
            t.scaling_configuration.scale_metric.value
            == json["transformer_scaling_config"]["scale_metric"]
        )
        assert (
            t.scaling_configuration.target
            == json["transformer_scaling_config"]["target"]
        )

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_deployment_without_transformer"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert t is None

    def test_from_response_json_default_resources(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert isinstance(t, transformer.Transformer)
        assert t.script_file == json["script_file"]

        assert t.resources.num_instances == SCALING_CONFIG.MIN_NUM_INSTANCES
        assert t.resources.requests.cores == RESOURCES.MIN_CORES
        assert t.resources.requests.memory == RESOURCES.MIN_MEMORY
        assert t.resources.requests.gpus == RESOURCES.GPUS
        assert t.resources.limits.cores == RESOURCES.MAX_CORES
        assert t.resources.limits.memory == RESOURCES.MAX_MEMORY
        assert t.resources.limits.gpus == RESOURCES.GPUS

    # constructor

    def test_constructor_default_resources(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer(json["script_file"], resources=None)

        # Assert
        assert t.script_file == json["script_file"]

        assert t.resources.num_instances == SCALING_CONFIG.MIN_NUM_INSTANCES
        assert t.resources.requests.cores == RESOURCES.MIN_CORES
        assert t.resources.requests.memory == RESOURCES.MIN_MEMORY
        assert t.resources.requests.gpus == RESOURCES.GPUS
        assert t.resources.limits.cores == RESOURCES.MAX_CORES
        assert t.resources.limits.memory == RESOURCES.MAX_MEMORY
        assert t.resources.limits.gpus == RESOURCES.GPUS

    def test_constructor_default_resources_when_scale_to_zero_is_required(
        self, mocker, backend_fixtures
    ):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer(json["script_file"], resources=None)

        # Assert
        assert t.script_file == json["script_file"]

        assert t.resources.num_instances == 0
        assert t.resources.requests.cores == RESOURCES.MIN_CORES
        assert t.resources.requests.memory == RESOURCES.MIN_MEMORY
        assert t.resources.requests.gpus == RESOURCES.GPUS
        assert t.resources.limits.cores == RESOURCES.MAX_CORES
        assert t.resources.limits.memory == RESOURCES.MAX_MEMORY
        assert t.resources.limits.gpus == RESOURCES.GPUS

    def test_constructor_default_scaling_configuration_marker(
        self, mocker, backend_fixtures
    ):
        # A non-provided scaling configuration stays None so an in-place edit cannot be silently dropped; the backend synthesizes the mode-appropriate default.
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer(
            json["script_file"], resources=None, scaling_configuration=Default()
        )

        # Assert
        assert t.scaling_configuration is None
        assert "scaleMetric" not in t.to_dict()
        assert "minInstances" not in t.to_dict()

    def test_resolve_default_num_instances_standard_mode_needs_one(self, mocker):
        # A defaulted instance count assumes Knative mode (scale-to-zero) until the owning predictor resolves the mode; Standard mode lifts it to one instance.
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        t = transformer.Transformer(script_file="t.py")
        assert t.resources.num_instances == 0

        # Act
        t._resolve_default_num_instances(effective_knative_mode=False)

        # Assert
        assert t.resources.num_instances == 1

        # Act: resolving back to Knative mode restores the scale-to-zero default
        t._resolve_default_num_instances(effective_knative_mode=True)

        # Assert
        assert t.resources.num_instances == 0

    def test_resolve_default_num_instances_keeps_explicit_value(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        t = transformer.Transformer(script_file="t.py", resources={"num_instances": 0})

        # Act
        t._resolve_default_num_instances(effective_knative_mode=False)

        # Assert: an explicit zero is left for the backend to reject loudly
        assert t.resources.num_instances == 0

    # validate resources

    def test_validate_resources_none(self):
        # Act
        res = transformer.Transformer._validate_resources(None)

        # Assert
        assert res is None

    def test_validate_resources_num_instances_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        tr = resources.TransformerResources(num_instances=0)

        # Act
        res = transformer.Transformer._validate_resources(tr)

        # Assert
        assert res == tr

    def test_validate_resources_num_instances_one_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        tr = resources.TransformerResources(num_instances=1)

        # Act
        res = transformer.Transformer._validate_resources(tr)

        # Assert
        assert res == tr

    def test_validate_resources_num_instances_one_with_scale_to_zero(self, mocker):
        # The cluster's scale-to-zero requirement only applies to Knative mode deployments and the transformer cannot know the mode at construction.
        # One instance must be accepted client-side (standard mode requires it) and the backend validates mode-aware.
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        tr = resources.TransformerResources(num_instances=1)

        # Act
        res = transformer.Transformer._validate_resources(tr)

        # Assert
        assert res == tr

    def test_init_num_instances_one_with_scale_to_zero_constructs(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        t = transformer.Transformer(script_file="t.py", resources={"num_instances": 1})

        # Assert
        assert t.resources.num_instances == 1

    # default num instances

    def test_get_default_num_instances_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        num_instances = transformer.Transformer._get_default_num_instances()

        # Assert
        assert num_instances == SCALING_CONFIG.MIN_NUM_INSTANCES

    def test_get_default_num_instances_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        num_instances = transformer.Transformer._get_default_num_instances()

        # Assert
        assert num_instances == 0

    # default resources

    def test_get_default_resources_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        res = transformer.Transformer._get_default_resources()

        # Assert
        assert isinstance(res, resources.TransformerResources)
        assert res.num_instances == SCALING_CONFIG.MIN_NUM_INSTANCES

    def test_get_default_resources_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        res = transformer.Transformer._get_default_resources()

        # Assert
        assert isinstance(res, resources.TransformerResources)
        assert res.num_instances == 0

    # extract fields from json
    def test_extract_fields_from_json(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_deployment_with_transformer"][
            "response"
        ]
        json_copy = copy.deepcopy(json)

        # Act
        sf, rc, sc, ev = transformer.Transformer.extract_fields_from_json(json_copy)

        # env_vars not present in this fixture
        assert ev is None

        # Assert
        assert sf == json["transformer"]
        assert isinstance(rc, resources.TransformerResources)

        tr_resources = json["transformer_resources"]
        assert rc.num_instances == tr_resources["requested_transformer_instances"]
        assert rc.requests.cores == tr_resources["requests"]["cores"]
        assert rc.requests.memory == tr_resources["requests"]["memory"]
        assert rc.requests.gpus == tr_resources["requests"]["gpus"]
        assert rc.limits.cores == tr_resources["limits"]["cores"]
        assert rc.limits.memory == tr_resources["limits"]["memory"]
        assert rc.limits.gpus == tr_resources["limits"]["gpus"]

        assert isinstance(sc, transformer.TransformerScalingConfig)
        assert sc.min_instances == json["transformer_scaling_config"]["min_instances"]
        assert (
            sc.scale_metric.value == json["transformer_scaling_config"]["scale_metric"]
        )
        assert sc.target == json["transformer_scaling_config"]["target"]

    def test_constructor_scaling_configuration_none(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        script_file = "transformer_file_name"

        # Act
        t = transformer.Transformer(
            script_file=script_file,
            resources=None,
            scaling_configuration=None,
        )

        # Assert
        assert t.script_file == script_file

        # A non-provided scaling config stays None: a local default object would invite in-place edits that are silently never serialized, and the backend synthesizes the mode-appropriate default.
        assert t.scaling_configuration is None

    # env vars

    def test_constructor_env_vars(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        t = transformer.Transformer(
            script_file="t.py", resources=None, env_vars={"A": "1"}
        )

        # Assert
        assert t.env_vars == {"A": "1"}

    def test_env_vars_setter(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(script_file="t.py", resources=None)
        assert t.env_vars is None

        # Act
        t.env_vars = {"A": "1"}

        # Assert
        assert t.env_vars == {"A": "1"}

    def test_to_dict_env_vars_serialises_to_transformer_env_vars_list(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(
            script_file="t.py",
            resources=None,
            env_vars={"FOO": "bar", "BAZ": "qux"},
        )

        # Act
        d = t.to_dict()

        # Assert
        assert "transformerEnvVars" in d
        assert sorted(d["transformerEnvVars"]) == ["BAZ=qux", "FOO=bar"]

    def test_to_dict_env_vars_none_omits_key(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(script_file="t.py", resources=None)

        # Act
        d = t.to_dict()

        # Assert
        assert "transformerEnvVars" not in d

    def test_to_dict_defaulted_scaling_config_omitted(self, mocker):
        # Only explicitly provided configs are stored and serialized; the backend synthesizes the mode-appropriate default when the key is absent.
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(script_file="t.py", resources=None)

        # Act
        d = t.to_dict()

        # Assert
        assert t.scaling_configuration is None
        assert "transformerScalingConfig" not in d

    def test_to_dict_provided_scaling_config_serialized(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(
            script_file="t.py",
            resources=None,
            scaling_configuration={"min_instances": 2},
        )

        # Act
        d = t.to_dict()

        # Assert
        assert "transformerScalingConfig" in d
        assert d["transformerScalingConfig"]["minInstances"] == 2

    def test_scaling_config_setter_accepts_dict(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(script_file="t.py", resources=None)
        t.scaling_configuration = {"min_instances": 4}

        # Act
        d = t.to_dict()

        # Assert
        assert isinstance(t.scaling_configuration, transformer.TransformerScalingConfig)
        assert d["transformerScalingConfig"]["minInstances"] == 4

    def test_to_dict_scaling_config_set_after_construction_serialized(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(script_file="t.py", resources=None)
        t.scaling_configuration = transformer.TransformerScalingConfig(min_instances=3)

        # Act
        d = t.to_dict()

        # Assert
        assert "transformerScalingConfig" in d
        assert d["transformerScalingConfig"]["minInstances"] == 3

    def test_extract_fields_from_json_env_vars(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = copy.deepcopy(
            backend_fixtures["transformer"]["get_deployment_with_transformer"][
                "response"
            ]
        )
        json["transformer_env_vars"] = ["FOO=bar"]

        # Act
        sf, rc, sc, ev = transformer.Transformer.extract_fields_from_json(json)

        # Assert
        assert ev == {"FOO": "bar"}
        # Key consumed (popped) on the way out
        assert "transformer_env_vars" not in json

    def test_from_response_json_with_env_vars(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = copy.deepcopy(
            backend_fixtures["transformer"]["get_deployment_with_transformer"][
                "response"
            ]
        )
        json["transformer_env_vars"] = ["FOO=bar"]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert t.env_vars == {"FOO": "bar"}

    def test_env_vars_lifecycle_add_change_remove(self, mocker):
        # Mirrors the loadtest scenario for the transformer side: set on construct,
        # override, clear with None, re-set, clear with {}. Each transition both
        # holds in memory and round-trips through to_dict.
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        t = transformer.Transformer(
            script_file="t.py",
            resources=None,
            env_vars={"TR_FOO": "bar", "TR_BAZ": "qux"},
        )
        assert t.env_vars == {"TR_FOO": "bar", "TR_BAZ": "qux"}
        assert sorted(t.to_dict()["transformerEnvVars"]) == ["TR_BAZ=qux", "TR_FOO=bar"]

        # Override
        t.env_vars = {"TR_NEW": "1"}
        assert t.env_vars == {"TR_NEW": "1"}
        assert t.to_dict()["transformerEnvVars"] == ["TR_NEW=1"]

        # Clear with None — to_dict omits the key so the backend stores null.
        t.env_vars = None
        assert t.env_vars is None
        assert "transformerEnvVars" not in t.to_dict()

        # Re-set then clear with {} — same wire output as None.
        t.env_vars = {"TR_AGAIN": "2"}
        assert t.to_dict()["transformerEnvVars"] == ["TR_AGAIN=2"]
        t.env_vars = {}
        assert t.env_vars == {}
        assert "transformerEnvVars" not in t.to_dict()

    def test_env_vars_wire_round_trip(self, mocker, backend_fixtures):
        # SDK → to_dict → decamelize → extract_fields_from_json must preserve
        # env_vars. Guards against any drift between serialiser and parser.
        import humps

        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        t = transformer.Transformer(
            script_file="t.py",
            resources=None,
            env_vars={"TR_FOO": "bar", "TR_K": "V=with=eq"},
        )
        wire = copy.deepcopy(
            backend_fixtures["transformer"]["get_deployment_with_transformer"][
                "response"
            ]
        )
        wire["transformer_env_vars"] = humps.decamelize(t.to_dict())[
            "transformer_env_vars"
        ]

        _, _, _, ev = transformer.Transformer.extract_fields_from_json(wire)

        assert ev == {"TR_FOO": "bar", "TR_K": "V=with=eq"}

    # auxiliary methods
    def _mock_serving_variables(self, mocker, num_instances, force_scale_to_zero=False):
        mocker.patch(
            "hopsworks_common.client._get_serving_num_instances_limits",
            return_value=num_instances,
        )
        mocker.patch(
            "hopsworks_common.client._is_scale_to_zero_required",
            return_value=force_scale_to_zero,
        )
