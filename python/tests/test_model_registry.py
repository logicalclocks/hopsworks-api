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

from hsml import model_registry


class TestModelRegistryPaths:
    def test_project_path_uses_the_backing_name(self):
        mr = model_registry.ModelRegistry(
            project_name="Display",
            project_id=1,
            model_registry_id=1,
            project_backing_name="backing1",
        )

        assert mr.project_path == "/Projects/backing1"

    def test_project_path_without_backing_name_uses_the_project_name(self):
        mr = model_registry.ModelRegistry(
            project_name="proj", project_id=1, model_registry_id=1
        )

        assert mr.project_path == "/Projects/proj"

    def test_models_path_is_kept_for_a_shared_registry(self):
        mr = model_registry.ModelRegistry(
            project_name="proj",
            project_id=1,
            model_registry_id=2,
            shared_registry_project_name="other",
            models_path="/Projects/other_ab12/Models",
        )

        assert mr._models_path == "/Projects/other_ab12/Models"
