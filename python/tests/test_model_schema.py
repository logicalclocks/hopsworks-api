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


from hsml import model_schema


class TestModelSchema:
    # constructor

    def test_constructor(self):
        # Act
        msch = model_schema.ModelSchema(input_schema="1234", output_schema="4321")

        # Assert
        assert msch.input_schema == "1234"
        assert msch.output_schema == "4321"
