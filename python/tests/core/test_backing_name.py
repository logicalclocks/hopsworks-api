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

from hopsworks_common.core import alerts_api


class TestAlertReceiverPrefix:
    def test_fix_receiver_name_prefixes_with_the_backing_name(self):
        api = alerts_api.AlertsApi()

        assert api._fix_receiver_name("mail", "backing1") == "backing1__mail"

    def test_fix_receiver_name_keeps_prefixed_and_global_names(self):
        api = alerts_api.AlertsApi()

        assert api._fix_receiver_name("backing1__mail", "backing1") == "backing1__mail"
        global_name = f"{alerts_api._GLOBAL_RECEIVER_PREFIX}__mail"
        assert api._fix_receiver_name(global_name, "backing1") == global_name

    def test_display_name_prefix_is_not_taken_for_the_backing_prefix(self):
        api = alerts_api.AlertsApi()

        assert (
            api._fix_receiver_name("Display Name__mail", "backing1")
            == "backing1__Display Name__mail"
        )
