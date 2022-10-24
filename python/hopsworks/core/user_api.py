/hopsworks-api/api/users/profile
#
#   Copyright 2022 Hopsworks AB
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

from hopsworks import client, user, constants


class UserApi:
    def get_current_user(self):
        """Get current user's profile.

        # Returns
            `User`: The User object
        # Raises
            `RestAPIError`: If unable to get the user
        """
        _client = client.get_instance()
        path_params = [
            "users",
            "profile",
        ]
        project_json = _client._send_request("GET", path_params)
        return user.User.from_response_json(project_json)
