#
#   Copyright 2022 Logical Clocks AB
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
from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper
import shutil
import logging

from hopsworks import client
from hopsworks.client.exceptions import RestAPIError
from hopsworks.client.exceptions import DatasetException


class DatasetApi:
    def __init__(
        self,
        project_id,
    ):
        self._project_id = project_id
        self._log = logging.getLogger(__name__)

    DEFAULT_FLOW_CHUNK_SIZE = 1048576

    def download(self, path: str, local_path: str = None, overwrite: bool = False):
        """Download file from Hopsworks Filesystem to the current working directory.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        downloaded_file_path = dataset_api.download("Resources/my_local_file.txt")

        ```
        # Arguments
            path: path in Hopsworks filesystem to the file
            local_path: path where to download the file in the local filesystem
            overwrite: overwrite local file if exists
        # Returns
            `str`: Path to downloaded file
        # Raises
            `RestAPIError`: If unable to download the file
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "dataset",
            "download",
            "with_auth",
            path,
        ]
        query_params = {"type": "DATASET"}

        # Build the path to download the file on the local fs and return to the user, it should be absolute for consistency
        # Download in CWD if local_path not specified
        if local_path is None:
            local_path = os.path.join(os.getcwd(), os.path.basename(path))
        # If local_path specified, ensure it is absolute
        else:
            if os.path.isabs(local_path):
                local_path = os.path.join(local_path, os.path.basename(path))
            else:
                local_path = os.path.join(
                    os.getcwd(), local_path, os.path.basename(path)
                )

        if os.path.exists(local_path):
            if overwrite:
                if os.path.isfile:
                    os.remove(local_path)
                else:
                    shutil.rmtree(local_path)
            else:
                raise IOError(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        file_size = int(self._get(path)["attributes"]["size"])
        with _client._send_request(
            "GET", path_params, query_params=query_params, stream=True
        ) as response:
            with open(local_path, "wb") as f:
                pbar = None
                try:
                    pbar = tqdm(
                        total=file_size,
                        bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                        desc="Downloading",
                    )
                except Exception:
                    self._log.exception("Failed to initialize progress bar.")
                    self._log.info("Starting download")

                for chunk in response.iter_content(
                    chunk_size=self.DEFAULT_FLOW_CHUNK_SIZE
                ):
                    f.write(chunk)

                    if pbar is not None:
                        pbar.update(len(chunk))

                if pbar is not None:
                    pbar.close()
                else:
                    self._log.info("Download finished")

        return local_path

    def upload(self, local_path: str, upload_path: str, overwrite: bool = False):
        """Upload a file to the Hopsworks filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        uploaded_file_path = dataset_api.upload("my_local_file.txt", "Resources")

        ```
        # Arguments
            local_path: local path to file to upload
            upload_path: path to directory where to upload the file in Hopsworks Filesystem
            overwrite: overwrite file if exists
        # Returns
            `str`: Path to uploaded file
        # Raises
            `RestAPIError`: If unable to upload the file
        """
        # local path could be absolute or relative,
        if not os.path.isabs(local_path) and os.path.exists(
            os.path.join(os.getcwd(), local_path)
        ):
            local_path = os.path.join(os.getcwd(), local_path)

        file_size = os.path.getsize(local_path)

        _, file_name = os.path.split(local_path)

        destination_path = upload_path + "/" + file_name

        if self.exists(destination_path):
            if overwrite:
                self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        with open(local_path, "rb") as f:
            pbar = None
            try:
                pbar = tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                    desc="Uploading",
                )
            except Exception:
                self._log.exception("Failed to initialize progress bar.")
                self._log.info("Starting upload")

            if pbar is not None:
                f = CallbackIOWrapper(pbar.update, f, "read")

            self._upload_request(upload_path, file_name, f, file_size)

            if pbar is not None:
                pbar.close()
            else:
                self._log.info("Upload finished")

        return upload_path + "/" + os.path.basename(local_path)

    def _upload_request(self, path, file_name, file, file_size):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", "v2", "upload", path]

        _client._send_request(
            "POST", path_params, files={"file": (file_name, file), "fileName": file_name, "fileSize": file_size},
            stream=True
        )

    def _get(self, path: str):
        """Get dataset.

        :param path: path to check
        :type path: str
        :return: dataset metadata
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def exists(self, path: str):
        """Check if a file exists in the Hopsworks Filesystem.

        # Arguments
            path: path to check
        # Returns
            `bool`: True if exists, otherwise False
        # Raises
            `RestAPIError`: If unable to check existence for the path
        """
        try:
            self._get(path)
            return True
        except RestAPIError:
            return False

    def remove(self, path: str):
        """Remove a path in the Hopsworks Filesystem.

        # Arguments
            path: path to remove
        # Raises
            `RestAPIError`: If unable to remove the path
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "dataset", path]
        _client._send_request("DELETE", path_params)
