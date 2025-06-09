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

from __future__ import annotations

import copy
import json
import logging
import math
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Literal, Optional, Union

from hopsworks_common import client, tag, usage, util
from hopsworks_common.client.exceptions import DatasetException, RestAPIError
from hopsworks_common.core import inode
from tqdm.auto import tqdm


class Chunk:
    def __init__(self, content, number, status):
        self.content = content
        self.number = number
        self.status = status
        self.retries = 0


class DatasetApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    DEFAULT_UPLOAD_FLOW_CHUNK_SIZE = 10 * 1024 * 1024
    DEFAULT_UPLOAD_SIMULTANEOUS_UPLOADS = 3
    DEFAULT_UPLOAD_SIMULTANEOUS_CHUNKS = 3
    DEFAULT_UPLOAD_MAX_CHUNK_RETRIES = 1

    DEFAULT_DOWNLOAD_FLOW_CHUNK_SIZE = 1024 * 1024
    FLOW_PERMANENT_ERRORS = [404, 413, 415, 500, 501]

    # alias for backwards-compatibility:
    DEFAULT_FLOW_CHUNK_SIZE = DEFAULT_DOWNLOAD_FLOW_CHUNK_SIZE

    @usage.method_logger
    def download(
        self,
        path: str,
        local_path: Optional[str] = None,
        overwrite: Optional[bool] = False,
        chunk_size: int = DEFAULT_DOWNLOAD_FLOW_CHUNK_SIZE,
    ):
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
            chunk_size: upload chunk size in bytes. Default 1 MB
        # Returns
            `str`: Path to downloaded file
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "download",
            "with_auth",
            path,
        ]
        query_params = {"type": "DATASET"}

        # Build the path to download the file on the local fs and return to the user, it should be absolute for consistency
        # Download in CWD if local_path not specified
        if local_path is None:
            local_path = os.getcwd()
        # If local_path specified, ensure it is absolute
        elif not os.path.isabs(local_path):
            local_path = os.path.join(os.getcwd(), local_path)

        # If local_path is a directory, download into the directory
        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, os.path.basename(path))

        if overwrite:
            if os.path.isfile(local_path):
                os.remove(local_path)
            elif os.path.isdir(local_path):
                shutil.rmtree(local_path)
        elif os.path.exists(local_path):
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

                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)

                    if pbar is not None:
                        pbar.update(len(chunk))

                if pbar is not None:
                    pbar.close()
                else:
                    self._log.info("Download finished")

        return local_path

    @usage.method_logger
    def upload(
        self,
        local_path: str,
        upload_path: str,
        overwrite: bool = False,
        chunk_size: int = DEFAULT_UPLOAD_FLOW_CHUNK_SIZE,
        simultaneous_uploads: int = DEFAULT_UPLOAD_SIMULTANEOUS_UPLOADS,
        simultaneous_chunks: int = DEFAULT_UPLOAD_SIMULTANEOUS_CHUNKS,
        max_chunk_retries: int = DEFAULT_UPLOAD_MAX_CHUNK_RETRIES,
        chunk_retry_interval: int = 1,
    ):
        """Upload a file or directory to the Hopsworks filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        # upload a file to Resources dataset
        uploaded_file_path = dataset_api.upload("my_local_file.txt", "Resources")

        # upload a directory to Resources dataset
        uploaded_file_path = dataset_api.upload("my_dir", "Resources")

        ```
        # Arguments
            local_path: local path to file or directory to upload, can be relative or absolute
            upload_path: path to directory where to upload the file in Hopsworks Filesystem
            overwrite: overwrite file or directory if exists
            chunk_size: upload chunk size in bytes. Default 10 MB
            simultaneous_chunks: number of simultaneous chunks to upload for each file upload. Default 3
            simultaneous_uploads: number of simultaneous files to be uploaded for directories. Default 3
            max_chunk_retries: maximum retry for a chunk. Default is 1
            chunk_retry_interval: chunk retry interval in seconds. Default is 1sec
        # Returns
            `str`: Path to uploaded file or directory
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        # local path could be absolute or relative,
        if not os.path.isabs(local_path) and os.path.exists(
            os.path.join(os.getcwd(), local_path)
        ):
            local_path = os.path.join(os.getcwd(), local_path)

        _, file_name = os.path.split(local_path)

        destination_path = upload_path + "/" + file_name

        if self.exists(destination_path):
            if overwrite:
                if "datasetType" in self._get(destination_path):
                    raise DatasetException(
                        "overwrite=True not supported on a top-level dataset"
                    )
                else:
                    self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        destination_path
                    )
                )

        if os.path.isdir(local_path):
            self.mkdir(destination_path)

        if os.path.isdir(local_path):
            with ThreadPoolExecutor(simultaneous_uploads) as executor:
                # if path is a dir, upload files and folders iteratively
                for root, dirs, files in os.walk(local_path):
                    # os.walk(local_model_path), where local_model_path is expected to be an absolute path
                    # - root is the absolute path of the directory being walked
                    # - dirs is the list of directory names present in the root dir
                    # - files is the list of file names present in the root dir
                    # we need to replace the local path prefix with the hdfs path prefix (i.e., /srv/hops/....../root with /Projects/.../)
                    remote_base_path = root.replace(
                        local_path, destination_path
                    ).replace(os.sep, "/")
                    for d_name in dirs:
                        self.mkdir(remote_base_path + "/" + d_name)

                    # uploading files in the same folder is done concurrently
                    futures = [
                        executor.submit(
                            self._upload_file,
                            f_name,
                            root + os.sep + f_name,
                            remote_base_path,
                            chunk_size,
                            simultaneous_chunks,
                            max_chunk_retries,
                            chunk_retry_interval,
                        )
                        for f_name in files
                    ]

                    # wait for all upload tasks to complete
                    _, _ = wait(futures)
                    try:
                        _ = [future.result() for future in futures]
                    except Exception as e:
                        raise e
        else:
            self._upload_file(
                file_name,
                local_path,
                upload_path,
                chunk_size,
                simultaneous_chunks,
                max_chunk_retries,
                chunk_retry_interval,
            )

        return upload_path + "/" + os.path.basename(local_path)

    def _upload_file(
        self,
        file_name,
        local_path,
        upload_path,
        chunk_size,
        simultaneous_chunks,
        max_chunk_retries,
        chunk_retry_interval,
    ):
        file_size = os.path.getsize(local_path)

        num_chunks = math.ceil(file_size / chunk_size)

        base_params = self._get_flow_base_params(
            file_name, num_chunks, file_size, chunk_size
        )

        chunk_number = 1
        with open(local_path, "rb") as f:
            pbar = None
            try:
                pbar = tqdm(
                    total=file_size,
                    bar_format="{desc}: {percentage:.3f}%|{bar}| {n_fmt}/{total_fmt} elapsed<{elapsed} remaining<{remaining}",
                    desc="Uploading {}".format(local_path),
                )
            except Exception:
                self._log.exception("Failed to initialize progress bar.")
                self._log.info("Starting upload")
            with ThreadPoolExecutor(simultaneous_chunks) as executor:
                all_chunks_processed = False

                while not all_chunks_processed:
                    chunks = []

                    if os.path.getsize(local_path) == 0:
                        chunks.append(Chunk(b'', 1, "pending"))
                        all_chunks_processed = True
                    else:
                        for _ in range(simultaneous_chunks):
                            chunk_data = f.read(chunk_size)
                            if not chunk_data:
                                all_chunks_processed = True
                                break
                            chunks.append(Chunk(chunk_data, chunk_number, "pending"))
                            chunk_number += 1

                    if not chunks:
                        break

                    futures = [
                        executor.submit(
                            self._upload_chunk,
                            base_params,
                            upload_path,
                            file_name,
                            chunk,
                            pbar,
                            max_chunk_retries,
                            chunk_retry_interval,
                        )
                        for chunk in chunks
                    ]

                    try:
                        for future in futures:
                            future.result()
                    except Exception:
                        if pbar:
                            pbar.close()
                        raise

            if pbar is not None:
                pbar.close()
            else:
                self._log.info("Upload finished")

    def _upload_chunk(
        self,
        base_params,
        upload_path,
        file_name,
        chunk: Chunk,
        pbar,
        max_chunk_retries,
        chunk_retry_interval,
    ):
        query_params = copy.copy(base_params)
        query_params["flowCurrentChunkSize"] = len(chunk.content)
        query_params["flowChunkNumber"] = chunk.number

        chunk.status = "uploading"
        while True:
            try:
                self._upload_request(
                    query_params, upload_path, file_name, chunk.content
                )
                break
            except RestAPIError as re:
                chunk.retries += 1
                if (
                    re.response.status_code in DatasetApi.FLOW_PERMANENT_ERRORS
                    or chunk.retries > max_chunk_retries
                ):
                    chunk.status = "failed"
                    raise re
                time.sleep(chunk_retry_interval)
                continue

        chunk.status = "uploaded"

        if pbar is not None:
            pbar.update(query_params["flowCurrentChunkSize"])

    def _get_flow_base_params(self, file_name, num_chunks, size, chunk_size):
        return {
            "templateId": -1,
            "flowChunkSize": chunk_size,
            "flowTotalSize": size,
            "flowIdentifier": str(size) + "_" + file_name,
            "flowFilename": file_name,
            "flowRelativePath": file_name,
            "flowTotalChunks": num_chunks,
        }

    def _upload_request(self, params, path, file_name, chunk):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", "upload", path]

        # Flow configuration params are sent as form data
        _client._send_request(
            "POST", path_params, data=params, files={"file": (file_name, chunk)}
        )

    def _get(self, path: str):
        """Get dataset metadata.

        # Arguments
            path: path to check
        # Returns
            `dict`: dataset metadata
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", path]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def get(self, path: str):
        """**Deprecated**

        Get dataset metadata.

        # Arguments
            path: path to check
        # Returns
            `dict`: dataset metadata
        """
        return self._get(path)

    def exists(self, path: str):
        """Check if a file exists in the Hopsworks Filesystem.

        # Arguments
            path: path to check
        # Returns
            `bool`: True if exists, otherwise False
        """
        try:
            self._get(path)
            return True
        except RestAPIError:
            return False

    def path_exists(self, remote_path: str):
        """**Deprecated**, use `exists` instead.

        Check if a path exists in datasets.

        # Arguments
            remote_path: path to check
        # Returns
            `bool`: True if exists, otherwise False
        """
        return self.exists(remote_path)

    @usage.method_logger
    def remove(self, path: str):
        """Remove a path in the Hopsworks Filesystem.

        # Arguments
            path: path to remove
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", path]
        return _client._send_request("DELETE", path_params)

    def rm(self, remote_path: str):
        """**Deprecated**, use `remove` instead.

        Remove a path in the Hopsworks Filesystem.

        # Arguments
            remote_path: path to remove
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self.remove(remote_path)

    @usage.method_logger
    def mkdir(self, path: str):
        """Create a directory in the Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        directory_path = dataset_api.mkdir("Resources/my_dir")

        ```
        # Arguments
            path: path to directory
        # Returns
            `str`: Path to created directory
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", path]
        query_params = {
            "action": "create",
            "searchable": "true",
            "generate_readme": "false",
            "type": "DATASET",
        }
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )["attributes"]["path"]

    @usage.method_logger
    def copy(self, source_path: str, destination_path: str, overwrite: bool = False):
        """Copy a file or directory in the Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        directory_path = dataset_api.copy("Resources/myfile.txt", "Logs/myfile.txt")

        ```
        # Arguments
            source_path: the source path to copy
            destination_path: the destination path
            overwrite: overwrite destination if exists
        # Raises
            `hopsworks.client.exceptions.DatasetException`: If the destination path already exists and overwrite is not set to True
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if self.exists(destination_path):
            if overwrite:
                if "datasetType" in self._get(destination_path):
                    raise DatasetException(
                        "overwrite=True not supported on a top-level dataset"
                    )
                else:
                    self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        destination_path
                    )
                )

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", source_path]
        query_params = {
            "action": "copy",
            "destination_path": destination_path,
        }
        _client._send_request("POST", path_params, query_params=query_params)

    @usage.method_logger
    def move(self, source_path: str, destination_path: str, overwrite: bool = False):
        """Move a file or directory in the Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        dataset_api = project.get_dataset_api()

        directory_path = dataset_api.move("Resources/myfile.txt", "Logs/myfile.txt")

        ```
        # Arguments
            source_path: the source path to move
            destination_path: the destination path
            overwrite: overwrite destination if exists
        # Raises
            `hopsworks.client.exceptions.DatasetException`: If the destination path already exists and overwrite is not set to True
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if self.exists(destination_path):
            if overwrite:
                if "datasetType" in self._get(destination_path):
                    raise DatasetException(
                        "overwrite=True not supported on a top-level dataset"
                    )
                else:
                    self.remove(destination_path)
            else:
                raise DatasetException(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        destination_path
                    )
                )

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", source_path]
        query_params = {
            "action": "move",
            "destination_path": destination_path,
        }
        _client._send_request("POST", path_params, query_params=query_params)

    @usage.method_logger
    def upload_feature_group(self, feature_group, path, dataframe):
        # Convert the dataframe into PARQUET for upload
        df_parquet = dataframe.to_parquet(index=False)
        parquet_length = len(df_parquet)
        num_chunks = math.ceil(parquet_length / self.DEFAULT_FLOW_CHUNK_SIZE)

        base_params = self._get_flow_base_params(
            feature_group, num_chunks, parquet_length, self.DEFAULT_FLOW_CHUNK_SIZE
        )

        chunk_number = 1
        for i in range(0, parquet_length, self.DEFAULT_FLOW_CHUNK_SIZE):
            query_params = base_params
            query_params["flowCurrentChunkSize"] = len(
                df_parquet[i : i + self.DEFAULT_FLOW_CHUNK_SIZE]
            )
            query_params["flowChunkNumber"] = chunk_number

            self._upload_request(
                query_params,
                path,
                util.feature_group_name(feature_group),
                df_parquet[i : i + self.DEFAULT_FLOW_CHUNK_SIZE],
            )

            chunk_number += 1

    @usage.method_logger
    def list_files(self, path: str, offset: int, limit: int):
        """**Deprecated**

        List contents of a directory in the Hopsworks Filesystem.

        # Arguments
            path: path to the directory to list the contents of.
            offset: the number of Inodes to skip.
            limit: max number of the returned Inodes.
        # Returns
            `tuple[int, list[hopsworks.core.inode.Inode]]`: count of Inodes in the directory and the list of them.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            path[(path.index("/", 10) + 1) :],
        ]
        query_params = {
            "action": "listing",
            "offset": offset,
            "limit": limit,
            "sort_by": "ID:asc",
        }

        inode_lst = _client._send_request("GET", path_params, query_params)

        return inode_lst["count"], inode.Inode.from_response_json(inode_lst)

    @usage.method_logger
    def list(
        self,
        remote_path: str,
        sort_by: str | None = None,
        offset: int = 0,
        limit: int = 1000,
    ):
        """**Deprecated**

        List contents of a directory in the Hopsworks Filesystem.

        # Arguments
            remote_path: path to the directory to list the contents of.
            sort_by: sort string, for example `"ID:asc"`.
            offset: the number of entities to skip.
            limit: max number of the returned entities.
        """
        # this method is probably to be merged with list_files
        # they seem to handle paths differently and return different results, which prevents the merge at the moment (2024-09-03), due to the requirement of backwards-compatibility
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        query_params = {
            "action": "listing",
            "sort_by": sort_by,
            "limit": limit,
            "offset": offset,
        }
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, headers=headers, query_params=query_params
        )

    @usage.method_logger
    def read_content(self, path: str, dataset_type: str = "DATASET"):
        _client = client.get_instance()

        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "download",
            "with_auth",
            path[1:],
        ]

        query_params = {
            "type": dataset_type,
        }

        return _client._send_request("GET", path_params, query_params, stream=True)

    def chmod(self, remote_path: str, permissions: str):
        """Change permissions of a file or a directory in the Hopsworks Filesystem.

        # Arguments
            remote_path: path to change the permissions of.
            permissions: permissions string, for example `"u+x"`.
        # Returns
            `dict`: the updated dataset metadata
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]
        headers = {"content-type": "application/json"}
        query_params = {"action": "PERMISSION", "permissions": permissions}
        return _client._send_request(
            "PUT", path_params, headers=headers, query_params=query_params
        )

    # region Archiving

    def _archive(
        self,
        remote_path: str,
        destination_path: Optional[str] = None,
        block: bool = False,
        timeout: Optional[int] = 120,
        action: Union[Literal["unzip"], Literal["zip"]] = "unzip",
    ):
        """Internal (de)compression logic.

        # Arguments
            remote_path: path to file or directory to unzip.
            destination_path: path to upload the zip, defaults to None; is used only if action is zip.
            block: if the operation should be blocking until complete, defaults to False.
            timeout: timeout in seconds for the blocking, defaults to 120; if None, the blocking is unbounded.
            action: zip or unzip, defaults to unzip.

        # Returns
            `bool`: whether the operation completed in the specified timeout; if non-blocking, always returns True.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "dataset", remote_path]

        query_params = {"action": action}

        if destination_path is not None:
            query_params["destination_path"] = destination_path
            query_params["destination_type"] = "DATASET"

        headers = {"content-type": "application/json"}

        _client._send_request(
            "POST", path_params, headers=headers, query_params=query_params
        )

        if not block:
            # the call is successful at this point if we don't want to block
            return True

        # Wait for zip file to appear. When it does, check that parent dir zipState is not set to CHOWNING
        count = 0
        while True:
            if action == "zip":
                zip_path = remote_path + ".zip"
                # Get the status of the zipped file
                if destination_path is None:
                    zip_exists = self.path_exists(zip_path)
                else:
                    zip_exists = self.path_exists(
                        destination_path + "/" + os.path.split(zip_path)[1]
                    )
                # Get the zipState of the directory being zipped
                dir_status = self.get(remote_path)
                zip_state = dir_status["zipState"] if "zipState" in dir_status else None
                if zip_exists and zip_state == "NONE":
                    return True
            elif action == "unzip":
                # Get the status of the unzipped dir
                unzipped_dir_exists = self.path_exists(
                    remote_path[: remote_path.index(".")]
                )
                # Get the zipState of the zip being extracted
                dir_status = self.get(remote_path)
                zip_state = dir_status["zipState"] if "zipState" in dir_status else None
                if unzipped_dir_exists and zip_state == "NONE":
                    return True
            time.sleep(1)
            count += 1
            if timeout is not None and count >= timeout:
                self._log.info(
                    f"Timeout of {timeout} seconds exceeded while {action} {remote_path}."
                )
                return False

    def unzip(
        self, remote_path: str, block: bool = False, timeout: Optional[int] = 120
    ):
        """Unzip an archive in the dataset.

        # Arguments
            remote_path: path to file or directory to unzip.
            block: if the operation should be blocking until complete, defaults to False.
            timeout: timeout in seconds for the blocking, defaults to 120; if None, the blocking is unbounded.

        # Returns
            `bool`: whether the operation completed in the specified timeout; if non-blocking, always returns True.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._archive(remote_path, block=block, timeout=timeout, action="unzip")

    def zip(
        self,
        remote_path: str,
        destination_path: Optional[str] = None,
        block: bool = False,
        timeout: Optional[int] = 120,
    ):
        """Zip a file or directory in the dataset.

        # Arguments
            remote_path: path to file or directory to unzip.
            destination_path: path to upload the zip, defaults to None.
            block: if the operation should be blocking until complete, defaults to False.
            timeout: timeout in seconds for the blocking, defaults to 120; if None, the blocking is unbounded.

        # Returns
            `bool`: whether the operation completed in the specified timeout; if non-blocking, always returns True.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._archive(
            remote_path,
            destination_path=destination_path,
            block=block,
            timeout=timeout,
            action="zip",
        )

    # region Dataset Tags

    def add(self, path: str, name: str, value: str):
        """**Deprecated**

        Attach a name/value tag to a model.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        # Arguments
            path: path to add the tag
            name: name of the tag to be added
            value: value of the tag to be added
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "tags",
            "schema",
            name,
            path,
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    def delete(self, path: str, name: str):
        """**Deprecated**

        Delete a tag.

        Tag names are unique identifiers.

        # Arguments
            path: path to delete the tags
            name: name of the tag to be removed
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "tags",
            "schema",
            name,
            path,
        ]
        _client._send_request("DELETE", path_params)

    def get_tags(self, path: str, name: str | None = None):
        """**Deprecated**

        Get the tags.

        Gets all tags if no tag name is specified.

        # Arguments
            path: path to get the tags
            name: tag name

        # Returns
            `dict`: tag names and values
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dataset",
            "tags",
        ]

        if name is not None:
            path_params.append("schema")
            path_params.append(name)
        else:
            path_params.append("all")

        path_params.append(path)

        return {
            tag._name: json.loads(tag._value)
            for tag in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }
