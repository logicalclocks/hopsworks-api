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

from __future__ import annotations

import os


class HdfsApi:
    def __init__(self):
        import fsspec.implementations.arrow as pfs

        host, port = os.environ["LIBHDFS_DEFAULT_FS"].split(":")

        self._hopsfs = pfs.HadoopFileSystem(
            host=host,
            port=int(port),
            user=os.environ["LIBHDFS_DEFAULT_USER"],
        )

    DEFAULT_BUFFER_SIZE = 0

    def upload(
        self,
        local_path: str,
        upload_path: str,
        overwrite: bool = False,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
    ):
        """Upload file/directory to the Hopsworks filesystem.
        :param local_path: local path to file to upload
        :type local_path: str
        :param upload_path: path to directory where to upload the file in Hopsworks filesystem
        :type upload_path: str
        :param overwrite: overwrite file if exists
        :type overwrite: bool
        :param buffer_size: size of the temporary read and write buffer. Defaults to 0.
        :type buffer_size: int
        """
        # local path could be absolute or relative,
        if not os.path.isabs(local_path) and os.path.exists(
            os.path.join(os.getcwd(), local_path)
        ):
            local_path = os.path.join(os.getcwd(), local_path)

        _, file_name = os.path.split(local_path)

        destination_path = upload_path + "/" + file_name

        if self._hopsfs.exists(destination_path):
            if overwrite:
                self._hopsfs.rm(destination_path, recursive=True)
            else:
                raise Exception(
                    "{} already exists, set overwrite=True to overwrite it".format(
                        local_path
                    )
                )

        self._hopsfs.upload(
            lpath=local_path,
            rpath=destination_path,
            recursive=True,
            buffer_size=buffer_size,
        )

        return upload_path + "/" + os.path.basename(local_path)

    def download(self, path, local_path, buffer_size=DEFAULT_BUFFER_SIZE):
        """Download file/directory on a path in datasets.
        :param path: path to download
        :type path: str
        :param local_path: path to download in datasets
        :type local_path: str
        :param buffer_size: size of the temporary read and write buffer. Defaults to 0.
        :type buffer_size: int
        """

        self._hopsfs.download(path, local_path, recursive=True, buffer_size=buffer_size)
