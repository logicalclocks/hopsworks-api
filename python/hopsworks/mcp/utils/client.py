import os
from typing import Optional

import httpx


class HopsworksClient:
    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 443,
        api_key_value: Optional[str] = None,
        api_key_file: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.api_key_value = api_key_value
        self.api_key_file = api_key_file
        self.base_url = f"https://{self.host}:{self.port}/hopsworks-api/api"

        self.api_key = None
        if (
            api_key_value is None
            and api_key_file is None
            and "HOPSWORKS_API_KEY" in os.environ
        ):
            self.api_key = os.environ["HOPSWORKS_API_KEY"]
        elif api_key_value:
            self.api_key = api_key_value
        elif api_key_file:
            with open(api_key_file, "r") as f:
                self.api_key = f.read().strip()

    def get_projects(self):
        url = f"{self.base_url}/project/"
        response = httpx.get(url)
        response.raise_for_status()
        return response.json()
