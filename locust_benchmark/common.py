import json
import os
from pathlib import Path

import hopsworks


# LOCUST_HOPSWORKS_CONFIG selects the configuration file; the default sits next to this module.
CONFIG = json.loads(
    Path(
        os.environ.get(
            "LOCUST_HOPSWORKS_CONFIG", Path(__file__).parent / "hopsworks_config.json"
        )
    ).read_text()
)
VERSION = 1
FG_NAME = "locust_fg"
META_FG_NAME = "locust_ip_meta_fg"
FV_NAME = "locust_fv"
JOIN_FV_NAME = "locust_join_fv"


def login():
    return hopsworks.login(
        host=CONFIG["host"],
        port=CONFIG["port"],
        project=CONFIG["project"],
        api_key_file=os.environ.get(
            "HOPSWORKS_API_KEY_FILE", str(Path(__file__).parent / ".api_key")
        ),
        engine="python",
    )
