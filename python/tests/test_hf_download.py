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
"""Unit tests for ModelRegistry.hf_download.

The HTTP layer (HuggingFaceApi / ModelApi) is mocked so we exercise the
client-side state machine: kick off → poll → success/failure-mapping.
"""

from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import (
    HuggingFaceImportException,
    RestAPIError,
)
from hsml.model_registry import ModelRegistry


def _make_registry():
    """Construct a ModelRegistry with both downstream APIs mocked."""
    mr = ModelRegistry(
        project_name="proj",
        project_id=119,
        model_registry_id=119,
    )
    mr._huggingface_api = MagicMock()
    mr._model_api = MagicMock()
    return mr


# ---- happy path ----


def test_hf_download_completed_returns_model(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-1"}
    mr._huggingface_api.get_status.side_effect = [
        {"status": "RUNNING", "completedFiles": 0, "totalFiles": 3},
        {"status": "COMPLETED", "completedFiles": 3, "totalFiles": 3},
    ]
    mock_model = MagicMock()
    mock_model.version = 1
    mr._model_api.get_models.return_value = [mock_model]
    monkeypatch.setattr("time.sleep", lambda _: None)

    result = mr.hf_download("Qwen/Qwen2.5-0.5B", poll_interval=0)

    assert result is mock_model
    mr._huggingface_api.start_import.assert_called_once()
    args = mr._huggingface_api.start_import.call_args
    # registry id should be the first positional argument
    assert args.args[0] == 119
    assert args.kwargs.get("hf_token") is None
    # No selection args supplied → backend defaults are used.
    assert args.kwargs.get("selected_formats") is None
    assert args.kwargs.get("selected_variants") is None


def test_hf_download_passes_selection_args_through(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-2"}
    mr._huggingface_api.get_status.return_value = {"status": "COMPLETED"}
    mr._model_api.get_models.return_value = [MagicMock(version=1)]
    monkeypatch.setattr("time.sleep", lambda _: None)

    mr.hf_download(
        "unsloth/Kimi-K2.6-GGUF",
        hf_token="hf_secret",
        selected_formats=["gguf"],
        selected_variants=["UD-Q4_K_XL"],
        selected_filenames=None,
        poll_interval=0,
    )
    kwargs = mr._huggingface_api.start_import.call_args.kwargs
    assert kwargs["hf_token"] == "hf_secret"
    assert kwargs["selected_formats"] == ["gguf"]
    assert kwargs["selected_variants"] == ["UD-Q4_K_XL"]


def test_hf_download_resolves_sanitised_model_name(monkeypatch):
    """Sanitised model name lookup after a successful import.

    The HF importer replaces non-alphanumerics in the repo name with '_';
    the Python client mirrors that when looking up the registered model.
    """
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-3"}
    mr._huggingface_api.get_status.return_value = {"status": "COMPLETED"}
    mock_model = MagicMock(version=2)
    mr._model_api.get_models.return_value = [MagicMock(version=1), mock_model]
    monkeypatch.setattr("time.sleep", lambda _: None)

    result = mr.hf_download("Qwen/Qwen2.5-0.5B", poll_interval=0)

    # Sanitiser: "Qwen2.5-0.5B" → "Qwen2_5_0_5B"
    mr._model_api.get_models.assert_called_with(
        "Qwen2_5_0_5B",
        119,
        shared_registry_project_name=None,
    )
    # Highest version wins.
    assert result is mock_model


def test_hf_download_strips_full_url_for_lookup(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-4"}
    mr._huggingface_api.get_status.return_value = {"status": "COMPLETED"}
    mr._model_api.get_models.return_value = [MagicMock(version=1)]
    monkeypatch.setattr("time.sleep", lambda _: None)

    mr.hf_download(
        "https://huggingface.co/Qwen/Qwen2.5-0.5B/tree/main",
        poll_interval=0,
    )
    # Looked up under the sanitised LAST path segment, not the URL.
    name_arg = mr._model_api.get_models.call_args.args[0]
    assert name_arg == "Qwen2_5_0_5B"


# ---- failure mapping ----


@pytest.mark.parametrize(
    "error_field, expected_code",
    [
        ("auth_required", "auth_required"),
        ("not_found_or_auth_required", "not_found_or_auth_required"),
        ("model_not_found", "model_not_found"),
        ("no_disk_space", "no_disk_space"),
        # Suffix-style codes — the prefix becomes error_code, full message preserved.
        ("download_failed: model.safetensors", "download_failed"),
        ("invalid_filename: ../etc/passwd", "invalid_filename"),
        ("registration_failed: db error", "registration_failed"),
        ("fetch_failed", "fetch_failed"),
    ],
)
def test_hf_download_failed_maps_error_code(monkeypatch, error_field, expected_code):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-x"}
    mr._huggingface_api.get_status.return_value = {
        "status": "FAILED",
        "error": error_field,
    }
    monkeypatch.setattr("time.sleep", lambda _: None)

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("foo/bar", poll_interval=0)
    assert exc.value.error_code == expected_code
    # Full backend message preserved for the user.
    assert error_field in str(exc.value)


def test_hf_download_cancelled_raises(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-c"}
    mr._huggingface_api.get_status.return_value = {
        "status": "CANCELLED",
        "error": "auth_required",
    }
    monkeypatch.setattr("time.sleep", lambda _: None)

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("foo/bar", poll_interval=0)
    assert exc.value.error_code == "auth_required"


# ---- synchronous start_import errors → typed exception ----


def _rest_error(status: int, body: dict) -> RestAPIError:
    """Build a RestAPIError with a controlled response body."""
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body
    response.url = "http://test"
    response.text = ""
    err = RestAPIError.__new__(RestAPIError)
    err.response = response
    err.url = "http://test"
    return err


def test_hf_download_start_returns_401_auth_required(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.side_effect = _rest_error(
        401, {"error": "auth_required"}
    )

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("gated/repo", hf_token="bad", poll_interval=0)
    assert exc.value.error_code == "auth_required"


def test_hf_download_start_returns_404_not_found_or_auth(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.side_effect = _rest_error(
        404, {"error": "not_found_or_auth_required"}
    )

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("typo/repo", poll_interval=0)
    assert exc.value.error_code == "not_found_or_auth_required"


def test_hf_download_start_returns_404_model_not_found(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.side_effect = _rest_error(
        404, {"error": "model_not_found"}
    )

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("bad/repo", hf_token="hf_x", poll_interval=0)
    assert exc.value.error_code == "model_not_found"


def test_hf_download_unknown_rest_error_passes_through(monkeypatch):
    """Unknown RestAPIError must pass through, not be re-wrapped.

    A RestAPIError whose body doesn't match any known HF code should reach
    the caller verbatim — re-wrapping it as HuggingFaceImportException would
    surface a misleading code that users couldn't trust.
    """
    mr = _make_registry()
    mr._huggingface_api.start_import.side_effect = _rest_error(
        500, {"error": "internal_db_error"}
    )

    with pytest.raises(RestAPIError):
        mr.hf_download("foo/bar", poll_interval=0)


def test_hf_download_start_returns_no_jobid(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {}

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("foo/bar", poll_interval=0)
    assert exc.value.error_code == "fetch_failed"


def test_hf_download_timeout_attempts_cleanup(monkeypatch):
    mr = _make_registry()
    mr._huggingface_api.start_import.return_value = {"jobId": "job-t"}
    mr._huggingface_api.get_status.return_value = {"status": "RUNNING"}
    monkeypatch.setattr("time.sleep", lambda _: None)
    monkeypatch.setattr("time.time", _ticking_clock(start=0.0, step=10.0))

    with pytest.raises(HuggingFaceImportException) as exc:
        mr.hf_download("foo/bar", timeout=5, poll_interval=0)
    assert exc.value.error_code == "fetch_failed"
    # Best-effort cleanup must have been invoked with cleanup=True.
    mr._huggingface_api.cancel.assert_called_once_with(119, "job-t", cleanup=True)


def _ticking_clock(start: float, step: float):
    """A deterministic time.time() that advances by `step` on every call."""
    state = {"now": start}

    def _now():
        v = state["now"]
        state["now"] += step
        return v

    return _now
