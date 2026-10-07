"""``hops datasource credentials`` acts on the caller's own binding.

``set`` stages the wallet under a new name, validates, then saves, and saves nothing
when the data source rejects the credentials; ``validate`` stops after the test and
removes the staged wallet; ``show`` and ``delete`` read and remove the binding.
"""

from __future__ import annotations

import os
import re
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.commands import datasource as ds
from hopsworks.cli.main import cli


@pytest.fixture
def connector(mock_project):
    """The SqlConnector mock ``fs.get_data_source(<name>).storage_connector`` resolves to."""
    fs = mock_project.get_feature_store.return_value
    sc = mock.MagicMock(name="SqlConnector")
    sc.credentials_mode = "PROVIDED"
    sc._env_name.return_value = "ORACLE_SALES"
    sc._validate_credentials.return_value = {"valid": True}
    sc.set_credentials.return_value = {
        "status": "VALID",
        "username_env_var": "DS_ORACLE_SALES_67_USER",
        "password_secret_name": "ds_oracle_sales_67_password",
        "validated_at": "2026-10-07T10:00:00Z",
    }
    sc.get_credentials.return_value = sc.set_credentials.return_value
    fs.get_data_source.return_value.storage_connector = sc
    return sc


def _run(argv, **kwargs):
    return CliRunner().invoke(cli, ["datasource", "credentials", *argv], **kwargs)


def test_set_validates_then_saves(connector):
    result = _run(["set", "oracle-sales", "--user", "SCOTT", "--password", "tiger"])

    assert result.exit_code == 0, result.output
    expected = {
        "user": "SCOTT",
        "password": "tiger",
        "wallet_password": None,
        "user_env_var": None,
        "password_secret": None,
        "wallet_password_secret": None,
    }
    connector._validate_credentials.assert_called_once_with(**expected)
    connector.set_credentials.assert_called_once_with(**expected)
    assert connector.mock_calls.index(
        mock.call._validate_credentials(**expected)
    ) < connector.mock_calls.index(mock.call.set_credentials(**expected))
    assert "DS_ORACLE_SALES_67_USER" in result.output
    assert "tiger" not in result.output


def test_set_saves_nothing_when_validation_fails(connector):
    connector._validate_credentials.return_value = {
        "valid": False,
        "errorCode": "ORA-01017",
        "message": "invalid username/password; logon denied",
    }

    result = _run(["set", "oracle-sales", "--user", "SCOTT", "--password", "wrong"])

    assert result.exit_code == 1, result.output
    assert "ORA-01017" in result.output
    assert "logon denied" in result.output
    connector.set_credentials.assert_not_called()


_BOUND_WALLET = "Users/scott/.datasources/oracle_sales/wallet-bound.zip"


@pytest.fixture
def dataset_api(mock_project):
    dataset_api = mock_project.get_dataset_api.return_value
    dataset_api.exists.return_value = False
    user = mock.MagicMock()
    user.username = "scott"
    users_api = mock.MagicMock()
    users_api._get_current_user.return_value = user
    with mock.patch("hopsworks.get_users_api", return_value=users_api, create=True):
        yield dataset_api


def _with_wallet(command, wallet):
    return [
        command,
        "oracle-sales",
        "--user",
        "SCOTT",
        "--password",
        "tiger",
        "--wallet",
        str(wallet),
        "--wallet-password",
        "wp",
    ]


@pytest.fixture
def wallet(tmp_path):
    wallet = tmp_path / "Wallet_db.zip"
    wallet.write_bytes(b"PK")
    return wallet


def _staged(dataset_api):
    staged, target_dir = dataset_api.upload.call_args.args
    return target_dir, os.path.basename(staged)


def test_set_stages_the_wallet_under_a_new_name_in_the_callers_home(
    connector, dataset_api, wallet
):
    result = _run(_with_wallet("set", wallet))

    assert result.exit_code == 0, result.output
    assert dataset_api.mkdir.call_args_list == [
        mock.call("Users/scott/.datasources"),
        mock.call("Users/scott/.datasources/oracle_sales"),
    ]
    target_dir, file_name = _staged(dataset_api)
    assert target_dir == "Users/scott/.datasources/oracle_sales"
    assert re.fullmatch(r"wallet-[0-9a-f]{32}\.zip", file_name)
    assert dataset_api.upload.call_args.kwargs == {}
    wallet_path = f"/Projects/demo/{target_dir}/{file_name}"
    assert (
        connector._validate_credentials.call_args.kwargs["wallet_path"] == wallet_path
    )
    assert connector.set_credentials.call_args.kwargs["wallet_path"] == wallet_path
    assert connector.set_credentials.call_args.kwargs["wallet_password"] == "wp"
    dataset_api.remove.assert_not_called()


def test_each_upload_gets_its_own_name(connector, dataset_api, wallet):
    _run(_with_wallet("set", wallet))
    first = _staged(dataset_api)
    _run(_with_wallet("set", wallet))

    assert _staged(dataset_api) != first


def test_a_rejected_wallet_is_removed_and_the_bound_one_untouched(
    connector, dataset_api, wallet
):
    connector.user_credentials = {
        "status": "VALID",
        "wallet_path": f"/Projects/demo/{_BOUND_WALLET}",
    }
    connector._validate_credentials.return_value = {"valid": False, "message": "no"}

    result = _run(_with_wallet("set", wallet))

    assert result.exit_code == 1, result.output
    target_dir, file_name = _staged(dataset_api)
    dataset_api.remove.assert_called_once_with(f"{target_dir}/{file_name}")
    touched = [c.args[0] for c in dataset_api.upload.call_args_list] + [
        c.args[0] for c in dataset_api.remove.call_args_list
    ]
    assert not any(path.endswith(_BOUND_WALLET) for path in touched)
    assert not any(path.endswith("/wallet.zip") for path in touched)
    connector.set_credentials.assert_not_called()


def test_a_failed_save_removes_the_staged_wallet(connector, dataset_api, wallet):
    connector.set_credentials.side_effect = RuntimeError("boom")

    result = _run(_with_wallet("set", wallet))

    assert result.exit_code == 1, result.output
    assert "boom" in result.output
    target_dir, file_name = _staged(dataset_api)
    dataset_api.remove.assert_called_once_with(f"{target_dir}/{file_name}")


@pytest.mark.parametrize("valid", [True, False])
def test_validate_removes_the_staged_wallet_either_way(
    connector, dataset_api, wallet, valid
):
    connector._validate_credentials.return_value = {"valid": valid}

    result = _run(_with_wallet("validate", wallet))

    assert result.exit_code == (0 if valid else 1), result.output
    target_dir, file_name = _staged(dataset_api)
    dataset_api.remove.assert_called_once_with(f"{target_dir}/{file_name}")
    connector.set_credentials.assert_not_called()


def test_set_takes_account_entry_names_instead_of_values(connector):
    result = _run(
        [
            "set",
            "oracle-sales",
            "--user-env-var",
            "MY_USER",
            "--password-secret",
            "my_pwd",
        ]
    )

    assert result.exit_code == 0, result.output
    kwargs = connector.set_credentials.call_args.kwargs
    assert kwargs["user_env_var"] == "MY_USER"
    assert kwargs["password_secret"] == "my_pwd"
    assert kwargs["user"] is None
    assert kwargs["password"] is None


@pytest.mark.parametrize(
    "argv, refused",
    [
        (
            ["--user", "u", "--user-env-var", "U", "--password", "p"],
            "--user and --user-env-var are alternatives",
        ),
        (
            ["--user", "u", "--password", "p", "--password-secret", "s"],
            "--password and --password-secret are alternatives",
        ),
        (["--password", "p"], "Pass --user or --user-env-var"),
        (
            ["--user", "u", "--password", "p", "--wallet-password", "w"],
            "needs --wallet",
        ),
    ],
)
def test_conflicting_or_missing_options_are_refused(connector, argv, refused):
    result = _run(["set", "oracle-sales", *argv])

    assert result.exit_code == 2, result.output
    assert refused in result.output
    connector._validate_credentials.assert_not_called()
    connector.set_credentials.assert_not_called()


def test_a_missing_password_is_an_error_off_a_terminal(connector, monkeypatch):
    monkeypatch.setattr(ds, "_interactive", lambda: False)

    result = _run(["set", "oracle-sales", "--user", "u"])

    assert result.exit_code == 2, result.output
    assert "HOPSWORKS_DS_CREDENTIALS_PASSWORD" in result.output
    connector.set_credentials.assert_not_called()


def test_the_password_is_read_from_the_credentials_environment_variable(connector):
    result = _run(
        ["set", "oracle-sales", "--user", "u"],
        env={"HOPSWORKS_DS_CREDENTIALS_PASSWORD": "from-env"},
    )

    assert result.exit_code == 0, result.output
    assert connector.set_credentials.call_args.kwargs["password"] == "from-env"


def test_validate_does_not_save(connector):
    result = _run(["validate", "oracle-sales", "--user", "u", "--password", "p"])

    assert result.exit_code == 0, result.output
    assert "accepted" in result.output
    connector._validate_credentials.assert_called_once()
    connector.set_credentials.assert_not_called()


def test_show_prints_names_only(connector):
    result = _run(["show", "oracle-sales"])

    assert result.exit_code == 0, result.output
    assert "VALID" in result.output
    assert "ds_oracle_sales_67_password" in result.output


def test_delete_asks_then_removes(connector):
    result = _run(["delete", "oracle-sales"], input="y\n")

    assert result.exit_code == 0, result.output
    connector.delete_credentials.assert_called_once_with()


def test_a_shared_data_source_is_refused(connector):
    connector.credentials_mode = "SHARED"

    result = _run(["show", "oracle-sales"])

    assert result.exit_code == 1, result.output
    assert "shared credentials" in result.output
    connector.get_credentials.assert_not_called()


def test_list_shows_the_credentials_column(mock_project):
    connectors = [
        {"id": 1, "name": "shared_one", "storageConnectorType": "SQL"},
        {
            "id": 2,
            "name": "mine",
            "storageConnectorType": "SQL",
            "credentialsMode": "PROVIDED",
            "userCredentials": {"status": "VALID"},
        },
        {
            "id": 3,
            "name": "not_yet",
            "storageConnectorType": "SQL",
            "credentialsMode": "PROVIDED",
            "userCredentials": {"status": "MISSING"},
        },
        {
            "id": 4,
            "name": "broken",
            "storageConnectorType": "SQL",
            "credentialsMode": "PROVIDED",
            "userCredentials": {"status": "INCOMPLETE"},
        },
    ]
    with mock.patch.object(ds, "_list_connectors", return_value=connectors):
        result = CliRunner().invoke(cli, ["--json", "datasource", "list"])

    assert result.exit_code == 0, result.output
    import json

    labels = {row["NAME"]: row["CREDENTIALS"] for row in json.loads(result.stdout)}
    assert labels == {
        "shared_one": "shared",
        "mine": "provided: yours set",
        "not_yet": "provided: missing",
        "broken": "provided: incomplete",
    }
