# Provided credentials for a data source

A data source with `credentials_mode = "PROVIDED"` stores no username, password, wallet or wallet password.
Each project member binds their own, and every read as that member uses them: SDK reads, Arrow Flight reads, data source browsing, dlt ingestion jobs, Spark on-demand feature groups and Trino catalogs built from the data source.

## Where a member's credentials live

| Credential | Stored as | Default name |
| --- | --- | --- |
| Username | Env var in the member's account, private | `DS_<NAME>_<feature store id>_USER` |
| Password | Secret in the member's account, private | `ds_<name>_<feature store id>_password` |
| Wallet zip (optional) | A zip under `/Projects/<project>/Users/<username>/.datasources/<name>/` | `wallet-<random hex>.zip` when the CLI uploads it |
| Wallet password (optional) | Secret in the member's account, private | `ds_<name>_<feature store id>_wallet_password` |

`<NAME>` is the data source name upper-cased with every character outside `[A-Z0-9_]` replaced by `_`; `<name>` is the same lower-cased.
A data source named `oracle-sales` in feature store 67 gives `DS_ORACLE_SALES_67_USER` and `ds_oracle_sales_67_password`.
The feature store id keeps the defaults of same-named data sources in two projects apart.

Instead of typing a value, a member can reference an existing env var or secret from their account.
The binding records names and the wallet path, never values.
Removing the binding deletes the wallet file and the default-named env var and secrets above.
An env var or secret the member named themselves stays, since they may use it elsewhere.
Removing a member from the project and deleting the data source clean up the same way.

## SDK

```python
sc = fs.get_data_source("oracle-sales").storage_connector
sc.credentials_mode              # "SHARED" or "PROVIDED"
sc.user_credentials              # your binding as the backend returned it with the data source, names only
sc.user, sc.password             # read from your own account entries at each access; the data source GET carries no values

sc.validate_credentials(user="SCOTT", password="...")        # True or False, stores nothing
sc.set_credentials(user="SCOTT", password="...")             # validates, saves; sc reads with them at once; FeatureStoreException when rejected
sc.set_credentials(user_env_var="MY_USER", password_secret="my_oracle_pwd")   # reuse account entries
sc.set_credentials(password="...")                          # rotate the password; the bound username and wallet are kept
sc.set_credentials(user="SCOTT", password="...",
                   wallet_path="/Projects/p/Users/scott/.datasources/oracle_sales/wallet-1.zip",
                   wallet_password="...")                   # upload the wallet to that path first
sc.get_credentials()             # {"status": "VALID" | "MISSING" | "INCOMPLETE", "username_env_var", "password_secret_name", "wallet_path", "wallet_password_secret_name", "validated_at"}
sc.delete_credentials()
```

`set_credentials` takes the wallet as a HopsFS path inside your own home directory in the project.
The CLI uploads a local zip there for you; from the SDK, upload it with `project.get_dataset_api().upload(local_zip, "Users/<username>/.datasources/<name>")` first.
Upload a replacement under a new file name rather than over the bound one: a rejected replacement then leaves the wallet you use untouched.
The CLI does this itself, uploading to `wallet-<random hex>.zip`; `credentials validate` removes that file afterwards, and `credentials set` removes it when the credentials are rejected.
After a successful save the backend binds the new file and deletes the one it replaces.
A call without `wallet_path` (or the CLI without `--wallet`) keeps the wallet already bound; to drop it, remove the binding first.

A `PROVIDED` data source is created without any credentials.
`SqlConnector(credentials_mode="PROVIDED", user=...).save()` raises `ValueError` before contacting the backend; create it without them and call `set_credentials` once it exists.

Using a `PROVIDED` data source without a valid binding (`spark_options()`, `read()`, `get_tables()`, `get_databases()`, `get_data()`) raises `hopsworks.client.exceptions.FeatureStoreException` naming `set_credentials`.
`INCOMPLETE` means a referenced secret or env var was deleted from your account since you saved the binding; call `set_credentials` again.

## Env variables in Hopsworks runtimes

Every job, notebook, terminal, app and Ray cluster a member starts receives, for each `PROVIDED` data source in the project's own feature store that the member has a complete binding for:

| Variable | Value |
| --- | --- |
| `HOPS_DS_<NAME>_USER` | username |
| `HOPS_DS_<NAME>_PASSWORD` | password |
| `HOPS_DS_<NAME>_WALLET_PATH` | the bound wallet's `/hopsfs/Users/<username>/.datasources/<name>/...` path, when a wallet is bound |
| `HOPS_DS_<NAME>_WALLET_PASSWORD` | wallet password, when set |
| `HOPS_DS_<NAME>_CONNECTOR_ID` | the data source's id, which says which data source the other four belong to |

Model deployments receive none of these, since a deployment is shared by the whole project.

Getting or listing a `PROVIDED` data source never returns a username, password or wallet password, only the names in `user_credentials`.
The SDK reads the values from your own account each time it builds a connection: the username from the env var named `username_env_var`, the password and wallet password from the secrets named `password_secret_name` and `wallet_password_secret_name`.
It falls back to these variables only when it cannot read your account (for example with an API key that lacks the `USER` scope), and only when `HOPS_DS_<NAME>_CONNECTOR_ID` equals that data source's `id`.
An account entry that was deleted is not replaced by a variable: the binding is `INCOMPLETE` until you set the credentials again.
A same-named data source read from another project's feature store has another id and never picks them up.
The SDK reads the wallet from `HOPS_DS_<NAME>_WALLET_PATH` when that file exists, is readable and has the bound wallet's file name; Spark does not mount `/hopsfs`, so there the SDK downloads the bound wallet from HopsFS instead.
`sc.wallet_path` then returns that local `/hopsfs/...` path, which HopsFS APIs such as the dataset API do not accept.
A runtime keeps the variables it started with: after `set_credentials` or a rotation made anywhere else, the SDK reads with the new credentials, while code that reads the variables itself needs a restart.
After `set_credentials` or `delete_credentials` on an object, that object never falls back to the variables.
Code that connects to the database itself (`oracledb`, a JDBC driver) should use these names; the member's own `DS_<NAME>_<feature store id>_USER` env var is also injected under its own name.
Two `PROVIDED` data sources in one feature store cannot normalise to the same `<NAME>`; the second create is rejected.

## Trino

A Trino catalog built from a `PROVIDED` data source works, and each member's queries on it run with that member's own login.
The catalog holds no credentials; Trino takes them from the extra credentials `hops_ds_<data source id>_user` and `hops_ds_<data source id>_password` sent with each query.
`project.get_trino_api().connect()` and `create_engine()` fetch your own credentials and send them for you; the extra-credentials endpoint they use returns only your own values.
Any other Trino client (the Trino CLI, JDBC, Superset, dbt) must send those two extra credentials itself, for example `--extra-credential hops_ds_42_user=SCOTT --extra-credential hops_ds_42_password=...` for data source id 42; without them the query fails with an authentication error from the database.

## Access states on mounted tables

For each external feature group mounted from a `PROVIDED` data source, Hopsworks keeps one access state per member, re-checked when the member saves or removes credentials and when a Data owner mounts a new table.

| State | Meaning |
| --- | --- |
| `PENDING` | A check is queued |
| `OK` | Your credentials read the backing table |
| `NO_CREDENTIALS` | You have no complete binding for the data source |
| `INVALID_CREDENTIALS` | The database rejected the login (`ORA-01017`, `ORA-28000`) |
| `NO_ACCESS` | Login works but the table cannot be read (`ORA-00942`, `ORA-01031`); your account lacks a grant |
| `ERROR` | Anything else (network, TNS, timeout); the message says what |

```python
fg = fs.get_feature_group("sales_external", version=1)
fg.data_source_access            # {"status", "error_code", "message", "checked_at"}, None for a SHARED data source
fg.test_data_source_access()     # runs the check now as you and returns the stored state
```

The state informs the UI and the CLI; it does not gate reads.
A read in `NO_ACCESS` still goes to the database and fails there with the database's own error.

## Roles

| Action | Data owner | Data scientist | Feature store restricted | Observer |
| --- | --- | --- | --- | --- |
| Create or delete a data source, choose its mode | yes | no | no | no |
| Mount an external feature group | yes | no | no | no |
| Add, validate, update, remove own credentials | yes | yes | yes | no |
| Test access on a feature group the member can see | yes | yes | yes | no |

A Data owner who creates a `PROVIDED` data source adds their own credentials before browsing its tables or mounting feature groups, since both run as the caller.
