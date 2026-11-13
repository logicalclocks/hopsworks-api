# Create a connector

Connectors are not UI-only.
`hops datasource create <type>` exists for every type the UI offers and sends the backend type discriminator for you.
`hops datasource create <type> --help` lists the options; the ones the backend always requires are marked `[required]`, and the rules between options (which credentials an auth type needs, Oracle's host or wallet) are checked before anything is sent.
An option that does not belong to the selected mode is refused, not silently stored.

**Never put a secret on the command line.**
Passwords, keys and tokens written as arguments end up in tool transcripts, process listings and shell history.
Every secret option takes `-` to read the value from stdin, so feed it from a file the user already has (`--password - < /path/to/secret`), or export `HOPSWORKS_DS_<CONNECTOR>_<OPTION>` beforehand, outside the transcript (`HOPSWORKS_DS_SQL_PASSWORD`, `HOPSWORKS_DS_REST_API_KEY`, `HOPSWORKS_DS_UNITY_CATALOG_CLIENT_SECRET`); the variables are scoped to one connector type, and `--help` names them.
Only one option per command can read stdin, so a connector with several secrets takes the rest from its variables.
A required secret that is neither given nor exported is asked for without echo when a person is at the terminal, and is an error otherwise.

```bash
hops datasource create jdbc <name> --url "jdbc:postgresql://host:5432/db" --user U --password - < pw.txt
hops datasource create sql <name> --database-type POSTGRESQL --host H --port 5432 --database D --user U --password - < pw.txt
hops datasource create sql <name> --database-type ORACLE --wallet-path /Projects/<project>/Resources/wallet --port 1522 --database TNS_ALIAS --user U --password - < pw.txt   # HOPSWORKS_DS_SQL_WALLET_PASSWORD for the wallet
hops datasource create sql <name> --database-type CLICKHOUSE --host H --port 8123 --database D --user U --password - < pw.txt   # --argument ssl=true for HTTPS (8443)
hops datasource create sql <name> --database-type TERADATA --host H --port 1025 --database D --user U --password - < pw.txt   # --argument logmech=LDAP for LDAP logon; a Trino catalog needs the Teradata plugin in the cluster's Trino image and `teradata` in the trino_connectors variable
hops datasource create sql <name> --database-type SQLSERVER --host H --port 1433 --database D --user U --password - < pw.txt   # tables are database.schema.table, so pick the schema (often dbo) as the group; --argument trustServerCertificate=true for a self-signed server certificate (Spark, Trino)
hops datasource create s3 <name> --bucket my-bucket --access-key AK --secret-key - --region eu-north-1 < secret_key.txt
hops datasource create gcs <name> --bucket my-bucket --key-path /Projects/<project>/Resources/key.json
hops datasource create adls <name> --account-name A --generation 2 --container-name C --directory-id T --application-id APP --service-credential - < credential.txt
hops datasource create hopsfs <name> --dataset Resources
hops datasource create snowflake <name> --url https://acct.snowflakecomputing.com --user U --database D --schema S --warehouse W --password - < pw.txt
hops datasource create bigquery <name> --parent-project billed-proj --project-id proj --dataset ds --key-path /Projects/<project>/Resources/key.json
hops datasource create redshift <name> --cluster-identifier C --endpoint E --database D --port 5439 --user U --password - < pw.txt
hops datasource create glue <name> --database D --region eu-north-1 --iam-role arn:aws:iam::123456789012:role/R
hops datasource create unity-catalog <name> --workspace-url https://dbc-xxx.cloud.databricks.com --access-token - < token.txt
hops datasource create unity-catalog <name> --workspace-url https://dbc-xxx.cloud.databricks.com --auth-method OAUTH_M2M --oauth-endpoint WORKSPACE --client-id C --client-secret - < client_secret.txt
hops datasource create sap-hana <name> --host H --user U --password - < pw.txt
hops datasource create mongodb <name> --connection-string mongodb+srv://cluster.mongodb.net --database D --user U --password - < pw.txt
hops datasource create kafka <name> --bootstrap-servers broker:9092 --security-protocol PLAINTEXT --external
hops datasource create opensearch <name> --host H --port 9200 --user U --password - < pw.txt
hops datasource create elasticsearch <name> --host H --user U --password - < pw.txt   # port 9200, https; --auth-type API_KEY --api-key - < key.txt for an API key
hops datasource create hopsworks <name> --host H --project REMOTE_PROJECT --api-key - < key.txt   # a project on another Hopsworks cluster; port 443; the API key needs the PROJECT and FEATURESTORE scopes there; --no-tls-verify for a self-signed certificate
hops datasource create sharepoint <name> --tenant-id T --client-id C --client-secret - --site-url https://contoso.sharepoint.com/sites/Finance < client_secret.txt   # --cloud US_GOV|CHINA; --site-id for a Sites.Selected app
hops datasource create google-sheets <name> --spreadsheet-id ID --key-path /Projects/<project>/Resources/key.json
hops datasource create rest <name> --base-url https://api.example.com --auth-type API_KEY --api-key - < api_key.txt
hops datasource create crm <name> --crm-type HUBSPOT --api-key - < api_key.txt
hops datasource delete <name> --yes
```

Files a connector points at (`--key-path`, truststores, keystores, wallets) are HopsFS paths and must be absolute: `/Projects/<project>/Resources/key.json`, not `Resources/key.json`. Upload the file first.

`--argument key=value` (and Kafka's or MongoDB's `--option`) carries plain connector settings only. Those values are stored and returned in clear text, so a key that looks like a credential (`sasl.jaas.config`, anything containing `password`, `secret`, `token`) is refused; there is no safe way to pass Kafka SASL credentials through the CLI until the backend keeps them in the secret store.
