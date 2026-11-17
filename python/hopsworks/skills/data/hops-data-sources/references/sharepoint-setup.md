# Setting up a SharePoint data source

Hopsworks reads SharePoint Online as a Microsoft Entra ID application with the client credentials flow.
Nobody signs in; the application's own permissions decide what it can read.
Everything on this page except the last step happens in Microsoft 365, and needs a tenant administrator.

## 1. Check the tenant has SharePoint Online

The tenant needs a SharePoint Online license (Microsoft 365 Business, E3/E5 or a developer sandbox).
Without one, Microsoft Graph answers every site request with `Tenant does not have a SPO license`, and the data source test fails although the credentials are valid.
`https://<tenant>.sharepoint.com` opening in a browser is a quick check.

## 2. Register the application

In the [Microsoft Entra admin center](https://entra.microsoft.com):

1. `Applications` → `App registrations` → `New registration`.
2. Supported account types: `Accounts in this organizational directory only`.
3. Redirect URI: none.
4. Note the `Application (client) ID` and the `Directory (tenant) ID` on the overview page.

## 3. Grant a Graph application permission

`API permissions` → `Add a permission` → `Microsoft Graph` → **Application permissions** (not Delegated), then `Grant admin consent for <tenant>`.
The status column must show "Granted"; without consent the token is issued but every Graph call returns 403.

Pick one:

| Permission | Reads | Extra step |
|---|---|---|
| `Sites.Selected` | only the sites granted to the application | grant the site (below) |
| `Sites.Read.All` | every site in the tenant | none |

The data source only reads.
Write permissions (`Sites.ReadWrite.All`) are needed only by tools that change the library, such as the loadtest that edits files to test the mirror.

Granting a site to a `Sites.Selected` application is done through Graph by an administrator application that holds `Sites.FullControl.All`:

```bash
# Site id from the URL
curl -H "Authorization: Bearer $ADMIN_TOKEN" \
  "https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/Finance?\$select=id"

# Grant read on that site to the Hopsworks application
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
  "https://graph.microsoft.com/v1.0/sites/<site-id>/permissions" \
  -d '{"roles": ["read"], "grantedToIdentities": [{"application": {"id": "<client-id>", "displayName": "hopsworks"}}]}'
```

If the `Sites.Selected` application cannot look the site up by its URL, pass the site id to the data source (`--site-id`, or `site_id=` in the SDK).

## 4. Create a client secret

`Certificates & secrets` → `Client secrets` → `New client secret`.
Copy the **Value** column immediately; it is shown once.
The `Secret ID` next to it is not the secret, and using it fails with `AADSTS7000215: Invalid client secret provided`.
Secrets expire; rotate by setting a new one on the data source before the old one does (client id and secret can change while syncs exist, tenant, cloud and site cannot).

## 5. Create a site and note its URL

Any team or communication site works, for example `https://contoso.sharepoint.com/sites/Finance`.
Its document libraries ("Documents" by default) are what syncs copy from.

## 6. Network access from the cluster

The Hopsworks backend and the sync jobs connect over HTTPS to Entra ID, Microsoft Graph and the tenant's SharePoint hosts.
For the global cloud: `login.microsoftonline.com`, `graph.microsoft.com`, `<tenant>.sharepoint.com`.
The `US_GOV` and `CHINA` clouds use their own hosts; set `cloud` accordingly.

## 7. Create the data source in Hopsworks

The cluster variable `enable_sharepoint_storage_connectors` must be `true` (the default).
Creating the data source and syncs needs the Data Owner role in the project.

```bash
# test only: acquires a token and lists the site's libraries, creates nothing, exits 1 on failure
hops datasource create sharepoint finance_sharepoint --tenant-id <tenant-id> --client-id <client-id> \
  --client-secret - --site-url https://contoso.sharepoint.com/sites/Finance --test < client_secret.txt

# create it, then list the libraries to get a drive id for a sync
hops datasource create sharepoint finance_sharepoint --tenant-id <tenant-id> --client-id <client-id> \
  --client-secret - --site-url https://contoso.sharepoint.com/sites/Finance < client_secret.txt
hops datasource sharepoint drives finance_sharepoint
```

The secret can also come from the `HOPSWORKS_DS_SHAREPOINT_CLIENT_SECRET` environment variable or `account:<NAME>` (your account variable), so it never appears on the command line.
In the UI: `Data Sources` → `New data source` → `SharePoint`, then `Test connection` and `Next: Create sync`.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `AADSTS7000215: Invalid client secret` | the Secret ID was used, or the secret belongs to another app | paste the secret **Value** of this application |
| `AADSTS700016` application not found | wrong client id or tenant id | copy both from the app's overview page |
| 270346 `SHAREPOINT_AUTH_FAILED` | Entra ID rejected the credentials, or the secret expired | check ids and secret; create a new secret |
| 270347 `SHAREPOINT_SITE_NOT_FOUND` | wrong URL, no SharePoint license, missing admin consent, or a `Sites.Selected` app without a grant for this site | check the URL, the license, consent, and the site grant; or pass the site id |
| 270348 `SHAREPOINT_DRIVE_NOT_FOUND` | the library id is not a library of the data source's site | use an id from `hops datasource sharepoint drives` |
| 270349 `SHAREPOINT_FOLDER_NOT_FOUND` | the folder path does not exist, or the synced folder was deleted in SharePoint | pick an existing folder; re-point the sync with `set_folder` |
| sync run fails with `SHAREPOINT_SYNC_NOT_DATA_OWNER` | the run's user (the job's creator for scheduled runs) is no longer a data owner | a data owner adopts it with `sync.update(take_ownership=True)` |
| sync run fails with "name(s) have characters HopsFS cannot store" | HopsFS stores names in Latin-1; `ł`, `€`, CJK and similar cannot be stored (accents in decomposed form are converted and work) | rename those files in SharePoint; the next run copies them |
| sync run fails: rclone not found | the sync job's environment was switched to one without rclone | use `python-feature-pipeline` or an environment built on it |
