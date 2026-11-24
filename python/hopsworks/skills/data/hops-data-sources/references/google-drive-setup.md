# Setting up a Google Drive data source

Hopsworks reads a Drive folder as a Google Cloud service account, through the Drive API with the read-only scope.
Nobody signs in; the service account is its own Google identity, and it sees only what is shared with it.
Everything except the last step happens in the Google Cloud console and in Google Drive.

## 1. Enable the Drive API

In the [Google Cloud console](https://console.cloud.google.com/), pick or create a project and enable the [Google Drive API](https://console.cloud.google.com/apis/library/drive.googleapis.com) in it.
Without it every request of the project's service accounts is refused with 403, whatever the folder is shared with.
Enabling takes a few minutes to propagate.

## 2. Create a service account and a JSON key

`IAM & Admin` → `Service Accounts` → `Create service account`.
It needs no role in the project; its access to the files comes from sharing the folder with it.
Open it, `Keys` → `Add key` → `Create new key` → `JSON`, and download the file; it is the only copy.
The `client_email` field of the file, `<name>@<project>.iam.gserviceaccount.com`, is the address to share with.

## 3. Share the folder with the service account

In Google Drive, right-click the folder → `Share`, enter the service account's email, pick `Viewer`, untick `Notify people` (it has no inbox) and confirm the "outside your organisation" warning if one appears.
A folder in a user's My Drive is invisible to the service account until this is done, even though the key was created in the same Google Cloud project.
A Workspace administrator may block sharing outside the organisation; then put the folder on a shared drive and add the service account as a member of the shared drive.

## 4. Copy the folder ID

The folder ID is the last part of the folder's URL, `https://drive.google.com/drive/folders/<folder ID>`: letters, digits, `-` and `_`.
The root of a shared drive has an ID of the same form.
Folder names are not used to find the folder.

## 5. Create the data source

```bash
hops datasource create google-drive <name> --key-file ./drive-sa.json --folder-id <folder ID> [--folder-name N]
hops datasource info <name>          # shows the service account's address
```

`--test` and the UI's Test connection only check that the key file is a service account key with `client_email` and `private_key`; sharing and the Drive API are checked by the first run of a files ingestion job.

## Troubleshooting a run

A run that cannot reach the folder fails before listing anything, with `root path '<folder ID>' not found or not readable:` followed by Google's answer on the execution (`hops job history <job>`, the job page, the execution logs).

| Google's answer | Cause | Fix |
| --- | --- | --- |
| `HttpError 403 ... Google Drive API has not been used in project <number> before or it is disabled` | Drive API not enabled in the service account's project. | Step 1, wait a few minutes, re-run. |
| `HttpError 404 ... File not found: <folder ID>` | Folder not shared with the service account, or the ID is not a folder's. Drive answers `not found` for anything the caller cannot see. | Step 3; check the ID against the URL (step 4). |
| `HttpError 403 ... insufficientFilePermissions` or `cannotDownloadFile` on one file | That file is not visible to the service account, or its owner disabled downloads for viewers. | Share the file or re-enable `Viewers and commenters can download`; the file counts as failed, the run goes on. |
| `RefreshError: invalid_grant: Invalid JWT Signature` | The key in the uploaded JSON was deleted or rotated in the console, or the service account was disabled. | Create a new JSON key (step 2), upload it and pick it as the data source's key file; the next run uses it. |
| `RefreshError: invalid_grant: Invalid grant: account not found` | The service account was deleted. | Steps 2 and 3 again with a new account, then update the data source. |

`file_cache is only supported with oauth2client<4.0.0` at the top of the logs is a notice of the Google client library, not an error.
