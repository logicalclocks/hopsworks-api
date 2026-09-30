---
name: hops-collaboration
description: Use when managing project membership and when sharing in Hopsworks. Share feature store / feature group / dataset data across projects. 
---

# Project Members, Platform Users, and Sharing

Manage collaboration and access control in Hopsworks: who is a member of a project and at what role, who can log in to the platform at all, and how data (feature stores, feature groups, individual features, generic datasets) gets exposed to other projects or to individually-restricted members.

## Contract
- **Input:** a project handle (`project = hopsworks.login()`) for project/sharing operations; a `HOPS_ADMIN` session for platform user administration.
- **Output:** an updated membership list, an updated platform user, or an updated share/grant.
- **Pre-condition:** for project-role changes and sharing, the caller needs the `Data owner` role in the relevant project. For platform user administration, the caller's account needs the `HOPS_ADMIN` platform role.

## Two separate role systems — don't confuse them

- **Project role** (`ProjectTeam.teamRole`): per-project, one of `Data owner`, `Data scientist`, `Observer`, `Feature store restricted`. Governs what a member can do *inside one project*.
- **Platform role** (`Users.bbcGroupCollection`): cluster-wide, one of `HOPS_ADMIN`, `HOPS_USER`, `HOPS_SERVICE_USER`. Governs whether an account can log in / administer the cluster at all, independent of any project.

A brand-new platform user has no project role until someone adds them to a project.

Role names are matched case-insensitively, so `data owner` and `Data owner` (or `hops_admin` and `HOPS_ADMIN`) are both accepted; the canonical casing above is what reaches the backend.

## Managing project members

```python
project = hopsworks.login()

# Add / list / remove
project.add_member("alice@example.com", "Data scientist")
members = project.get_members()  # list[ProjectMember]
project.remove_member("alice@example.com")

# Change role — two equivalent ways
project.get_members_api().update_role("alice@example.com", "Observer")
member = next(m for m in members if m.email == "alice@example.com")
member.update_role("Observer")
```

Key facts:
- Adding a member with `Feature store restricted` role gives them **zero** feature store access by default — see "Restricted per-user access" below, it must be granted explicitly per feature group.
- The project owner's role can't be changed or removed; a `Data scientist` removing a member can only remove themselves (the backend enforces this — expect `RestAPIError`, not a client-side check).
- `remove_member(email, delete_home_dir=True)` permanently deletes that member's home directory files in the project — irreversible.

## Administering platform users (admin-only)

Distinct from project membership — this manages accounts on the cluster itself, and requires `HOPS_ADMIN`.

```python
users_api = hopsworks.get_users_api()

new_user = users_api.register_user(
    email="alice@example.com", first_name="Alice", last_name="Smith", role="HOPS_USER"
)
if new_user.password:
    # Only set when no password was supplied; hand it off securely, never log or print it.
    temporary_password = new_user.password

users_api.activate_user(new_user.id)      # or reject_user / resend_confirmation_email
users_api.set_role(new_user.id, "HOPS_ADMIN")
users_api.update_user(new_user.id, max_num_projects=10)
users_api.delete_user(new_user.id)        # fails if the user still owns projects
```

`get_users()` / `get_user(id)` / `get_user_by_email(email)` return `AdminUser` objects (`id`, `email`, `roles`, `status`, ...) — distinct from the lightweight `User` class used elsewhere (e.g. as a feature group's `creator`). Look a user up by email when you don't already have their id; it scans the full user list, so pass the id when you have it.

## Sharing feature store data across projects

Three granularities, all requiring `Data owner` in the *source* project:

```python
fs = project.get_feature_store()

fs.share("target_project")                       # whole feature store, read-only
fg.share("target_project")                       # one feature group, whole
fg.share("target_project", features=["amount"])   # one feature group, selected columns only

fg.shared_with()   # or fs.shared_with() — list current shares
fg.unshare("target_project")
```

Primary keys and the event-time column are always included even when `features=` is a subset — the target project needs them to read the data at all.

## Restricted per-user access (within the same project)

For a member with the `Feature store restricted` role: they see **no** feature groups by default, even inside their own project. Access must be granted individually, per feature group (or per feature):

```python
fg.grant_restricted_access("restricted_user@example.com")                      # whole fg
fg.grant_restricted_access("restricted_user@example.com", features=["amount"]) # columns only
fg.get_restricted_access()      # list current grants
fg.revoke_restricted_access("restricted_user@example.com")
```

The target user must already hold `Feature store restricted` in the project (add them via `project.add_member(email, "Feature store restricted")` first) — the backend rejects the grant otherwise.

## Sharing generic datasets

For non-feature-store datasets (`Resources/`, `Models/`, `Jupyter/`, ...), unrelated to the feature store sharing above:

```python
dataset_api = project.get_dataset_api()

dataset_api.share("Resources/my_dir", target_project="other_project")                          # read-only by default
dataset_api.share("Resources/my_dir", target_project="other_project", permission="EDITABLE")
dataset_api.unshare("Resources/my_dir", target_project="other_project")
```

Feature-store-backed datasets can only be shared as `READ_ONLY` through this API — use feature store / feature group sharing above for anything richer.
