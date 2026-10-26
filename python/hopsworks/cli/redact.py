"""Drop credentials from a data source before the CLI prints it.

The SDK serializes a connector with its secrets, because backend writes and jobs
need them. What the CLI prints is read by people and agents, so the secrets are
removed here, at the presentation boundary, and nowhere else.
"""

from __future__ import annotations

import re
from typing import Any


# A key or option name that holds a secret contains one of these once lowercased
# and stripped of everything but letters and digits (`x-api-key` is `xapikey`).
_SECRET_FRAGMENTS = (
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "privatekey",
    "apikey",
    "accesskey",
    "encryptionkey",
    "credential",
    "jaas",
    "authorization",
    "cookie",
    "signature",
    "bearer",
)
_SECRET_NAMES = frozenset({"key", "pwd", "pw", "sig", "sas"})
# Names that carry a fragment but hold a setting or the name of an account entry.
_KEPT_SUFFIXES = ("url", "minutes", "secretname", "envvar", "credentialsmode")
_DROP = object()


def _normal(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def is_secret_name(name: Any) -> bool:
    """Whether a key or an option name holds a secret.

    Args:
        name: Key of a mapping, or the name of a name/value option.

    Returns:
        True when its value must not be printed.
    """
    if not isinstance(name, str):
        return False
    flat = _normal(name)
    if flat.endswith(_KEPT_SUFFIXES):
        return False
    return flat in _SECRET_NAMES or any(f in flat for f in _SECRET_FRAGMENTS)


# `name=value` inside a URL query, a JDBC property list (`;a=b`, `(a=b)`) or a
# MySQL style `,a=b`, where the name is a secret one.
_PAIR = re.compile(r"([?&;,(])([^=&;,()\s]+)=([^&;,()\s]*)")


def _clean_text(text: str) -> str:
    if "://" not in text and not text.lower().startswith("jdbc:"):
        return text
    # Userinfo of every URL inside the string, e.g. `jdbc:postgresql://u:p@h/db`.
    text = re.sub(r"(://)[^/@\s?#]*@", r"\1", text)
    text = _PAIR.sub(lambda m: "" if is_secret_name(m.group(2)) else m.group(0), text)
    return re.sub(r"\?(?=$|[&#])", "", text.replace("?&", "?"))


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        if "name" in value and "value" in value and is_secret_name(value.get("name")):
            return _DROP
        return {
            k: cleaned
            for k, v in value.items()
            if not is_secret_name(k) and (cleaned := _clean(v)) is not _DROP
        }
    if isinstance(value, list):
        return [c for v in value if (c := _clean(v)) is not _DROP]
    if isinstance(value, str):
        return _clean_text(value)
    return value


def redact(payload: Any) -> Any:
    """Return a copy of a serialized connector, binding or result without its credentials.

    Drops every secret key, every name/value option whose name is a secret one
    (SQL `arguments`, Snowflake `sfOptions`, headers) and, in any string that is
    a URL or a JDBC connection string, its userinfo and its secret properties.
    Everything else stays.

    Args:
        payload: The dict a connector, a credential binding or a validation result serializes to.

    Returns:
        A new structure of the same shape.
    """
    return _clean(payload)
