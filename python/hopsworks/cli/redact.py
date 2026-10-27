"""Drop credentials from a data source before the CLI prints it.

The SDK serializes a connector with its secrets, because backend writes and jobs
need them. What the CLI prints is read by people and agents, so the secrets are
removed here, at the presentation boundary, and nowhere else.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote


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
    return re.sub(r"[^a-z0-9]", "", unquote(name).lower())


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


# `name=value` in a property list (`a=b;c=d`, `a=b,c=d`, `(a=b)`) whose name is a
# secret one. A value may be braced or quoted and then hold any delimiter.
_PAIR = re.compile(
    r"""(^|[?&;,(])([^=&;,()\s]+)=(\{[^}]*\}?|"[^"]*"?|'[^']*'?|[^&;,()\s]*)"""
)
# The public head of a URL or a JDBC connection string: scheme, host, port and
# database. Everything after it (login, query, properties) is dropped.
_PUBLIC_URL = re.compile(
    r"(?i)((?:[a-z][a-z0-9+.-]*:)+)(@)?(//)?([a-z0-9.-]+|\[[0-9a-f:.]+\])(:\d+)?(/[a-z0-9._-]*)?"
)
HIDDEN = "(hidden by hops: it may hold a credential)"


def _clean_text(text: str) -> str:
    if "://" not in text and "jdbc:" not in text.lower():
        return (
            _PAIR.sub(lambda m: "" if is_secret_name(m.group(2)) else m.group(0), text)
            if "=" in text
            else text
        )
    # A login can hide anywhere in a connection string (`u:p@h`, Oracle's `u/p@h`,
    # an encoded or braced property), so only a plain head is printed. The one `@`
    # kept is Oracle's empty login right after the scheme (`jdbc:oracle:thin:@h`).
    m = _PUBLIC_URL.fullmatch(text) or _PUBLIC_URL.match(text)
    if not m or text.count("@") > (1 if m.group(2) else 0) or re.search(r"\s", text):
        return HIDDEN
    return m.group(0)


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
    a URL or a JDBC connection string, everything after its scheme, host, port and
    database. A connection string with a login in it, or text around a URL, is
    replaced by HIDDEN. Everything else stays.

    Args:
        payload: The dict a connector, a credential binding or a validation result serializes to.

    Returns:
        A new structure of the same shape.
    """
    return _clean(payload)
