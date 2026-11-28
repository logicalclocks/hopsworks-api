"""The gateway's signature over the caller: a principal header alone is not proof."""

import base64
import hashlib
import hmac
import time

import pytest
from hopsworks_agents.protocol import scope


#: The vector the authenticator's and the backend's tests use.
MASTER = b"0123456789abcdef0123456789abcdef"
DEPLOYMENT_KEY = "e9AV4-UqqYsmJmjUeMJJMudEM7tbNW9L7GpmP5cAt1E"


def sign(principal, kind, at, key=DEPLOYMENT_KEY):
    raw = base64.urlsafe_b64decode(key + "=" * (-len(key) % 4))
    digest = hmac.new(raw, f"{principal}\n{kind}\n{at}".encode(), hashlib.sha256)
    return base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode()


def headers(principal="user:alice", kind="apikey", at=None, signature=None):
    at = int(time.time()) if at is None else at
    return {
        scope.PRINCIPAL_HEADER: principal,
        scope.AUTH_KIND_HEADER: kind,
        scope.TIME_HEADER: str(at),
        scope.SIGNATURE_HEADER: sign(principal, kind, at)
        if signature is None
        else signature,
    }


@pytest.fixture
def keyed(monkeypatch):
    monkeypatch.setenv(scope.PRINCIPAL_KEY_ENV, DEPLOYMENT_KEY)


def test_the_deployment_key_is_derived_as_the_authenticator_derives_it():
    derived = hmac.new(MASTER, b"agent-principal:proj1/agent", hashlib.sha256)
    assert base64.urlsafe_b64encode(derived.digest()).rstrip(b"=").decode() == (
        DEPLOYMENT_KEY
    )
    # and the authenticator's signature of the shared vector verifies
    assert sign("user:alice", "apikey", 1700000000) == (
        "J_-SOLEN21_GC1z-GF7dFkxjMx5eTHqsLhSMWRHPLLY"
    )


def test_a_signed_principal_is_the_caller(keyed):
    assert scope._header_principal(headers()) == "user:alice"


@pytest.mark.parametrize(
    "forged",
    [
        # no signature at all: a workload that reached the pod around the gateway
        {scope.PRINCIPAL_HEADER: "user:alice"},
        # another caller's signature, replayed for a different principal
        {**headers(principal="user:bob"), scope.PRINCIPAL_HEADER: "user:alice"},
        # the auth kind changed after signing
        {**headers(), scope.AUTH_KIND_HEADER: "jwt"},
        # signed with another deployment's key
        headers(signature=sign("user:alice", "apikey", int(time.time()), "A" * 43)),
        # too old, or from the future
        headers(at=int(time.time()) - scope.SIGNATURE_MAX_AGE_S - 5),
        headers(at=int(time.time()) + scope.SIGNATURE_MAX_AGE_S + 5),
        # garbage in the time
        {**headers(), scope.TIME_HEADER: "soon"},
    ],
)
def test_an_unsigned_or_forged_principal_is_no_caller(keyed, forged):
    assert scope._header_principal(forged) is None


def test_on_a_deployment_a_forged_principal_is_refused(keyed, monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "42")
    with pytest.raises(scope.PrincipalRequired):
        scope._principal_of({scope.PRINCIPAL_HEADER: "user:alice"})
    assert scope._principal_of(headers()) == "user:alice"


def test_without_the_key_the_header_is_taken_as_it_comes(monkeypatch):
    # an older platform, or a local run: nothing to verify with
    monkeypatch.delenv(scope.PRINCIPAL_KEY_ENV, raising=False)
    assert scope._header_principal({scope.PRINCIPAL_HEADER: "user:alice"}) == (
        "user:alice"
    )
