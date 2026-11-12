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
"""Embed text with Hopsworks embedding deployments, as the caller.

Two deployments serve the same model: one for agent queries, one for builds
and refreshes. Each is named by configuration, never by a key.

    HOPSABLE_EMBEDDING_PROJECT           the project serving the deployments
    HOPSABLE_EMBEDDING_MODEL             the model name sent in each request, and the one each answer must report
    HOPSABLE_EMBEDDING_DEPLOYMENT        the query deployment, default ``embedder``
    HOPSABLE_EMBEDDING_BUILD_DEPLOYMENT  the build deployment, default ``embedderbuild``
    HOPSABLE_EMBEDDING_QUERY_DEADLINE_S  seconds one query call may take in all, default 30
    HOPSABLE_EMBEDDING_BUILD_DEADLINE_S  seconds one build call may take in all, default 600

The call goes through the Istio ingress the way ``deployment.predict`` does,
with the caller's own credentials: in a job or a deployment that is its
serving API key (``Authorization: ApiKey``); outside a cluster it falls back to
the Hopsworks inference route of the serving project. No other key is read.

The request is OpenAI's embeddings body inside KServe's envelope::

    {"instances": [{"model": "<model>", "input": ["text", ...]}]}
    {"predictions": [{"model": "<model>", "data": [{"embedding": [...]}, ...]}]}

A busy pod answers 429 with ``Retry-After`` at once. That, and a connection that
failed before a byte was sent, are the only retried failures: with the
``Retry-After`` wait plus jitter, or an exponential backoff when that is longer,
inside one deadline that counts the time each attempt took. Every attempt is
one HTTP request: the transport underneath does not retry.

``hops fg embed`` writes groups with :func:`embed_texts`, and the agent ``hops
agent init --search`` writes embeds its queries with :func:`embed_query`.
"""

from __future__ import annotations

import json
import os
import random
import time
from types import SimpleNamespace

import requests
from hopsworks_apigen import public


PROJECT_ENV = "HOPSABLE_EMBEDDING_PROJECT"
DEPLOYMENT_ENV = "HOPSABLE_EMBEDDING_DEPLOYMENT"
BUILD_DEPLOYMENT_ENV = "HOPSABLE_EMBEDDING_BUILD_DEPLOYMENT"
MODEL_ENV = "HOPSABLE_EMBEDDING_MODEL"
QUERY_DEADLINE_ENV = "HOPSABLE_EMBEDDING_QUERY_DEADLINE_S"
BUILD_DEADLINE_ENV = "HOPSABLE_EMBEDDING_BUILD_DEADLINE_S"

DEFAULT_QUERY_DEPLOYMENT = "embedder"
DEFAULT_BUILD_DEPLOYMENT = "embedderbuild"

#: Texts per request, as the deployments cap them: the query one takes 8, the build one 64.
QUERY_BATCH = 8
BUILD_BATCH = 64
#: Seconds one call may take in all, attempts and waits included.
QUERY_DEADLINE_S = 30.0
BUILD_DEADLINE_S = 600.0
#: Seconds to connect for one attempt; the read waits for what is left of the deadline.
CONNECT_TIMEOUT_S = 10.0
#: Backoff between attempts: the first wait, doubling up to the cap, before jitter.
BACKOFF_FIRST_S = 0.5
BACKOFF_MAX_S = 8.0
#: Jitter added to each wait, as a fraction of it.
JITTER = 0.25
#: Wait used when a 429 carries no usable Retry-After.
RETRY_AFTER_DEFAULT_S = 1.0

_routes: dict[tuple[str, str], tuple] = {}


class EmbedError(RuntimeError):
    """An embedding call did not complete; it names the deployment and the last status."""


@public
def endpoint(
    project: str | None = None,
    deployment: str | None = None,
    model: str | None = None,
    *,
    build: bool = False,
) -> tuple[str, str, str]:
    """The (project, deployment, model) to embed with: the arguments, else the environment.

    The deployment is the query one, or the build one with ``build``; each has a default.

    Raises:
        ValueError: The project or the model is neither passed nor set in the environment.
    """
    if build:
        deployment = (
            deployment
            or os.environ.get(BUILD_DEPLOYMENT_ENV)
            or DEFAULT_BUILD_DEPLOYMENT
        )
    else:
        deployment = (
            deployment or os.environ.get(DEPLOYMENT_ENV) or DEFAULT_QUERY_DEPLOYMENT
        )
    project = project or os.environ.get(PROJECT_ENV)
    model = model or os.environ.get(MODEL_ENV)
    missing = [
        name
        for name, value in ((PROJECT_ENV, project), (MODEL_ENV, model))
        if not value
    ]
    if missing:
        raise ValueError(
            "No embedding endpoint is configured: set " + ", ".join(missing) + "."
        )
    return project, deployment, model


@public
def embed_texts(
    texts: list[str],
    *,
    project: str | None = None,
    deployment: str | None = None,
    model: str | None = None,
) -> list[list[float]]:
    """One vector per text, in order, from the build deployment, for builds and refreshes.

    Texts go in requests of ``BUILD_BATCH``, each with its own deadline
    (``BUILD_DEADLINE_S``). An answer with another count of vectors, or from
    another model, raises, so a vector is never paired with the wrong text.

    Raises:
        ValueError: No endpoint is configured, or the answer is not one vector per text of the model.
        EmbedError: The deployment stayed busy past the deadline, or refused the call.
    """
    return _embed(
        texts,
        endpoint(project, deployment, model, build=True),
        BUILD_BATCH,
        _deadline(BUILD_DEADLINE_ENV, BUILD_DEADLINE_S),
    )


@public
def embed_query(
    text: str,
    *,
    project: str | None = None,
    deployment: str | None = None,
    model: str | None = None,
) -> list[float]:
    """The vector of one query text from the query deployment, within ``QUERY_DEADLINE_S``.

    Raises:
        ValueError: No endpoint is configured, or the answer is not one vector of the model.
        EmbedError: The deployment stayed busy past the deadline, or refused the call.
    """
    return _embed(
        [text],
        endpoint(project, deployment, model),
        QUERY_BATCH,
        _deadline(QUERY_DEADLINE_ENV, QUERY_DEADLINE_S),
    )[0]


def _deadline(env: str, default: float) -> float:
    raw = os.environ.get(env)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{env} must be a number of seconds, got {raw!r}.") from None
    if value <= 0:
        raise ValueError(f"{env} must be positive, got {raw!r}.")
    return value


def _embed(
    texts: list[str], target: tuple[str, str, str], batch: int, deadline_s: float
) -> list[list[float]]:
    project, deployment, model = target
    vectors: list[list[float]] = []
    for start in range(0, len(texts), batch):
        chunk = list(texts[start : start + batch])
        answer = _predict(
            project,
            deployment,
            {"instances": [{"model": model, "input": chunk}]},
            deadline_s,
        )
        vectors.extend(_vectors(answer, len(chunk), model))
    return vectors


def _vectors(answer: dict, count: int, model: str) -> list[list[float]]:
    try:
        prediction = answer["predictions"][0]
        data = prediction["data"]
    except (KeyError, IndexError, TypeError):
        raise ValueError(
            "The embedding deployment answered without predictions[0].data."
        ) from None
    if prediction.get("model") != model:
        raise ValueError(
            f"The embedding deployment reports model {prediction.get('model')!r}, not {model!r}."
        )
    if len(data) != count:
        raise ValueError(
            f"The embedding deployment returned {len(data)} vectors for {count} texts."
        )
    if all("index" in item for item in data):
        data = sorted(data, key=lambda item: item["index"])
    return [list(item["embedding"]) for item in data]


def _predict(project: str, deployment: str, body: dict, deadline_s: float) -> dict:
    """POST ``body``; retried only on a 429 or a connection that never sent, inside ``deadline_s`` in all."""
    from hopsworks_common.client.base import _never_sent

    payload = json.dumps(body)
    start = time.monotonic()
    deadline = start + deadline_s
    attempt = 0
    last = "no attempt"
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            raise EmbedError(
                f"Embedding deployment {deployment!r} gave no answer in {deadline_s:g} s: last {last}."
            )
        wait = 0.0
        try:
            return _send(project, deployment, payload, left, deadline)
        except requests.HTTPError as exc:
            response = exc.response
            status = getattr(response, "status_code", None)
            if status != 429:
                raise EmbedError(
                    f"Embedding deployment {deployment!r} answered HTTP {status}: not retried."
                ) from exc
            last = "HTTP 429"
            wait = _retry_after(response)
        except requests.exceptions.ConnectionError as exc:
            if not _never_sent(exc):
                raise EmbedError(
                    f"Embedding deployment {deployment!r} dropped the connection after the request went out: not retried."
                ) from exc
            last = f"connection error ({type(exc).__name__})"
        except requests.exceptions.ReadTimeout as exc:
            raise EmbedError(
                f"Embedding deployment {deployment!r} did not answer within the deadline of {deadline_s:g} s."
            ) from exc
        pause = max(wait, min(BACKOFF_MAX_S, BACKOFF_FIRST_S * 2**attempt))
        pause += random.uniform(0, JITTER * pause)
        attempt += 1
        if time.monotonic() + pause >= deadline:
            raise EmbedError(
                f"Embedding deployment {deployment!r} stayed unavailable for {deadline_s:g} s: last {last}."
            )
        time.sleep(pause)


def _retry_after(response) -> float:
    try:
        return max(0.0, float(response.headers.get("Retry-After")))
    except (TypeError, ValueError, AttributeError):
        return RETRY_AFTER_DEFAULT_S


def _send(
    project: str, deployment: str, payload: str, left: float, deadline: float
) -> dict:
    """One HTTP request.

    ``timeout`` makes the client send once (``_send_until_available`` skips its
    unavailable-backend loop); ``deadline`` makes an error answer raise
    ``requests.HTTPError`` unread and skips the stale-session repeat. The
    session is a plain ``requests`` one, whose adapters retry nothing.
    """
    from hopsworks_common import client

    namespace, name, project_id = _route(project, deployment)
    istio = client.istio._get_instance()
    if istio is not None:
        from hsml.core.serving_api import ServingApi

        path = ServingApi()._get_istio_inference_path(
            SimpleNamespace(project_namespace=namespace, name=name)
        )
        sender, with_base = istio, False
    else:
        path = ["project", project_id, "inference", "models", name + ":predict"]
        sender, with_base = client._get_instance(), True
    return sender._send_request(
        "POST",
        path,
        headers={"content-type": "application/json"},
        data=payload,
        with_base_path_params=with_base,
        timeout=(min(CONNECT_TIMEOUT_S, left), left),
        retry_while_unavailable=False,
        deadline=deadline,
    )


def _route(project: str, deployment: str) -> tuple[str, str, int]:
    """The path segment the ingress routes the serving project by, the deployment and the project id, read once per process.

    The segment is the project's backing name, read from the project; the
    deployment is named as configured, never looked up in the caller's project.
    """
    key = (project, deployment)
    if key not in _routes:
        from hopsworks_common import client
        from hopsworks_common.project import Project

        info = client._get_instance()._send_request(
            "GET", ["project", "getProjectInfo", project]
        )
        serving = Project.from_response_json(info)
        _routes[key] = (serving.backing_name, deployment, serving.id)
    return _routes[key]
