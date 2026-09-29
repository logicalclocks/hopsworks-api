"""The RAG agent reference: document chunks, the agent's steps, and its app."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from hopsworks.cli import scaffold


RAG = (
    Path(scaffold.__file__).resolve().parents[1]
    / "skills"
    / "ml"
    / "hops-reqs"
    / "references"
    / "rag_agent"
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # A dataclass looks its module up in sys.modules.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ingest = _load(RAG / "ingest_docs.py", "rag_ingest_docs")
agent = _load(RAG / "agent.py", "rag_agent_module")


# region Chunks


def test_a_text_document_is_cut_into_pages_and_paragraphs(tmp_path):
    doc = tmp_path / "faq.txt"
    doc.write_text("Returns take 30 days.\n\nRefunds take 5 days.\f\nPage two.\n")
    chunks = ingest.chunk_document(doc)
    assert [(c.page, c.offset) for c in chunks] == [(1, 0), (2, 0)]
    assert chunks[0].text == "Returns take 30 days.\n\nRefunds take 5 days."
    assert chunks[0].doc_name == "faq.txt"


def test_short_paragraphs_join_and_long_ones_split_at_sentences():
    short = [f"Paragraph {i} is short." for i in range(40)]
    long = " ".join(f"Sentence {i} of a long paragraph." for i in range(80))
    chunks = ingest.chunk_pages("doc.md", [short, [long]])
    first_page = [c for c in chunks if c.page == 1]
    assert all(len(c.text) >= ingest.MIN_CHARS for c in first_page[:-1])
    # Each chunk starts at the paragraph it cites.
    assert first_page[0].offset == 0 and first_page[1].offset > 0
    assert first_page[1].text.startswith(f"Paragraph {first_page[1].offset} ")
    second_page = [c for c in chunks if c.page == 2]
    assert len(second_page) > 1
    assert all(len(c.text) <= ingest.MAX_CHARS for c in second_page)
    assert all(c.offset == 0 for c in second_page)


def test_chunk_ids_are_stable_and_distinct():
    a, b = ingest.chunk_pages("x.md", [["one " * 120, "two " * 120]])
    assert a.chunk_id == ingest.Chunk("x.md", a.page, a.offset, "other").chunk_id
    assert a.chunk_id != b.chunk_id


def test_the_sample_documents_are_read(tmp_path):
    samples = sorted((RAG / "sample_docs").iterdir())
    assert {p.suffix for p in samples} <= set(ingest.SUFFIXES)
    for path in samples:
        chunks = ingest.chunk_document(path)
        assert chunks and all(c.page == 1 for c in chunks), path.name


def test_an_unsupported_file_is_refused(tmp_path):
    path = tmp_path / "notes.rtf"
    path.write_text("x")
    with pytest.raises(ValueError, match="only"):
        ingest.read_pages(path)


# endregion

# region The agent's steps


class _Fg:
    def __init__(self, rows=None, hits=None):
        self.rows, self.hits = rows, hits

    def filter(self, _condition):
        return self

    def read(self, online, dataframe_type):
        assert online and dataframe_type == "pandas"
        return self.rows

    def find_neighbors(self, vector, k):
        self.asked = (vector, k)
        return self.hits[:k]


class _Column:
    def __eq__(self, other):
        return ("user_id", other)


def _predict(llm=None):
    events = pd.DataFrame(
        {
            "user_id": [7, 7],
            "event_id": [1, 2],
            "event_type": ["bought", "returned"],
            "product_name": ["headphones", "headphones"],
            "amount": [59.0, 59.0],
            "event_time": pd.to_datetime(["2026-09-01", "2026-09-10"]),
        }
    )
    columns = [
        "chunk_id",
        "doc_name",
        "path",
        "url",
        "page",
        "offset",
        "text",
        "embedding",
    ]
    hits = [
        (
            0.9,
            [
                "c1",
                "returns.md",
                "/Projects/p/r/returns.md",
                "/p/1/f",
                1,
                2,
                "Refunds take 5 days.",
                [0.1],
            ],
        ),
        (
            0.5,
            [
                "c2",
                "delivery.md",
                "/Projects/p/r/delivery.md",
                "/p/1/f",
                1,
                0,
                "Standard delivery.",
                [0.2],
            ],
        ),
    ]
    predict = agent.Agent.__new__(agent.Agent)
    predict.events = _Fg(rows=events)
    predict.events.user_id = _Column()
    predict.chunks = _Fg(hits=hits)
    predict.columns = columns
    predict.encoder = SimpleNamespace(
        encode=lambda text, normalize_embeddings: [0.3, 0.4]
    )
    predict.llm = llm
    return predict


def _run(predict, **request):
    state = {"trace": [], **request}
    for step in (predict.lookup_events, predict.retrieve, predict.answer):
        state.update(step(state))
    return state


def test_the_agent_reads_events_then_the_nearest_chunks_then_answers():
    captured = {}

    class Llm:
        def invoke(self, messages):
            captured["messages"] = messages
            return SimpleNamespace(
                content="Your refund takes 5 days [returns.md p.1 ¶2]."
            )

    predict = _predict(Llm())
    state = _run(predict, user_id=7, query="Where is my refund?", k=1)
    assert predict.chunks.asked == ([0.3, 0.4], 1)
    assert [e["event_type"] for e in state["events"]] == ["returned", "bought"]
    assert state["sources"] == [
        {
            "chunk_id": "c1",
            "doc_name": "returns.md",
            "path": "/Projects/p/r/returns.md",
            "url": "/p/1/f",
            "page": 1,
            "offset": 2,
            "text": "Refunds take 5 days.",
            "score": 0.9,
        }
    ]
    system, user = captured["messages"]
    assert system[0] == "system" and "Cite every document" in system[1]
    assert "returned headphones" in user[1] and "[returns.md p.1 ¶2]" in user[1]
    assert user[1].endswith("Question: Where is my refund?")
    assert [t["step"] for t in state["trace"]] == ["events", "retrieve", "answer"]


def test_without_an_llm_the_agent_says_so_and_still_returns_the_context():
    state = _run(_predict(), user_id=7, query="Where is my refund?", k=25)
    assert "LLM_URL and LLM_API_KEY" in state["answer"]
    assert len(state["sources"]) == 2 and len(state["events"]) == 2


def test_a_small_corpus_answers_at_the_default_k():
    predict = _predict()
    calls = []

    def find_neighbors(vector, k):
        calls.append(k)
        if k > 2:  # what a client before FSTORE-1970 raises with fewer hits than k
            raise RuntimeError("[knn] requires k to be in the range (0, 10000]")
        return predict.chunks.hits[:k]

    predict.chunks.find_neighbors = find_neighbors
    state = _run(predict, user_id=7, query="refund?", k=25)
    assert calls == [25, 12, 6, 3, 1]
    assert len(state["sources"]) == 1


def test_a_request_needs_a_user_and_a_question():
    predict = _predict()
    assert predict.predict({"user_id": 7, "query": " "}) == {
        "error": "send user_id and query"
    }
    ran = {}
    predict.graph = SimpleNamespace(
        invoke=lambda state: (
            ran.setdefault("state", state)
            | {"answer": "a", "sources": [], "events": []}
        )
    )
    reply = predict.predict({"instances": [{"user_id": 7, "query": "hi"}]})
    assert ran["state"]["k"] == agent.DEFAULT_K and reply["answer"] == "a"


def test_the_agent_serves_the_predict_route_and_query(monkeypatch):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    served = SimpleNamespace(predict=lambda body: {"echo": body})
    client = TestClient(agent.build_app(served))
    assert client.get("/").json() == {"status": "ok"}
    body = {"user_id": 7, "query": "hi"}
    assert client.post("/predict", json=body).json() == {"echo": body}
    assert client.post("/query", json=body).json() == {"echo": body}
    for path in (
        "/v1/models/helpdeskagent:predict",
        "/v1/models/helpdeskagent%3Apredict",
    ):
        assert client.post(path, json=body).json() == {"echo": body}, path
    assert client.post("/v1/models/helpdeskagent", json=body).status_code == 404


# endregion

# region The app


def test_the_app_lists_users_and_forwards_a_question_to_the_agent(monkeypatch):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    app = _load(RAG / "app" / "app.py", "rag_app_under_test")
    sent = {}

    class Deployment:
        def predict(self, data):
            sent["data"] = data
            return {"predictions": {"answer": "ok", "sources": [], "events": []}}

    fg = SimpleNamespace(
        select=lambda cols: SimpleNamespace(
            read=lambda dataframe_type: pd.DataFrame({"user_id": [3, 1, 3]})
        )
    )
    project = SimpleNamespace(
        get_feature_store=lambda: SimpleNamespace(
            get_feature_group=lambda n, version: fg
        ),
        get_model_serving=lambda: SimpleNamespace(
            get_deployment=lambda n: Deployment()
        ),
    )
    monkeypatch.setattr(app, "_project", lambda: project)
    client = TestClient(app.app)
    assert client.get("/api/users").json() == [1, 3]
    reply = client.post("/api/ask", json={"user_id": 3, "query": "refund?"})
    assert reply.json()["answer"] == "ok"
    assert sent["data"] == {"instances": [{"user_id": 3, "query": "refund?", "k": 25}]}
    assert client.post("/api/ask", json={"user_id": 3, "query": ""}).status_code == 422
    page = client.get("/")
    assert page.status_code == 200 and "static/app.js" in page.text


def test_the_app_uses_only_relative_urls():
    script = (RAG / "app" / "static" / "app.js").read_text(encoding="utf-8")
    page = (RAG / "app" / "static" / "index.html").read_text(encoding="utf-8")
    for url in re.findall(r"request\(\s*[`\"']([^`\"']+)", script):
        assert not url.startswith(("/", "http")), url
    for url in re.findall(r'(?:src|href)="([^"]+)"', page):
        assert not url.startswith(("/", "http")), url
    assert "cdn" not in page.lower() and "https://" not in page


# endregion
