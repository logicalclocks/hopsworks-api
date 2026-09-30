# ruff: noqa: INP001
"""The help desk agent: a LangGraph workflow served as a Hopsworks agent deployment.

An agent deployment runs this file, which serves HTTP on port 8080:
`POST /predict` (where the Hopsworks inference endpoint forwards a request),
`POST /v1/models/<name>:predict` (KServe's path, which `deployment.predict`
uses inside the cluster, with the colon percent-encoded) and `POST /query`, with
`GET /` for the readiness probe. The graph is built before the server starts, so the deployment turns
ready only once it can answer.

Request: `{"user_id": 17, "query": "Where is my refund?", "k": 25}` (`k` optional).
Response: `{"answer", "sources": [{doc_name, url, path, page, offset, score, text}],
"events": [...], "trace": [...]}`.

Three steps, always in this order: `events` reads the user's recent events from
the online `user_events` feature group, `retrieve` embeds the query with the
registered sentence-transformers model and takes the k nearest chunks from the
embedding feature group's vector index, and `answer` puts both into the context
window of the LLM at LLM_URL (an OpenAI-compatible endpoint; model LLM_MODEL),
with LLM_API_KEY. The three come from the user's account environment variables,
which Hopsworks sets in the deployment. Without them the answer says so and the
retrieved context is still returned.

Every step's inputs and outputs are logged and returned as `trace`.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, TypedDict

_logger = logging.getLogger("helpdesk_agent")

CHUNKS = {"name": "helpdesk_doc_chunks", "version": 1}
EVENTS = {"name": "user_events", "version": 1}
EMBEDDER = "helpdesk_embedder"
DEFAULT_K = 25
RECENT_EVENTS = 20

SYSTEM_PROMPT = """You are a help desk assistant for an online shop.
Answer the customer's question using only the documents and the customer's recent
events below. Cite every document you use as [doc_name p.page ¶offset]. If the
documents do not answer the question, say so and suggest contacting support."""


class State(TypedDict, total=False):
    """What flows through the graph."""

    user_id: Any
    query: str
    k: int
    events: list[dict]
    sources: list[dict]
    answer: str
    trace: list[dict]


def _trace(state: State, step: str, **fields) -> list[dict]:
    entry = {"step": step, **fields}
    _logger.info("%s", json.dumps(entry, default=str)[:2000])
    return [*state.get("trace", []), entry]


def context_prompt(state: State) -> str:
    """The user message: the events, the retrieved chunks, then the question."""
    events = "\n".join(
        f"- {e.get('event_time')}: {e.get('event_type')} {e.get('product_name', '')} "
        f"{e.get('amount', '')}".rstrip()
        for e in state.get("events", [])
    )
    sources = "\n\n".join(
        f"[{s['doc_name']} p.{s['page']} ¶{s['offset']}] ({s['url']})\n{s['text']}"
        for s in state.get("sources", [])
    )
    return (
        f"Customer {state['user_id']}'s recent events:\n{events or '(none)'}\n\n"
        f"Documents:\n{sources or '(none found)'}\n\n"
        f"Question: {state['query']}"
    )


class Agent:
    """Loads the feature groups, the embedder and the LLM once; answers each request."""

    def __init__(self):
        import hopsworks
        from sentence_transformers import SentenceTransformer

        project = hopsworks.login()
        fs = project.get_feature_store()
        self.chunks = fs.get_feature_group(CHUNKS["name"], version=CHUNKS["version"])
        self.events = fs.get_feature_group(EVENTS["name"], version=EVENTS["version"])
        self.columns = [f.name for f in self.chunks.columns]
        registry = project.get_model_registry()
        model = max(registry.get_models(EMBEDDER), key=lambda m: m.version)
        self.encoder = SentenceTransformer(model.download())
        self.llm = self._llm()
        self.graph = self._graph()

    def _llm(self):
        url, key = os.environ.get("LLM_URL"), os.environ.get("LLM_API_KEY")
        if not (url and key):
            return None
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            base_url=url,
            api_key=key,
            model=os.environ.get("LLM_MODEL", "gpt-4o-mini"),
            temperature=0,
        )

    def _graph(self):
        from langgraph.graph import END, START, StateGraph

        graph = StateGraph(State)
        graph.add_node("events", self.lookup_events)
        graph.add_node("retrieve", self.retrieve)
        graph.add_node("answer", self.answer)
        graph.add_edge(START, "events")
        graph.add_edge("events", "retrieve")
        graph.add_edge("retrieve", "answer")
        graph.add_edge("answer", END)
        return graph.compile()

    def lookup_events(self, state: State) -> State:
        """The user's most recent events from the online store."""
        fg = self.events
        rows = fg.filter(fg.user_id == state["user_id"]).read(online=True, dataframe_type="pandas")
        if len(rows):
            rows = rows.sort_values("event_time", ascending=False).head(RECENT_EVENTS)
        events = json.loads(rows.to_json(orient="records", date_format="iso"))
        return {"events": events, "trace": _trace(state, "events", events=events)}

    def retrieve(self, state: State) -> State:
        """The k chunks nearest the query in the vector index."""
        vector = list(map(float, self.encoder.encode(state["query"], normalize_embeddings=True)))
        hits = self.nearest(vector, state["k"])
        sources = []
        for score, values in hits:
            row = dict(zip(self.columns, values, strict=True))
            row.pop("embedding", None)
            sources.append({**row, "score": float(score)})
        return {
            "sources": sources,
            "trace": _trace(
                state,
                "retrieve",
                k=state["k"],
                hits=[(s["doc_name"], s["page"], s["offset"], s["score"]) for s in sources],
            ),
        }

    def nearest(self, vector: list[float], k: int) -> list:
        """The k nearest chunks, or all of them when there are fewer.

        Clients before FSTORE-1970 fail when a project index holds fewer than k
        matches (they misread OpenSearch 2.19's k-range error), which a help desk
        with a few documents hits at the default k. Halving k until the search
        succeeds returns what there is; with a fixed client the first call does.
        """
        while True:
            try:
                return self.chunks.find_neighbors(vector, k=k)
            except Exception as exc:
                if k <= 1 or "requires k" not in str(exc):
                    raise
                k //= 2

    def answer(self, state: State) -> State:
        """The LLM's answer over the context, or why there is none."""
        prompt = context_prompt(state)
        if self.llm is None:
            answer = (
                "No LLM is configured: set LLM_URL and LLM_API_KEY in your account "
                "settings (Environment variables) and restart the agent. The documents "
                "and events found for this question are listed below."
            )
        else:
            reply = self.llm.invoke([("system", SYSTEM_PROMPT), ("user", prompt)])
            answer = reply.content
        return {
            "answer": answer,
            "trace": _trace(state, "answer", prompt=prompt, answer=answer),
        }

    def predict(self, inputs):
        """Answer one `{user_id, query, k}` request."""
        request = inputs[0] if isinstance(inputs, list) else inputs
        request = request.get("instances", [request])[0] if "instances" in request else request
        user_id, query = request.get("user_id"), (request.get("query") or "").strip()
        if user_id is None or not query:
            return {"error": "send user_id and query"}
        k = int(request.get("k") or DEFAULT_K)
        state = self.graph.invoke({"user_id": user_id, "query": query, "k": k, "trace": []})
        return {
            "answer": state["answer"],
            "sources": [
                {
                    key: s.get(key)
                    for key in (
                        "doc_name",
                        "url",
                        "path",
                        "page",
                        "offset",
                        "score",
                        "text",
                    )
                }
                for s in state["sources"]
            ],
            "events": state["events"],
            "trace": state["trace"],
        }


def build_app(agent: Agent):
    """The HTTP server around `agent`."""
    from fastapi import FastAPI, HTTPException

    app = FastAPI(title="Help desk agent", docs_url=None, redoc_url=None)

    @app.get("/")
    def ready() -> dict:
        return {"status": "ok"}

    @app.post("/predict")
    def predict(payload: dict) -> dict:
        return agent.predict(payload)

    @app.post("/v1/models/{target}")
    def kserve_predict(target: str, payload: dict) -> dict:
        if not target.replace("%3A", ":").endswith(":predict"):
            raise HTTPException(status_code=404, detail="Not Found")
        return agent.predict(payload)

    @app.post("/query")
    def query(payload: dict) -> dict:
        return agent.predict(payload)

    return app


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(build_app(Agent()), host="0.0.0.0", port=8080)
