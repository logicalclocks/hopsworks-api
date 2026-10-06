# Building a RAG agent system

An agent system with `requirements.task: rag` (the help desk example) answers a
user's question from documents the user uploads and from the user's recent
events. It is built phase by phase like a batch or real-time system, with these
phases, from the reference implementation in [rag_agent/](rag_agent/):

| Phase | Builds | From |
| --- | --- | --- |
| `reqs` | the docs directory, and the user's documents in it | this page |
| `data` | the events feature group, online | hops-synthetic-data |
| `features` | the chunk embedding feature group with its vector index, and the ingestion job | `rag_agent/ingest_docs.py` |
| `train` | the embedding model, downloaded from Hugging Face into the Model Registry; nothing is trained | `rag_agent/register_embedder.py` |
| `infer` | the LangGraph agent deployment | `rag_agent/agent.py` |
| `app` | the JavaScript chat UI | `rag_agent/app/` |

Names come from `system.yaml`; the reference files carry the help desk
example's (`helpdesk_doc_chunks`, `user_events`, `helpdesk_embedder`,
`helpdeskagent`). Copy each file into the system and change only those names:
the code is tested as it is.

## reqs: the documents

Create the docs directory without asking: `hops files mkdir <data.docs.path>`
(`Resources/helpdesk-docs` for the example; creating an existing one is fine).
Then ask with `AskUserQuestion`, printing where and what:

> Upload the help desk documents to `Resources/helpdesk-docs` (in the Hopsworks
> file browser: Resources, then helpdesk-docs, then drag the files in). PDF, TXT,
> Markdown, Word (.docx) and OpenDocument (.odt) files are read.

with the options "Uploaded", "Use the sample documents" (copy
`rag_agent/sample_docs/*` there with `hops files upload`) and "Later"
(continue with the sample documents and say the ingestion job is run again
after an upload). Count the files with `hops files list`; `data.docs.files` records
the count and `data.docs.status: present` when there is at least one.

The LLM is not asked here. `hops mlsystem create` asked for its URL and API key and saved
them as the user's account environment variables `LLM_URL`, `LLM_API_KEY` and
`LLM_MODEL`, which Hopsworks sets in every job, app and deployment the user
starts; `inference.agent.llm` records their names, never their values. When
they are missing (`python -c "import hopsworks; hopsworks.login(); print([v.name
for v in hopsworks.get_env_vars_api().get_env_vars(include_value=False)])"`), the agent still
deploys and answers with the retrieved passages only, and the report says how to
add them (Account settings, Environment variables).

## data: the user's events

Synthetic, as **hops-synthetic-data** says, into an online feature group
`user_events` with `primary_key: [user_id, event_id]` and `event_time`: per user
a few purchases, and some of them returned, refunded, delivered late or
complained about, with `product_name` and `amount`. The agent reads a user's
events with an online read filtered on `user_id`
(`fg.filter(fg.user_id == u).read(online=True)`), so the group is
online-enabled and `user_id` is part of its key. `met` when the backfill is
materialized and an online read for one user returns rows.

## The environments

Two, because Hopsworks runs jobs only in an environment built from a feature or
training pipeline base, and serves deployments from an inference base. Both
install the same `requirements.txt`, so documents and questions are embedded by
the same library versions:

```bash
hops env clone <slug>-jobs-env --from torch-training-pipeline     # ingestion and registration jobs
hops env install <slug>-jobs-env -f requirements.txt
hops env clone <slug>-agent-env --from torch-inference-pipeline   # the agent deployment
hops env install <slug>-agent-env -f requirements.txt
```

`requirements.txt` is `rag_agent/requirements.txt`, copied into the system. The
app runs in `python-agent-pipeline`, which ships FastAPI.

## train: the embedding model

`register_embedder.py` as the job `<slug>-register-embedder`, in `<slug>-jobs-env`
(`hops job deploy <slug>-register-embedder src/<slug_pkg>/register_embedder.py
--env <slug>-jobs-env --run --wait`): it downloads `sentence-transformers/all-MiniLM-L6-v2` (the default; another
sentence-transformers model when `training.embedding_model.repo` names one) at a
pinned revision and registers it as `training.embedding_model.name`. Record
`training: {required: false, embedding_model: {repo, revision, name, version,
dimension}, status: met}`. It is idempotent: a second run finds the revision
registered and leaves it.

## features: the chunk embeddings

`ingest_docs.py` as the job `<slug>-ingest-docs`, in `<slug>-jobs-env`
(`hops job deploy <slug>-ingest-docs src/<slug_pkg>/ingest_docs.py --env <slug>-jobs-env
--args "--docs <data.docs.path> --fg <name> --model <embedding_model.name>" --run --wait`). It reads
every document in the directory, cuts each page into chunks of paragraphs
(400 to 1200 characters, never across a page), embeds each chunk, and writes
`chunk_id, doc_name, path, url, page, offset, text, embedding` into an
online feature group with an `EmbeddingIndex` on `embedding` (cosine, the
model's dimension). `url` opens the document's folder in the file browser,
relative to the UI's origin; `page` is 1-based, `offset` the 0-based paragraph
in the page. A re-run overwrites the chunks of each document and deletes those of
documents that were removed, so **the job is the command that reads all the
documents**: run it again (`hops job run <slug>-ingest-docs --wait`) after
uploading more. Record the pipeline with its job; `met` when the group has rows
for every readable document and `find_neighbors` returns hits for a sample
question.

## infer: the agent

`agent.py` is a LangGraph workflow with three nodes in a fixed order: `events`
(the user's 20 most recent events, online), `retrieve` (the query embedded and
the `k` nearest chunks from the vector index, `k` 25 by default and per request),
and `answer` (both in the LLM's context window, with the passages cited as
`[doc_name p.page ¶offset]`). The file is the server: it serves `POST /predict`
and KServe's `POST /v1/models/<name>:predict` on port 8080, and builds the graph
before listening, so the deployment is ready only when it can answer. torch and
the embedder need 3 GB; the 1 GB default is OOM-killed. Deploy it as an agent:

```bash
hops agent create src/<slug_pkg>/agent.py --name <inference.agent.deployment> --environment <slug>-agent-env --memory 3072
hops agent start <inference.agent.deployment>
hops agent query <inference.agent.deployment> --data '{"user_id": 17, "query": "Where is my refund?"}'
```

The request is `{user_id, query, k}`; the reply is `{answer, sources: [{doc_name,
url, path, page, offset, score, text}], events, trace}`, where `trace` holds
every step's inputs and outputs. `measured` gets one line per benchmark run: the
p99 over 50 questions of the sample set against `requirements.sla.agent`.
`met` when the query returns sources and events, and an answer when the LLM is
configured.

## app: the chat

Copy `rag_agent/app/` to `<slug>/app/` and deploy it as **hops-app** says, in
`python-agent-pipeline`: a user id picker (the users with events), the question
and Send, which posts `{user_id, query}` to `/api/ask`; the app calls the agent
deployment and shows the answer, the cited passages with a link to each
document, and the events the agent read.
