"""Stable attribute names shared by every Hopsworks agent component.

The SDK stamps these on spans, the OTLP sidecar parses them, and the eval
runner reads them back. Three components agreeing on string literals by
copy-paste is how they stop agreeing, so they live here.

Deliberately dependency-free — no imports beyond the standard library — so the
sidecar and the eval runner can import it without pulling FastAPI. Keep it that
way when this module moves into ``hopsworks-api``.

References:
- OTel GenAI semantic conventions (``gen_ai.*``)
- OpenInference (``openinference.span.kind``, ``input.value``/``output.value``)
- The Hopsworks agent evaluation design (``hopsworks.*``)
"""

from __future__ import annotations


# ── environment ───────────────────────────────────────────────────────────

# Injected by the platform when the deployment runs in eval mode (tools mocked
# or pointed at scratch resources). The SDK only *reports* it in the manifest;
# what eval mode means for a given agent is the agent's business.
# Not HOPSWORKS_EVAL_MODE, and not AGENT_EVAL_MODE: the platform reserves the
# HOPS_, HOPSWORKS_, HOPSFS_ and AGENT_ prefixes, so a deployment could not set
# either of them and the flag was unusable by the only people who need it.
#
# EVAL_ is the prefix the evaluation side already uses — EVAL_JUDGE_API_KEY,
# EVAL_JUDGE_MODEL — so this joins a family rather than starting one.
EVAL_MODE_ENV = "EVAL_MODE"

# ── span naming ───────────────────────────────────────────────────────────

OPERATION_INVOKE_AGENT = "invoke_agent"
OPERATION_EXECUTE_TOOL = "execute_tool"
OPERATION_CHAT = "chat"

# ── agent identity (OTel GenAI) ───────────────────────────────────────────

GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
GEN_AI_AGENT_NAME = "gen_ai.agent.name"
GEN_AI_AGENT_VERSION = "gen_ai.agent.version"
GEN_AI_CONVERSATION_ID = "gen_ai.conversation.id"
# OpenInference's end-user attribute. The Hopsworks sidecar reads it into the
# user_id column of otel_spans, and every OpenInference-aware viewer reads it
# too, so a trace carrying it says who it was with without Hopsworks present.
# Only ever set from a subject the client asserted: a subject learned mid-turn
# cannot reach spans already exported, which is what the conversation-subject
# index is for.
USER_ID = "user.id"
GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_USAGE_PROMPT_TOKENS = "gen_ai.usage.prompt_tokens"
GEN_AI_USAGE_COMPLETION_TOKENS = "gen_ai.usage.completion_tokens"

# ── OpenInference ─────────────────────────────────────────────────────────

SPAN_KIND = "openinference.span.kind"
SPAN_KIND_AGENT = "AGENT"
SPAN_KIND_TOOL = "TOOL"
SPAN_KIND_LLM = "LLM"

# The sidecar already reads these as its last-resort message source, so a root
# span that sets them needs no sidecar change to produce correct transcripts.
INPUT_VALUE = "input.value"
OUTPUT_VALUE = "output.value"

# ── tools ─────────────────────────────────────────────────────────────────

# `gen_ai.tool.*` is still in development upstream; pin these against the
# semantic-conventions release adopted at implementation time. Tool failures
# are span status ERROR plus `error.type`, per OTel, not a boolean attribute.
TOOL_NAME = "tool.name"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"
GEN_AI_TOOL_CALL_ID = "gen_ai.tool.call.id"
GEN_AI_TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments"
GEN_AI_TOOL_CALL_RESULT = "gen_ai.tool.call.result"

# Where a tool call's arguments and result actually turn up, most specific
# first. The design doc names `gen_ai.tool.*` and OpenInference; in practice
# LlamaIndex and LangChain instrumentation write `input.value` / `output.value`
# on the TOOL span and nothing else, so reading one convention would see
# arguments on almost no real trace. First non-empty wins.
TOOL_ARGUMENT_KEYS = (
    GEN_AI_TOOL_CALL_ARGUMENTS,
    "tool.parameters",
    "tool.arguments",
    INPUT_VALUE,
)
TOOL_RESULT_KEYS = (
    GEN_AI_TOOL_CALL_RESULT,
    "tool.result",
    OUTPUT_VALUE,
)

# ── model / provider ──────────────────────────────────────────────────────

GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_RESPONSE_MODEL = "gen_ai.response.model"
GEN_AI_PROVIDER_NAME = "gen_ai.provider.name"
LLM_MODEL_NAME = "llm.model_name"
LLM_TOKEN_COUNT_PROMPT = "llm.token_count.prompt"
LLM_TOKEN_COUNT_COMPLETION = "llm.token_count.completion"
LLM_TOKEN_COUNT_INPUT = "llm.token_count.input"
LLM_TOKEN_COUNT_OUTPUT = "llm.token_count.output"
INPUT_TOKEN_KEYS = (
    GEN_AI_USAGE_INPUT_TOKENS,
    GEN_AI_USAGE_PROMPT_TOKENS,
    LLM_TOKEN_COUNT_PROMPT,
    LLM_TOKEN_COUNT_INPUT,
)
OUTPUT_TOKEN_KEYS = (
    GEN_AI_USAGE_OUTPUT_TOKENS,
    GEN_AI_USAGE_COMPLETION_TOKENS,
    LLM_TOKEN_COUNT_COMPLETION,
    LLM_TOKEN_COUNT_OUTPUT,
)

# ── Hopsworks correlation ─────────────────────────────────────────────────

CONVERSATION_ID = "hopsworks.conversation_id"
RESPONSE_ID = "hopsworks.response_id"
MESSAGE_ID = "hopsworks.message_id"
DEPLOYMENT_ID = "hopsworks.deployment_id"
FRAMEWORK = "hopsworks.framework"

# ── eval correlation ──────────────────────────────────────────────────────

# Sent by the eval runner as W3C baggage and copied onto spans by the SDK's
# baggage span processor. Namespaced under `hopsworks.` so a future OTel
# semantic convention cannot collide with a bare `eval.*`.
EVAL_RUN_ID = "hopsworks.eval.run_id"
EVAL_SUITE_ID = "hopsworks.eval.suite_id"
EVAL_SUITE_VERSION = "hopsworks.eval.suite_version"
EVAL_TASK_ID = "hopsworks.eval.task_id"
EVAL_TASK_VERSION = "hopsworks.eval.task_version"
EVAL_TRIAL_ID = "hopsworks.eval.trial_id"
EVAL_TRIAL_INDEX = "hopsworks.eval.trial_index"

# Baggage crosses the network from whoever called the agent, so it is only as
# trustworthy as the caller. The span processor copies entries under these
# prefixes and drops everything else, rather than letting an arbitrary caller
# write arbitrary attributes into the project's trace tables.
BAGGAGE_PREFIXES = ("hopsworks.eval.",)

# ── guardrails ────────────────────────────────────────────────────────────

# Defined ahead of any enforcement layer so that when guardrails ship, the
# derived tables and this module need no migration.
GUARDRAIL_EVENT = "hopsworks.guardrail.trigger"
GUARDRAIL_NAME = "hopsworks.guardrail.name"
GUARDRAIL_ACTION = "hopsworks.guardrail.action"
GUARDRAIL_RULE_ID = "hopsworks.guardrail.rule_id"
GUARDRAIL_POLICY_VERSION = "hopsworks.guardrail.policy_version"
GUARDRAIL_TRIGGER_COUNT = "hopsworks.guardrail.trigger_count"
GUARDRAIL_BLOCKED = "hopsworks.guardrail.blocked"
