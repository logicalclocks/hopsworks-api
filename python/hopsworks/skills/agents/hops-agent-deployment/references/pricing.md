# Price the turns: checks and where the cost shows

The prices and how to set them are in SKILL.md, "Price the turns, or cost reads zero".

**Then check against a real trace, because the source can be wrong.** The spans say which model actually answered, and a framework default beats anything the README claims:

```python
from collections import Counter
trace = agent.trace(agent.traces(limit=1)[0].trace_id)
print(Counter(s["attributes"].get("llm.model_name") for s in trace.spans
               if s["attributes"].get("openinference.span.kind") == "LLM"))
```

One chat after deploying settles it in a second, and it is the only way to catch an agent priced for the model its docstring names while the SDK runs its own default.

**Where the cost shows.** A trace: `trace.total_input_tokens` / `total_output_tokens` and `trace.total_input_cost` / `total_output_cost` (there is no `total_cost` or `estimated_cost` on a trace). The totals are the sum over the trace's **model calls** — `llm.token_count.prompt` / `completion` on each LLM span, the turn's own figure only when no model call was traced — and cached prompt tokens are counted and priced at the full input price, so the cost is an upper bound for a provider that discounts cache reads. A run: `estimated_cost` and `costed_trial_rate` on its metrics, suite runs and monitor samples alike. A window: `agent.llm_metrics()` → `estimated_cost_usd` per model.

The two keys are per deployment, so two agents on different models each cost correctly, and re-deploying with new prices is what changes them — they are read per turn, not baked into old traces. Re-reading an old window after a price change does not re-cost it.
