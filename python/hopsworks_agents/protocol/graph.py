"""Extract an agent's structure graph for the chat panel's Graph tab.

LangChain/LangGraph compiled graphs expose ``.get_graph()`` returning a
drawable graph with ``.nodes`` and ``.edges``; LlamaIndex workflows and plain
dicts are accepted too. The result is a framework-neutral ``{nodes, edges}``
spec the panel renders with reactflow.
"""

from __future__ import annotations

import logging
from typing import Any


log = logging.getLogger(__name__)


def to_graph_spec(graph: Any) -> dict[str, Any] | None:
    """Normalise a framework graph to ``{"nodes": [...], "edges": [...]}``.

    Accepts a compiled LangGraph/LangChain runnable (``.get_graph()`` is
    called), an already-drawable graph (``.nodes`` / ``.edges``), or a plain
    ``{"nodes", "edges"}`` dict. Returns None (best-effort) when the shape is
    unrecognised — the Graph capability is simply not advertised.
    """
    if graph is None:
        return None
    if isinstance(graph, dict) and "nodes" in graph and "edges" in graph:
        return graph

    drawable = graph
    get_graph = getattr(graph, "get_graph", None)
    if callable(get_graph):
        try:
            drawable = get_graph()
        except Exception:  # noqa: BLE001
            log.warning("Could not read the agent graph via get_graph()", exc_info=True)
            return None

    nodes_attr = getattr(drawable, "nodes", None)
    edges_attr = getattr(drawable, "edges", None)
    if nodes_attr is None or edges_attr is None:
        # a LlamaIndex Workflow has no get_graph(); derive it from @step methods
        return _llamaindex_workflow_spec(graph)

    try:
        node_items = nodes_attr.values() if isinstance(nodes_attr, dict) else nodes_attr
        nodes = []
        for node in node_items:
            node_id = getattr(node, "id", None)
            if node_id is None:
                node_id = str(node)
            label = getattr(node, "name", None) or str(node_id)
            nodes.append({"id": str(node_id), "label": str(label)})

        edges = []
        for edge in edges_attr:
            source = getattr(edge, "source", None)
            target = getattr(edge, "target", None)
            if source is None or target is None:
                continue
            spec: dict[str, Any] = {"source": str(source), "target": str(target)}
            data = getattr(edge, "data", None)
            if data:
                spec["label"] = str(data)
            if getattr(edge, "conditional", False):
                spec["conditional"] = True
            edges.append(spec)
    except Exception:  # noqa: BLE001
        log.warning("Could not extract nodes/edges from the agent graph", exc_info=True)
        return None

    if not nodes:
        return None
    return {"nodes": nodes, "edges": edges}


def _llamaindex_workflow_spec(workflow: Any) -> dict[str, Any] | None:
    """Derive a step graph from a LlamaIndex Workflow's @step methods.

    Each ``@step`` declares the event types it consumes (parameter types) and
    produces (return types) via ``__step_config``; the flow is the join over
    those. Event nodes are collapsed into labeled edges between steps, and
    ``StartEvent`` / ``StopEvent`` become synthetic ``__start__`` / ``__end__``
    nodes — so a custom workflow renders as cleanly as a LangGraph.
    """
    get_steps = getattr(workflow, "_get_steps", None)
    if not callable(get_steps):
        return None
    try:
        steps = get_steps()
    except Exception:  # noqa: BLE001
        return None
    if not steps:
        return None

    consumers: dict[str, list[str]] = {}  # event name -> steps that accept it
    producers: dict[str, list[str]] = {}  # event name -> steps that return it
    step_returns: dict[str, set[str]] = {}  # step -> event names it returns
    step_names: list[str] = []

    for name, fn in steps.items():
        config = getattr(fn, "__step_config", None)
        if config is None:
            continue
        step_names.append(name)
        for event in getattr(config, "accepted_events", None) or []:
            ev = getattr(event, "__name__", str(event))
            consumers.setdefault(ev, []).append(name)
        returns = getattr(config, "return_types", None) or []
        step_returns[name] = {getattr(e, "__name__", str(e)) for e in returns}
        for event in returns:
            ev = getattr(event, "__name__", str(event))
            producers.setdefault(ev, []).append(name)

    if not step_names:
        return None

    start, end = "__start__", "__end__"
    nodes = [{"id": n, "label": n} for n in step_names]
    nodes.append({"id": start, "label": "__start__"})
    nodes.append({"id": end, "label": "__end__"})

    edges: list[dict[str, Any]] = []
    for name in consumers.get("StartEvent", []):
        edges.append({"source": start, "target": name})
    for name in producers.get("StopEvent", []):
        # a step that can also branch elsewhere makes this a conditional exit
        conditional = len(step_returns.get(name, set())) > 1
        edge: dict[str, Any] = {"source": name, "target": end}
        if conditional:
            edge["conditional"] = True
        edges.append(edge)
    # collapse each intermediate event into producer -> consumer edges
    for ev_name, prod_steps in producers.items():
        if ev_name in ("StartEvent", "StopEvent"):
            continue
        for producer in prod_steps:
            for consumer in consumers.get(ev_name, []):
                edge = {"source": producer, "target": consumer, "label": ev_name}
                if len(step_returns.get(producer, set())) > 1:
                    edge["conditional"] = True
                edges.append(edge)

    return {"nodes": nodes, "edges": edges}
