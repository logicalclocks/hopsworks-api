"""Extract an agent's structure graph for the chat panel's Graph tab.

LangChain/LangGraph compiled graphs expose ``.get_graph()`` returning a
drawable graph with ``.nodes`` and ``.edges``; LlamaIndex workflows, OpenAI
Agents agents and plain dicts are accepted too. The result is a
framework-neutral ``{nodes, edges}`` spec the panel renders with reactflow.

Every graph here is the structure the agent declares, not the path a turn
took: it says what the agent can do, and is the same for every request. What
actually ran is in the trace.
"""

from __future__ import annotations

import logging
from typing import Any

from hopsworks_apigen import public


log = logging.getLogger(__name__)


@public
def to_graph_spec(graph: Any) -> dict[str, Any] | None:
    """Normalise a framework graph to ``{"nodes": [...], "edges": [...]}``.

    Accepts a compiled LangGraph/LangChain runnable (``.get_graph()`` is
    called), an already-drawable graph (``.nodes`` / ``.edges``), an OpenAI
    Agents agent, or a plain ``{"nodes", "edges"}`` dict. Returns None
    (best-effort) when the shape is unrecognised — the Graph capability is
    simply not advertised.
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
        # Frameworks that draw nothing for themselves. A LlamaIndex Workflow
        # declares its flow as @step methods; an OpenAI Agents agent hangs it
        # off the agent object as handoffs, tools and servers.
        return _llamaindex_workflow_spec(graph) or _agent_structure_spec(graph)

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


def _looks_like_an_agent(obj: Any) -> bool:
    """Whether this object declares its structure the way an OpenAI Agents agent does."""
    if not isinstance(getattr(obj, "name", None), str):
        return False
    return any(
        isinstance(getattr(obj, attr, None), list)
        for attr in ("handoffs", "tools", "mcp_servers")
    )


def _agent_structure_spec(agent: Any) -> dict[str, Any] | None:
    """Derive the graph an OpenAI Agents agent declares on itself.

    The SDK keeps no graph object. The structure hangs off the agent: the
    agents it can hand off to, the tools it can call, the MCP servers it can
    reach. Handoffs are followed, so a triage agent brings the whole tree it
    fronts and every agent in that tree brings its own tools and servers. A
    tool of the same name reached from two agents is one node with two edges,
    which is what makes a shared tool read as shared.

    The same reading ``agents.extensions.visualization.draw_graph`` does. What
    the node is stays on the spec as ``kind`` rather than being baked into a
    colour here, so the panel decides how an agent differs from a tool; the
    frameworks that draw a graph of uniform steps carry no kind at all.
    """
    if not _looks_like_an_agent(agent):
        return None

    start, end = "__start__", "__end__"
    nodes: dict[str, dict[str, Any]] = {start: {"id": start, "label": start}}
    edges: list[dict[str, Any]] = []

    def add(node_id: str, label: str, kind: str | None = None) -> None:
        node: dict[str, Any] = {"id": node_id, "label": label}
        if kind:
            node["kind"] = kind
        nodes.setdefault(node_id, node)

    def attached(owner: str, items: Any, kind: str) -> None:
        for item in items or []:
            label = getattr(item, "name", None) or str(item)
            node_id = f"{kind}:{label}"
            add(node_id, str(label), kind)
            edges.append({"source": owner, "target": node_id, "kind": kind})

    def walk(current: Any) -> None:
        name = getattr(current, "name", None)
        # Already walked, so its tools and handoffs are in: two agents that can
        # hand off to each other would otherwise recurse for ever. The edge to
        # it is the caller's to add, and still gets added.
        if not isinstance(name, str) or name in nodes:
            return
        add(name, name, "agent")
        attached(name, getattr(current, "tools", None), "tool")
        attached(name, getattr(current, "mcp_servers", None), "mcp")

        handoffs = getattr(current, "handoffs", None) or []
        for handoff in handoffs:
            # An entry is either the agent itself or a Handoff naming one. A
            # Handoff carries no tools, so that agent is as far as this goes.
            target = getattr(handoff, "name", None)
            if isinstance(target, str):
                walk(handoff)
            else:
                target = getattr(handoff, "agent_name", None)
                if not isinstance(target, str):
                    continue
                add(target, target, "agent")
            edges.append({"source": name, "target": target})

        if not handoffs:
            # Nothing to transfer to, so this is where a run can finish.
            edges.append({"source": name, "target": end})

    walk(agent)
    root = getattr(agent, "name", None)
    if root not in nodes:
        return None
    edges.insert(0, {"source": start, "target": root})
    if any(edge["target"] == end for edge in edges):
        add(end, end)
    return {"nodes": list(nodes.values()), "edges": edges}
