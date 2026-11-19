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
"""Tools that ask an `ontology` factory system about its data.

The surface is the one the auto-ontology MCP server (Apache-2.0) publishes: the business glossary and table reads, a cheap coverage check, the text-to-SQL turn and a readiness verdict.
Every tool names the system by its slug, which is also its app's name, and reaches the app through the Hopsworks app proxy as the logged-in member, so the member's project role decides what the app allows.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from fastmcp import (
    Context,  # noqa: TC002
)
from fastmcp.exceptions import ToolError
from hopsworks.mcp.models.ontology import OntologyAnswer, OntologyReadiness
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import ontology_api


if TYPE_CHECKING:
    from fastmcp import FastMCP


def _refused(exc: RestAPIError, system: str) -> ToolError:
    return ToolError(ontology_api._refusal(exc, system) or str(exc))


async def _call(system: str, fn: Any, *args: Any) -> Any:
    try:
        return await asyncio.to_thread(fn, system, *args)
    except RestAPIError as exc:
        raise _refused(exc, system) from exc


class OntologyTools:
    """Tools for the ontology systems of the current project."""

    def __init__(self, mcp: FastMCP):
        """Register the tools with the MCP server.

        Parameters:
            mcp: The MCP server instance.
        """
        self.mcp = mcp
        for tool in (
            self.search_terms,
            self.get_term,
            self.get_term_columns,
            self.get_term_sql_attributes,
            self.get_sql_attribute,
            self.describe_table,
            self.check_answerable,
            self.ask_question,
            self.check_readiness,
        ):
            self.mcp.tool(tags=[TAGS.ONTOLOGY, TAGS.READ, TAGS.STATEFUL])(tool)

    async def search_terms(
        self,
        system: str,
        query: str | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """Search the business glossary of an ontology system.

        Terms are the business concepts compiled over the system's data sources, such as Customer, Open Order or Net Revenue, each mapped to columns and SQL expressions.
        Use this first to learn what a business word means in this system before asking a question that depends on it.

        Parameters:
            system: The ontology system's slug, also its app's name.
            query: A case-insensitive substring of the term name.
            skip: Rows to skip before the page starts.
            limit: Page size, at most 100; every match when omitted.

        Returns:
            The matching terms.
        """
        return await _call(
            system,
            ontology_api._get,
            "api/terms",
            {"query": query, "skip": skip, "limit": limit},
        )

    async def get_term(self, system: str, term_id: str) -> Any:
        """Fetch one glossary term: its description, synonyms and related terms.

        Use it after search_terms to confirm a term means what you assumed.

        Parameters:
            system: The ontology system's slug, also its app's name.
            term_id: The term's id, from search_terms.

        Returns:
            The term.
        """
        return await _call(
            system, ontology_api._get, f"api/terms/{quote(term_id, safe='')}"
        )

    async def get_term_columns(
        self,
        system: str,
        term_id: str,
        skip: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """List the table columns a glossary term maps to: the way from a business concept to the data behind it.

        Parameters:
            system: The ontology system's slug, also its app's name.
            term_id: The term's id, from search_terms.
            skip: Rows to skip before the page starts.
            limit: Page size, at most 100; every row when omitted.

        Returns:
            The term's column attributes.
        """
        return await _call(
            system,
            ontology_api._get,
            f"api/terms/{quote(term_id, safe='')}/column-attributes",
            {"skip": skip, "limit": limit},
        )

    async def get_term_sql_attributes(
        self,
        system: str,
        term_id: str,
        skip: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """List the reviewed SQL expressions under a glossary term, such as a margin, a ratio or a rolling total.

        Prefer one of these to writing the arithmetic yourself.

        Parameters:
            system: The ontology system's slug, also its app's name.
            term_id: The term's id, from search_terms.
            skip: Rows to skip before the page starts.
            limit: Page size, at most 100; every row when omitted.

        Returns:
            The term's SQL attributes.
        """
        return await _call(
            system,
            ontology_api._get,
            f"api/terms/{quote(term_id, safe='')}/sql-attributes",
            {"skip": skip, "limit": limit},
        )

    async def get_sql_attribute(self, system: str, attr_id: str) -> Any:
        """Fetch one SQL attribute: its expression, the term it belongs to and what it is for.

        Parameters:
            system: The ontology system's slug, also its app's name.
            attr_id: The SQL attribute's id.

        Returns:
            The SQL attribute.
        """
        return await _call(
            system, ontology_api._get, f"api/sql-attributes/{quote(attr_id, safe='')}"
        )

    async def describe_table(
        self,
        system: str,
        table_id: str,
        skip: int | None = None,
        limit: int | None = None,
    ) -> Any:
        """Describe one table in full: its columns, the glossary terms that reference it and the SQL attributes over it.

        Parameters:
            system: The ontology system's slug, also its app's name.
            table_id: The table's id, from a term's columns.
            skip: Rows to skip before the page starts.
            limit: Page size, at most 100; every row when omitted.

        Returns:
            The table's details.
        """
        return await _call(
            system,
            ontology_api._get,
            f"api/exploration/tables/{quote(table_id, safe='')}/details",
            {"skip": skip, "limit": limit},
        )

    async def check_answerable(
        self, system: str, question: str, max_distance: float | None = None
    ) -> Any:
        """Grade how well the ontology covers the entities of a question, without running the agent.

        A cheap pre-flight: use it to decide whether the system can answer a question before ask_question, which is far slower.

        Parameters:
            system: The ontology system's slug, also its app's name.
            question: The question, in plain language.
            max_distance: The largest L2 vector distance at which a candidate still counts; lower is closer.

        Returns:
            The coverage of each entity in the question.
        """
        body: dict[str, Any] = {"question": question}
        if max_distance is not None:
            body["max_distance"] = max_distance
        return await _call(
            system, ontology_api._post, "api/question-entity-coverage", body
        )

    async def ask_question(
        self,
        system: str,
        question: str,
        conversation_id: str | None = None,
        evidence: str | None = None,
        ctx: Context | None = None,
    ) -> OntologyAnswer:
        """Ask an ontology system a question about its data: it resolves the question against its ontology, writes SQL, runs it and returns the answer with the SQL and the rows.

        It takes many model calls, typically tens of seconds, so use check_answerable first when unsure the question is in scope, and ask one well-formed question rather than several narrow ones.
        Each step the agent takes is reported as progress.
        Observers and Feature store restricted members get no chat.

        Parameters:
            system: The ontology system's slug, also its app's name.
            question: The question, in plain language.
            conversation_id: A conversation UUID to continue, so follow-up questions resolve against its earlier turns; a conversation takes one question at a time.
            evidence: Authoritative evidence given separately from the question.
            ctx: The MCP context, provided automatically.

        Returns:
            The answer, its SQL and at most 100 rows.
        """
        events = ontology_api._ask(system, question, conversation_id, evidence)
        received: list[dict[str, Any]] = []
        steps = 0
        try:
            while True:
                event = await asyncio.to_thread(next, events, None)
                if event is None:
                    break
                received.append(event)
                if event.get("type") == "step" and ctx:
                    steps += 1
                    await ctx.report_progress(
                        progress=steps, message=ontology_api._step_line(event)
                    )
            return OntologyAnswer(**ontology_api._answer(received))
        except RestAPIError as exc:
            raise _refused(exc, system) from exc
        except (RuntimeError, OSError) as exc:
            raise ToolError(f"{system}: {exc}") from exc

    async def check_readiness(self, system: str) -> OntologyReadiness:
        """Report whether an ontology system can answer questions: an ingested catalog, a data source to run SQL on, and a compiled ontology are all required.

        Use it before the first question to an unfamiliar system, and whenever ask_question comes back empty: it separates a system that is not set up from a question that was not understood.
        Facts the caller may not read are listed as unverified, not as blockers.

        Parameters:
            system: The ontology system's slug, also its app's name.

        Returns:
            The verdict, with what blocks it.
        """
        return OntologyReadiness(**await _call(system, ontology_api._readiness))
