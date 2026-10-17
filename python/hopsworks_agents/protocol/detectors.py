"""PII and secret detection, shared by everything that needs it.

Three callers, one implementation, deliberately:

- **trace-to-eval promotion**, which copies production user data into
  long-lived suites and must not carry secrets across that boundary;
- **``OTEL_REDACT_PATTERNS``**, the sidecar's redaction of message content
  before it is persisted;
- **runtime guardrails**, once they ship, which run synchronously in the
  serving path.

That last one is why this lives on the *serving* side rather than in
``hopsworks_agents.eval``: a guardrail runs in the request path, so if the
detectors lived on the eval side the serving package would have to import it
and the one structural rule of the split would invert. The eval side imports
*down* into this module. ``tests/test_import_isolation.py`` holds the line.

Dependency-free on purpose — the standard library only — so the sidecar and a
job can both import it without pulling anything in.

**What this is.** A reviewer's aid that raises the floor, not a guarantee. It
finds shapes it knows; it cannot find a customer's name in free text, an
internal identifier, or a secret in a format nobody anticipated. The promotion
workflow requires human confirmation *because* of that, and a detector that
quietly misses is more dangerous than no detector at all, since the reviewer
starts trusting it. Recall is what matters here; a false positive costs one
click.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


# Compiled once: Pattern.compile is expensive and these are immutable.
_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("EMAIL", re.compile(r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b")),
    # A private key block is unambiguous and catastrophic; matched first and
    # broadly rather than precisely.
    (
        "PRIVATE_KEY",
        re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"
        ),
    ),
    ("AWS_ACCESS_KEY", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("GITHUB_TOKEN", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}\b")),
    ("SLACK_TOKEN", re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{10,}\b")),
    ("OPENAI_KEY", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
    ("ANTHROPIC_KEY", re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b")),
    (
        "JWT",
        re.compile(
            r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"
        ),
    ),
    ("BEARER_TOKEN", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{20,}")),
    ("US_SSN", re.compile(r"\b(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b")),
    ("IP_ADDRESS", re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")),
    (
        "PHONE",
        re.compile(
            r"(?<![\d-])(?:\+\d{1,3}[\s-]?)?(?:\(\d{2,4}\)[\s-]?|\d{2,4}[\s-])\d{3}[\s-]?\d{3,4}(?![\d-])"
        ),
    ),
]

# Card numbers get their own pass: the shape alone matches far too much (order
# ids, timestamps), so a Luhn check decides. Without it every long number in a
# transcript is flagged and reviewers start skimming past the warnings.
_CARD_CANDIDATE = re.compile(r"\b(?:\d[ -]?){13,19}\b")

_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|password|passwd|token|credential)s?\b\s*[:=]\s*"
    r"[\"']?([^\s\"',;]{8,})"
)


@dataclass(frozen=True)
class Finding:
    """One detection. ``start``/``end`` index into the string that was scanned,.

    so a reviewer can be shown exactly what was matched rather than a count.
    """

    kind: str
    value: str
    start: int
    end: int

    @property
    def preview(self) -> str:
        """Enough to recognise, not enough to leak. Shown in review UIs, which.

        would otherwise re-expose the very secret being redacted.
        """
        if len(self.value) <= 8:
            return "*" * len(self.value)
        return f"{self.value[:3]}…{self.value[-2:]}"


def _luhn(digits: str) -> bool:
    total = 0
    for index, char in enumerate(reversed(digits)):
        value = int(char)
        if index % 2 == 1:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def _overlaps(start: int, end: int, taken: list[tuple[int, int]]) -> bool:
    return any(start < t_end and end > t_start for t_start, t_end in taken)


def detect(text: str) -> list[Finding]:
    """Every PII/secret shape found in ``text``, ordered by position.

    Overlapping matches are resolved in favour of the first pattern to claim
    the span, so a private key block is not also reported as a dozen other
    things inside it.
    """
    if not text:
        return []

    findings: list[Finding] = []
    taken: list[tuple[int, int]] = []

    for kind, pattern in _PATTERNS:
        for match in pattern.finditer(text):
            if _overlaps(match.start(), match.end(), taken):
                continue
            findings.append(Finding(kind, match.group(0), match.start(), match.end()))
            taken.append((match.start(), match.end()))

    for match in _CARD_CANDIDATE.finditer(text):
        digits = re.sub(r"[ -]", "", match.group(0))
        if not (13 <= len(digits) <= 19 and _luhn(digits)):
            continue
        if _overlaps(match.start(), match.end(), taken):
            continue
        findings.append(
            Finding("CREDIT_CARD", match.group(0), match.start(), match.end())
        )
        taken.append((match.start(), match.end()))

    # `api_key: hunter2` style assignments, which none of the shape patterns
    # above catch because the value itself looks like nothing in particular
    for match in _ASSIGNMENT.finditer(text):
        start, end = match.span(2)
        if _overlaps(start, end, taken):
            continue
        findings.append(Finding("SECRET_ASSIGNMENT", match.group(2), start, end))
        taken.append((start, end))

    return sorted(findings, key=lambda f: f.start)


def redact(text: str, findings: list[Finding] | None = None) -> str:
    """Replace findings with typed placeholders.

    Typed rather than a uniform blob (``[EMAIL]``, not ``***``) so a redacted
    task still reads as a task: an eval input where the shape of the removed
    value is visible remains a usable test case, while one full of identical
    blanks does not.
    """
    if not text:
        return text
    found = findings if findings is not None else detect(text)
    result = text
    for finding in sorted(found, key=lambda f: f.start, reverse=True):
        result = f"{result[: finding.start]}[{finding.kind}]{result[finding.end :]}"
    return result


def scan(*texts: str | None) -> list[Finding]:
    """Findings across several strings, for callers holding a whole transcript.

    rather than one message.
    """
    findings: list[Finding] = []
    for text in texts:
        if text:
            findings.extend(detect(text))
    return findings
