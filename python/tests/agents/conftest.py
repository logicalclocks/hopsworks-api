"""Which of these tests need the `agents` extra installed.

Serving an agent needs FastAPI, and the matrix jobs that pin old optional
dependencies (`dev-pandas1`) install the package without that extra. The four
modules below import `AgentApp` at module level, so without it they fail to
collect -- and a collection error aborts the whole run, taking every other test
in `python/tests` with it.

Ignored by name rather than skipped inside each module: the eval tests next to
them need none of this, and listing the files here keeps the import blocks in
the test modules as ruff wants them.
"""

NEEDS_FASTAPI = [
    "test_app.py",
    "test_evaluation.py",
    "test_identity.py",
    "test_memory_config.py",
]

collect_ignore = []

try:  # noqa: SIM105 -- the import is the check
    import fastapi  # noqa: F401
except ImportError:
    collect_ignore = list(NEEDS_FASTAPI)
