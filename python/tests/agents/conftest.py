"""Which of these tests need the `agents` extra installed.

Serving an agent needs FastAPI, and the matrix jobs that pin old optional
dependencies (`dev-pandas1`) install the package without that extra. The
modules below import `AgentApp` at module level, so without it they fail to
collect -- and a collection error aborts the whole run, taking every other test
in `python/tests` with it.

Ignored by name rather than skipped inside each module: the eval tests next to
them need none of this, and listing the files here keeps the import blocks in
the test modules as ruff wants them.
"""

import pytest


NEEDS_FASTAPI = [
    "test_a2a.py",
    "test_a2a_card_and_artifacts.py",
    "test_a2a_durable.py",
    "test_a2a_push.py",
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


@pytest.fixture
def skill_catalogue(monkeypatch, tmp_path):
    from hopsworks import _skill_catalogue

    catalogue = tmp_path / "package"
    entries = {
        "data/orders": "[Guide](references/guide.md)",
        "agents/jobs": "[Orders](../../data/orders/SKILL.md)",
        "agents/unused": "Unrelated playbook",
    }
    for folder, body in entries.items():
        root = catalogue / folder
        root.mkdir(parents=True)
        (root / "SKILL.md").write_text(
            f"---\nname: {root.name}\ndescription: About {root.name}\n"
            f"allowed-tools:\n  - Read\n  - Bash\n---\n{body}\n",
            encoding="utf-8",
        )
    refs = catalogue / "data/orders/references"
    refs.mkdir()
    (refs / "guide.md").write_text(
        "[Details](../../../agents/jobs/details.md#usage)\n", encoding="utf-8"
    )
    (catalogue / "agents/jobs/details.md").write_text("Job details", encoding="utf-8")
    monkeypatch.setattr(_skill_catalogue, "_packaged_skills_dir", lambda: catalogue)
    return catalogue
