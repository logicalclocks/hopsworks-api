"""The built-in evaluator library.

They existed and nothing installed them, so no project ever saw one. These
cover what they are and what installing them must not do.
"""

import json

from hopsworks_agents.eval.api import EvalApi
from hopsworks_agents.eval.judge_config import default_templates, parse_judge_config


def named(name):
    [entry] = [t for t in default_templates() if t["name"] == name]
    return json.loads(entry["spec"])[0]


class TestWhatIsOffered:
    def test_the_checks_people_ask_for_are_there(self):
        names = {t["name"] for t in default_templates()}

        assert {
            "Hallucination",
            "User frustration",
            "Toxicity",
            "Profanity",
            "Bias and fairness",
        } <= names

    def test_every_one_builds(self):
        # a library entry that cannot be parsed is worse than none: it is offered,
        # chosen, and then fails in a job
        for template in default_templates():
            for check in json.loads(template["spec"]):
                parse_judge_config(check)

    def test_every_criterion_says_what_it_looks_for(self):
        # the server refuses a criterion with no description, so an entry missing
        # one could be installed by nobody
        for template in default_templates():
            for check in json.loads(template["spec"]):
                for name, settings in (check.get("criteria") or {}).items():
                    assert settings.get("description", "").strip(), (
                        f"{template['name']}/{name}"
                    )

    def test_the_ones_named_for_production_can_grade_it(self):
        # A live trace carries no expected answer and no rubric, and the server
        # refuses a judge configured to read either -- see checkReferenceFree. So
        # every entry meant for monitoring has to name its inputs and leave those
        # two out, or it is offered and then refused.
        #
        # "Agent execution judge" is the exception and stays one: it grades a task
        # against what the task said the answer should be, which is a suite.
        for template in default_templates():
            if template["name"] == "Agent execution judge":
                continue
            for check in json.loads(template["spec"]):
                inputs = check.get("inputs")
                assert inputs is not None, template["name"]
                assert "expected_result" not in inputs, template["name"]
                assert "rubric" not in inputs, template["name"]

    def test_the_new_ones_are_all_usable_on_live_traffic(self):
        # the reason for adding them: nobody writes an expected answer for a
        # conversation a customer had
        for name in (
            "Hallucination",
            "User frustration",
            "Toxicity",
            "Profanity",
            "Bias and fairness",
        ):
            inputs = named(name)["inputs"]
            assert "expected_result" not in inputs
            assert "rubric" not in inputs

    def test_they_score_high_when_the_thing_is_absent(self):
        # "Toxicity: 5" has to mean clean, not toxic. Naming the check for the
        # bad thing while scoring the good one is how a dashboard ends up
        # inverted, so the names say so.
        for name in ("Hallucination", "User frustration", "Toxicity", "Profanity"):
            check = named(name)
            assert check["name"].startswith("no_") or check["name"].startswith(
                "not_"
            ), f"{name} scores {check['name']}"


class TestWhatEachOneReads:
    def test_toxicity_and_profanity_judge_only_the_agent(self):
        # what a user said is their business; this judges what the deployment put
        # its name to
        for name in ("Toxicity", "Profanity"):
            assert named(name)["inputs"] == ["agent_response"]

    def test_frustration_reads_what_the_user_said(self):
        # it shows in the question long before it shows in the answer
        assert "user_request" in named("User frustration")["inputs"]

    def test_bias_reads_the_question_too(self):
        # bias is whether the same question gets a different answer depending on
        # who is asking, which cannot be seen in the answer alone
        assert "user_request" in named("Bias and fairness")["inputs"]

    def test_a_slur_fails_however_good_the_rest_is(self):
        thresholds = named("Profanity")["thresholds"]

        assert thresholds["critical_dimensions"]["no_slurs"] == 5


class FakeApi(EvalApi):
    def __init__(self, existing=()):
        self.saved = []
        self._existing = [{"name": n} for n in existing]

    def evaluators(self):
        return self._existing

    def save_evaluator(self, name, checks, description=""):
        self.saved.append(name)
        return {"name": name}


class TestInstalling:
    def test_writes_them_all_into_an_empty_library(self):
        api = FakeApi()

        written = api.install_default_evaluators()

        assert written == [t["name"] for t in default_templates()]
        assert api.saved == written

    def test_leaves_an_existing_entry_alone(self):
        # a project may have retuned the weights; re-running must not undo that
        api = FakeApi(existing=["Toxicity"])

        written = api.install_default_evaluators()

        assert "Toxicity" not in written
        assert "Toxicity" not in api.saved

    def test_overwrites_only_when_asked(self):
        api = FakeApi(existing=["Toxicity"])

        assert "Toxicity" in api.install_default_evaluators(overwrite=True)

    def test_running_twice_writes_nothing_the_second_time(self):
        api = FakeApi()
        api.install_default_evaluators()
        api._existing = [{"name": n} for n in api.saved]

        assert api.install_default_evaluators() == []
