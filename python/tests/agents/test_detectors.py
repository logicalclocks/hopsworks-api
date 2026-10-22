"""Detector recall, and the false positives that would destroy it.

Recall is what matters: a detector that quietly misses is more dangerous than
none at all, because the reviewer starts trusting it. But precision matters for
a second-order reason — flag every long number in a transcript and reviewers
learn to skim past the warnings, which costs recall in practice.

Every secret below is fabricated.
"""

import pytest
from hopsworks_agents.protocol import detectors


def kinds(text: str) -> set[str]:
    return {finding.kind for finding in detectors.detect(text)}


class TestRecall:
    @pytest.mark.parametrize(
        "kind,text",
        [
            ("EMAIL", "write to enrique.munoz@example.com about it"),
            ("AWS_ACCESS_KEY", "key AKIAIOSFODNN7EXAMPLE here"),
            ("GITHUB_TOKEN", "ghp_1234567890abcdefghijklmnopqrstuvwx"),
            # assembled rather than written out: a literal of this shape
            # trips GitHub push protection, and a test fixture is not
            # worth teaching a repository to ignore real Slack tokens
            ("SLACK_TOKEN", "xoxb-" + "1" * 12 + "-" + "a" * 16),
            ("OPENAI_KEY", "sk-abcdefghijklmnopqrstuvwxyz012345"),
            ("US_SSN", "ssn 123-45-6789 on file"),
            ("IP_ADDRESS", "connect to 10.114.123.120 now"),
            (
                "JWT",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
                "eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r_wW1g",
            ),
            ("BEARER_TOKEN", "Authorization: Bearer abcdefghijklmnopqrstuvwxyz123456"),
            ("SECRET_ASSIGNMENT", 'api_key: "hunter2hunter2"'),
        ],
    )
    def test_finds_known_shapes(self, kind, text):
        assert kind in kinds(text), f"{kind} was missed in {text!r}"

    def test_finds_a_private_key_block(self):
        text = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "MIIEowIBAAKCAQEAxyz\n"
            "-----END RSA PRIVATE KEY-----"
        )
        assert "PRIVATE_KEY" in kinds(text)

    def test_finds_a_valid_card_number(self):
        # a Luhn-valid test number
        assert "CREDIT_CARD" in kinds("card 4111 1111 1111 1111 charged")

    def test_finds_several_things_in_one_transcript(self):
        text = (
            "Customer enrique.munoz@example.com, card 4111111111111111, "
            "called from 10.0.0.5"
        )
        assert {"EMAIL", "CREDIT_CARD", "IP_ADDRESS"} <= kinds(text)


class TestPrecision:
    def test_a_long_number_that_is_not_a_card_is_not_flagged(self):
        # order ids and timestamps are long digit runs; flagging them trains
        # reviewers to ignore the warnings, which costs recall where it counts
        assert "CREDIT_CARD" not in kinds("order 1234567890123456 shipped")

    def test_ordinary_prose_is_clean(self):
        text = "Could you remind me which albums I was interested in?"
        assert detectors.detect(text) == []

    def test_a_version_number_is_not_an_ip(self):
        assert "IP_ADDRESS" not in kinds("upgraded to 5.1.0")

    def test_overlapping_matches_are_reported_once(self):
        # a private key block contains things that look like other secrets;
        # reporting each one separately buries the finding that matters
        text = (
            "-----BEGIN PRIVATE KEY-----\nsk-abcdefghijklmnopqrstuvwxyz012345\n"
            "-----END PRIVATE KEY-----"
        )
        found = detectors.detect(text)
        assert [f.kind for f in found] == ["PRIVATE_KEY"]


class TestRedaction:
    def test_replaces_with_a_typed_placeholder(self):
        # typed, so a redacted task still reads as a task
        assert detectors.redact("mail me at a@b.com") == "mail me at [EMAIL]"

    def test_redacts_every_finding(self):
        text = "a@b.com and c@d.com"
        assert detectors.redact(text) == "[EMAIL] and [EMAIL]"

    def test_leaves_clean_text_untouched(self):
        text = "which albums did I like?"
        assert detectors.redact(text) == text

    def test_multiple_kinds_keep_their_positions(self):
        text = "user a@b.com from 10.0.0.5"
        assert detectors.redact(text) == "user [EMAIL] from [IP_ADDRESS]"

    def test_preview_does_not_leak_the_secret(self):
        # a review UI shows findings; showing the value would re-expose exactly
        # what is being removed
        finding = detectors.detect("sk-abcdefghijklmnopqrstuvwxyz012345")[0]
        assert "abcdefghijklmnopqrstuvwxyz" not in finding.preview
        assert len(finding.preview) < 10

    def test_scan_covers_several_fields(self):
        found = detectors.scan("a@b.com", None, "10.0.0.5", "")
        assert {f.kind for f in found} == {"EMAIL", "IP_ADDRESS"}
