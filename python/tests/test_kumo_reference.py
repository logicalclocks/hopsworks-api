"""The Kumo run reference's registration job, which ships as skill reference code."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest import mock

import pytest


REGISTER = (
    Path(__file__).resolve().parents[1]
    / "hopsworks/skills/ml/hops-reqs/references/kumo_run/register_kumo.py"
)


@pytest.fixture
def register_kumo():
    spec = importlib.util.spec_from_file_location("register_kumo", REGISTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_model_not_registered_yet_is_absent(register_kumo):
    registry = mock.Mock()
    registry.get_models.return_value = []
    assert not register_kumo.already_registered(registry, "kumo_tabular", "abc")


def test_the_registered_revision_is_found(register_kumo):
    registry = mock.Mock()
    registry.get_models.return_value = [
        mock.Mock(description="Kumo Tabular abc (small)")
    ]
    assert register_kumo.already_registered(registry, "kumo_tabular", "abc")


def test_a_failed_lookup_is_not_taken_as_absent(register_kumo):
    registry = mock.Mock()
    registry.get_models.side_effect = RuntimeError("503 from the registry")
    with pytest.raises(RuntimeError):
        register_kumo.already_registered(registry, "kumo_tabular", "abc")
