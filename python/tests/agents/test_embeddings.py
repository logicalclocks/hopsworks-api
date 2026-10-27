"""Embedding models come from the model registry, and reach the hub only as a fallback."""

import sys
import types
from unittest import mock

import pytest


class FakeModel:
    def __init__(self, name, version, path="/cache/model"):
        self.name, self.version, self.path = name, version, path
        self.downloaded = 0

    def download(self):
        self.downloaded += 1
        return self.path


class FakeRegistry:
    """The slice of ModelRegistry the module touches."""

    def __init__(self, models=()):
        self.models = list(models)
        self.saved = []
        self.python = types.SimpleNamespace(create_model=self._create)

    def get_models(self, name):
        return [m for m in self.models if m.name == name]

    def get_model(self, name, version):
        return next(
            (m for m in self.models if m.name == name and m.version == version), None
        )

    def _create(self, name, description=None):
        registry = self

        class Unsaved:
            def save(self, path):
                saved = FakeModel(name, len(registry.get_models(name)) + 1)
                saved.description, saved.saved_from = description, path
                registry.models.append(saved)
                registry.saved.append(saved)
                return saved

        return Unsaved()


@pytest.fixture
def fake_st(monkeypatch):
    """A sentence_transformers module recording what it was asked to load or save."""
    loaded, saved = [], []

    class SentenceTransformer:
        def __init__(self, name_or_path):
            loaded.append(name_or_path)
            self.name = name_or_path

        def save(self, path):
            saved.append((self.name, path))

    module = types.ModuleType("sentence_transformers")
    module.SentenceTransformer = SentenceTransformer
    monkeypatch.setitem(sys.modules, "sentence_transformers", module)
    return loaded, saved


def _project(registry):
    return types.SimpleNamespace(get_model_registry=lambda: registry)


def test_registry_name_follows_the_backend_rule():
    from hopsworks_agents.protocol.embeddings import registry_name

    assert registry_name("all-MiniLM-L6-v2") == "all_MiniLM_L6_v2"
    assert registry_name("BAAI/bge-small-en-v1.5") == "BAAI_bge_small_en_v1_5"


class TestRegister:
    def test_downloads_once_and_saves_to_the_registry(self, fake_st):
        from hopsworks_agents.protocol.embeddings import register_sentence_transformer

        loaded, saved = fake_st
        registry = FakeRegistry()
        model = register_sentence_transformer(
            "all-MiniLM-L6-v2", project=_project(registry)
        )

        assert loaded == ["all-MiniLM-L6-v2"]
        assert model.name == "all_MiniLM_L6_v2" and model.version == 1
        assert saved[0][1] == model.saved_from  # the registry got what the hub gave
        assert "all-MiniLM-L6-v2" in model.description

    def test_is_idempotent_unless_forced(self, fake_st):
        from hopsworks_agents.protocol.embeddings import register_sentence_transformer

        loaded, _ = fake_st
        registry = FakeRegistry(
            [FakeModel("all_MiniLM_L6_v2", 1), FakeModel("all_MiniLM_L6_v2", 2)]
        )
        project = _project(registry)

        again = register_sentence_transformer(project=project)
        assert again.version == 2 and loaded == []  # nothing downloaded

        forced = register_sentence_transformer(project=project, force=True)
        assert forced.version == 3 and loaded == ["all-MiniLM-L6-v2"]

    def test_logs_in_when_no_project_is_given(self, fake_st, monkeypatch):
        from hopsworks_agents.protocol.embeddings import register_sentence_transformer

        registry = FakeRegistry([FakeModel("custom", 1)])
        hopsworks = types.ModuleType("hopsworks")
        hopsworks.login = lambda: _project(registry)
        monkeypatch.setitem(sys.modules, "hopsworks", hopsworks)
        assert register_sentence_transformer(name="custom").version == 1


class TestLoad:
    def test_loads_the_highest_registered_version_from_its_download(self, fake_st):
        from hopsworks_agents.protocol.embeddings import load_sentence_transformer

        loaded, _ = fake_st
        v2 = FakeModel("all_MiniLM_L6_v2", 2, path="/cache/v2")
        registry = FakeRegistry([FakeModel("all_MiniLM_L6_v2", 1), v2])
        model = load_sentence_transformer(project=_project(registry))

        assert model.name == "/cache/v2" and v2.downloaded == 1
        assert loaded == ["/cache/v2"]  # never the hub name

    def test_a_pinned_version_is_honoured(self, fake_st):
        from hopsworks_agents.protocol.embeddings import load_sentence_transformer

        registry = FakeRegistry(
            [
                FakeModel("all_MiniLM_L6_v2", 1, "/v1"),
                FakeModel("all_MiniLM_L6_v2", 2, "/v2"),
            ]
        )
        assert (
            load_sentence_transformer(version=1, project=_project(registry)).name
            == "/v1"
        )

    def test_falls_back_to_the_hub_when_unregistered(self, fake_st, caplog):
        from hopsworks_agents.protocol.embeddings import load_sentence_transformer

        loaded, _ = fake_st
        with caplog.at_level("WARNING"):
            model = load_sentence_transformer(project=_project(FakeRegistry()))
        assert model.name == "all-MiniLM-L6-v2" and loaded == ["all-MiniLM-L6-v2"]
        assert "register_sentence_transformer" in caplog.text

    def test_falls_back_when_the_registry_is_unreachable(self, fake_st):
        from hopsworks_agents.protocol.embeddings import load_sentence_transformer

        broken = types.SimpleNamespace(
            get_model_registry=mock.Mock(side_effect=ConnectionError("down"))
        )
        assert load_sentence_transformer(project=broken).name == "all-MiniLM-L6-v2"

    def test_no_fallback_means_an_error(self, fake_st):
        from hopsworks_agents.protocol.embeddings import load_sentence_transformer

        with pytest.raises(LookupError, match="register_sentence_transformer"):
            load_sentence_transformer(fallback=False, project=_project(FakeRegistry()))
        broken = types.SimpleNamespace(
            get_model_registry=mock.Mock(side_effect=ConnectionError("down"))
        )
        with pytest.raises(ConnectionError):
            load_sentence_transformer(fallback=False, project=broken)


def test_the_embedder_loads_through_the_registry(fake_st, monkeypatch):
    from hopsworks_agents.protocol.summarizers import sentence_transformer_embedder

    loaded, _ = fake_st
    st = sys.modules["sentence_transformers"].SentenceTransformer
    st.encode = lambda self, text, normalize_embeddings=True: types.SimpleNamespace(
        tolist=lambda: [0.1, 0.2]
    )
    st.get_sentence_embedding_dimension = lambda self: 2
    registry = FakeRegistry([FakeModel("all_MiniLM_L6_v2", 1, "/cache/v1")])

    embed = sentence_transformer_embedder(project=_project(registry))
    assert loaded == ["/cache/v1"]
    assert embed("hi") == [0.1, 0.2]
    assert (
        embed.dimension == 2 and embed.model_id == "all-MiniLM-L6-v2"
    )  # the hub name, so vector stores match


def test_exports():
    import hopsworks_agents.protocol as protocol

    assert callable(protocol.register_sentence_transformer)
    assert callable(protocol.load_sentence_transformer)
