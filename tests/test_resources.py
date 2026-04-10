from dagster import build_init_resource_context

from MaximaDagster import resources


class _FakeClient:
    def __init__(self, apiUrl: str):
        self.apiUrl = apiUrl
        self.auth_calls = []

    def authenticate(self, apiKey: str) -> None:
        self.auth_calls.append(apiKey)


def test_girder_client_resource_authenticates_with_config(monkeypatch) -> None:
    monkeypatch.setattr(resources, "GC", _FakeClient)
    context = build_init_resource_context(
        config={"api_url": "https://girder.example/api/v1", "api_key": "secret"}
    )

    client = resources.GirderClient(context)

    assert isinstance(client, _FakeClient)
    assert client.apiUrl == "https://girder.example/api/v1"
    assert client.auth_calls == ["secret"]
