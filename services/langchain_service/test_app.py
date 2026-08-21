import json

import pytest
from fastapi.testclient import TestClient

from src import app as app_module
from src.app import app, sse_pack


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_agent_cache(monkeypatch):
    # get_agent() caches the built agent in a module-level global.
    # Reset it before each test so tests don't leak state into each other.
    monkeypatch.setattr(app_module, "_agent", None)


def test_health_endpoint_returns_ok(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_sse_pack_formats_event_as_server_sent_event():
    packed = sse_pack("token", {"text": "hello"})

    assert packed == 'event: token\ndata: {"text": "hello"}\n\n'


def test_sse_pack_serializes_non_ascii_content():
    # ensure_ascii=False must be preserved, otherwise clients get escaped unicode
    packed = sse_pack("token", {"text": "città"})

    assert "città" in packed


class FakeAgent:
    """Stand-in for the real LangGraph agent, so tests don't call a live LLM."""

    async def astream_events(self, _input, version):
        events = [
            {"event": "on_chat_model_stream", "data": {"chunk": FakeChunk("Hel")}, "name": "model"},
            {"event": "on_chat_model_stream", "data": {"chunk": FakeChunk("lo")}, "name": "model"},
            {"event": "on_tool_start", "data": {}, "name": "lookup_tool"},
            {"event": "on_tool_end", "data": {"output": "42"}, "name": "lookup_tool"},
        ]
        for event in events:
            yield event


class FakeChunk:
    def __init__(self, content):
        self.content = content


def test_chat_stream_emits_tokens_and_notifies_operator(client, monkeypatch):
    notified = {}

    async def fake_build_agent():
        return FakeAgent()

    async def fake_notify_operator(machine_id, summary):
        notified["machine_id"] = machine_id
        notified["summary"] = summary

    monkeypatch.setattr(app_module, "build_agent", fake_build_agent)
    monkeypatch.setattr(app_module, "notify_operator", fake_notify_operator)

    response = client.post(
        "/chat/stream",
        json={"message": "why did line 3 stop?", "machine_id": "washer-01"},
    )

    assert response.status_code == 200
    body = response.text
    events = [json.loads(line[len("data: "):]) for line in body.splitlines() if line.startswith("data: ")]

    assert {"state": "started"} in events
    assert {"text": "Hel"} in events
    assert {"text": "lo"} in events
    assert {"name": "lookup_tool"} in events
    assert {"ok": True} in events

    # the accumulated token text is what gets forwarded to the operator
    assert notified["machine_id"] == "washer-01"
    assert notified["summary"] == "Hello"
