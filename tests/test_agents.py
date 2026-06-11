"""Tests for the Ollama agents — all against a stubbed client, no server."""

import json
import random

import pytest

from convicts_dilemma.agents.ollama_agent import EmpatheticAgent, OllamaAgent
from convicts_dilemma.pipeline.bronze import bronze_llm_raw_dir, write_bronze
from convicts_dilemma.simulation import TournamentConfig, run_tournament
from convicts_dilemma.strategies import LLM_ROSTER, REGISTRY, create_strategy


class StubClient:
    """Mimics ollama.Client.chat with canned (or broken) responses."""

    def __init__(self, action="D", reason="canned", broken=False):
        self.payload = json.dumps({"action": action, "reason": reason})
        self.broken = broken
        self.calls: list[dict] = []

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self.broken:
            return {"message": {"content": "not json at all {"}}
        return {"message": {"content": self.payload}}


def make_agent(**stub_kwargs) -> tuple[OllamaAgent, StubClient]:
    stub = StubClient(**stub_kwargs)
    agent = EmpatheticAgent(rng=random.Random("test"), client_factory=lambda: stub)
    return agent, stub


def test_registry_exposes_llm_personas_but_not_in_default_roster():
    from convicts_dilemma.strategies import DEFAULT_ROSTER

    assert set(LLM_ROSTER) <= set(REGISTRY)
    assert len(LLM_ROSTER) == 4
    assert not (set(LLM_ROSTER) & set(DEFAULT_ROSTER))
    agent = create_strategy("llm_calculating", random.Random("x"), ollama_model="custom:tag")
    assert agent.is_llm
    assert agent.model == "custom:tag"


def test_decide_parses_structured_response_and_records_provenance():
    agent, stub = make_agent(action="D", reason="exploit the naive")
    action = agent.decide(["C"], ["C"])
    assert action == "D"
    assert agent.last_reasoning == "exploit the naive"

    [record] = agent.raw_records
    assert record["fallback"] is False
    assert record["action"] == "D"
    assert record["latency_ms"] is not None

    # The structured-output schema and the persona are actually sent.
    [call] = stub.calls
    assert call["format"]["properties"]["action"]["enum"] == ["C", "D"]
    assert "empathetic" in call["messages"][0]["content"]


def test_prompt_is_compact_features_not_full_transcript():
    agent, stub = make_agent()
    my_history = ["C"] * 30
    their_history = ["C"] * 25 + ["D"] * 5
    agent.decide(my_history, their_history)

    [call] = stub.calls
    prompt = call["messages"][1]["content"]
    # Only the last 10 opponent moves appear, plus aggregate rates.
    assert "CCCCCDDDDD" in prompt
    assert "83%" in prompt  # opponent coop rate 25/30
    assert "100%" in prompt  # own coop rate
    # Match length must never be revealed (avoids end-game defection).
    assert "30" not in prompt.replace("Round 31", "")


def test_broken_response_falls_back_to_tit_for_tat():
    agent, _ = make_agent(broken=True)
    # Opponent defected last round -> TFT fallback defects.
    action = agent.decide(["C", "C"], ["C", "D"])
    assert action == "D"
    assert agent.last_reasoning.startswith("[fallback:tit_for_tat]")
    assert agent.raw_records[0]["fallback"] is True
    # First round with no history -> fallback cooperates.
    agent2, _ = make_agent(broken=True)
    assert agent2.decide([], []) == "C"


def test_tournament_with_llm_agent_uses_llm_round_count(tmp_path, monkeypatch):
    # Every LLM agent created by the registry gets the stub client.
    monkeypatch.setattr(
        "convicts_dilemma.agents.ollama_agent._default_client_factory",
        lambda: StubClient(action="C", reason="be kind"),
    )
    config = TournamentConfig(
        strategies=("tit_for_tat", "llm_empathetic"),
        n_rounds=50,
        llm_n_rounds=7,
        seed=3,
        include_self_play=True,
    )
    result = run_tournament(config)

    by_pairing = {(m.player_a, m.player_b): m for m in result.matches}
    # Coded-only match keeps the full length; LLM matches are shortened.
    assert len(by_pairing[("tit_for_tat", "tit_for_tat")].rounds) == 50
    llm_match = by_pairing[("tit_for_tat", "llm_empathetic")]
    assert len(llm_match.rounds) == 7
    # Reasoning lands in the round records (slot b = the LLM player).
    assert all(r["reasoning_b"] == "be kind" for r in llm_match.rounds)
    assert all(r["reasoning_a"] is None for r in llm_match.rounds)

    # Bronze persists the raw provenance as JSONL.
    summary = write_bronze(result, root=tmp_path)
    assert summary["n_llm_decisions"] == 7 + 2 * 7  # vs TFT + both self-play slots
    jsonl = (
        bronze_llm_raw_dir(tmp_path)
        / f"run_id={result.run_id}"
        / f"match_id={llm_match.match_id}"
        / "decisions.jsonl"
    )
    lines = [json.loads(line) for line in jsonl.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 7
    assert {line["slot"] for line in lines} == {"b"}
    assert all(line["model"] == "llama3.2:3b" for line in lines)
    assert manifest_says_llm(result)


def manifest_says_llm(result) -> bool:
    return result.manifest["llm_n_rounds"] == 7 and result.manifest["engine_version"] == 2
