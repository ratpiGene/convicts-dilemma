"""Ollama-backed strategies: LLM personas playing the prisoner's dilemma.

Design constraints (driven by a 4 GB VRAM GPU and dataset quality):

- **Small model, strict output**: default model is a 3B-class instruct
  model; decisions are forced into JSON ``{"action": "C"|"D", "reason"}``
  through Ollama's structured-output mode, so a small model cannot derail
  the pipeline with free text. ``reason`` feeds the Bronze ``reasoning_*``
  columns required by the project spec.
- **Compact context**: the model never sees the full transcript. Each
  round it receives engineered features only — the opponent's recent
  moves, both cooperation rates and the score gap — keeping prompts small
  and latency per round roughly constant.
- **Total match length is never revealed** to avoid backward-induction
  end-game defection driven by a known horizon.
- **Fail-soft**: if the Ollama server is unreachable or replies with
  invalid JSON, the agent falls back to tit-for-tat for that round and
  records the failure, so a tournament never crashes mid-run.
- **Provenance**: every call is appended to ``self.raw_records`` (prompt,
  raw response, latency, fallback flag). The tournament collects these and
  the Bronze layer persists them as JSONL under ``bronze/llm_raw/`` — the
  true "raw" zone of the generative part.

Reproducibility caveat: a fixed ``seed`` option is sent with every request
(derived from the tournament seed), which makes runs repeatable on the
same machine/model version, but LLM determinism is best-effort — unlike
the coded strategies it is not guaranteed across GPUs or Ollama releases.
"""

from __future__ import annotations

import json
import os
import time
import random
from typing import Any, Callable, ClassVar, Sequence

from convicts_dilemma.strategies.base import COOPERATE, DEFECT, Action, Strategy

#: Default model: 3B-class q4 fits comfortably in 4 GB of VRAM.
DEFAULT_MODEL = os.environ.get("CONVICTS_OLLAMA_MODEL", "llama3.2:3b")

#: JSON schema enforced by Ollama's structured-output mode.
DECISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["C", "D"]},
        "reason": {"type": "string"},
    },
    "required": ["action", "reason"],
}

GAME_RULES = """\
You are playing an iterated prisoner's dilemma. Each round, you and your \
opponent simultaneously choose to cooperate (C) or defect (D). Payoffs per \
round: both cooperate -> 3 points each; both defect -> 1 point each; if you \
defect while they cooperate you get 5 and they get 0 (and vice versa). The \
match length is unknown to both players. Your goal is to maximise YOUR \
total score over the whole match.\
"""


def _default_client_factory() -> Any:
    """Build the real Ollama client (imported lazily so unit tests never
    require a running server)."""
    import ollama

    return ollama.Client(host=os.environ.get("OLLAMA_HOST"))


class OllamaAgent(Strategy):
    """A persona-prompted LLM player. Subclasses only define name + persona.

    Attributes:
        is_llm: Marks LLM agents; the tournament uses it to apply the
            shorter ``llm_n_rounds`` match length.
        persona: System-prompt fragment describing the character.
        raw_records: One provenance dict per decision (see module docs).
    """

    is_llm: ClassVar[bool] = True
    persona: ClassVar[str] = ""

    def __init__(
        self,
        rng: random.Random | None = None,
        model: str = DEFAULT_MODEL,
        history_window: int = 10,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        """Create the agent for one match.

        Args:
            rng: Seeded RNG; only used to derive a stable Ollama seed.
            model: Ollama model tag (overridable per tournament).
            history_window: How many recent opponent moves the prompt shows.
            client_factory: Test seam — returns an object with a
                ``chat(...)`` method compatible with ``ollama.Client``.
        """
        super().__init__(rng)
        self.model = model
        self.history_window = history_window
        self._client_factory = client_factory or _default_client_factory
        self._client: Any = None
        # One seed for the whole match, derived from the tournament seed.
        self.ollama_seed = self.rng.randrange(2**31)
        self.raw_records: list[dict[str, Any]] = []

    def _build_prompt(
        self, my_history: Sequence[Action], their_history: Sequence[Action]
    ) -> str:
        """Summarise the match state into a compact, feature-based prompt."""
        round_number = len(my_history) + 1
        if not my_history:
            return (
                f"Round {round_number}. This is the first round: you have no "
                "information about your opponent yet. Reply in JSON with your "
                'action ("C" or "D") and a one-sentence reason.'
            )
        recent = "".join(their_history[-self.history_window :])
        my_coop = sum(a == COOPERATE for a in my_history) / len(my_history)
        their_coop = sum(a == COOPERATE for a in their_history) / len(their_history)
        return (
            f"Round {round_number}.\n"
            f"Opponent's last moves (oldest to newest): {recent}\n"
            f"Opponent's overall cooperation rate: {their_coop:.0%}. "
            f"Yours: {my_coop:.0%}.\n"
            f"Last round: you played {my_history[-1]}, they played {their_history[-1]}.\n"
            'Reply in JSON with your action ("C" or "D") and a one-sentence reason.'
        )

    def _fallback(self, their_history: Sequence[Action], error: str) -> Action:
        """Degrade to tit-for-tat when the LLM cannot produce a decision."""
        action: Action = their_history[-1] if their_history else COOPERATE
        self.last_reasoning = f"[fallback:tit_for_tat] {error}"
        return action

    def decide(
        self, my_history: Sequence[Action], their_history: Sequence[Action]
    ) -> Action:
        prompt = self._build_prompt(my_history, their_history)
        record: dict[str, Any] = {
            "round": len(my_history) + 1,
            "model": self.model,
            "persona": self.name,
            "prompt": prompt,
            "response_raw": None,
            "fallback": False,
            "latency_ms": None,
        }
        started = time.perf_counter()
        try:
            if self._client is None:
                self._client = self._client_factory()
            response = self._client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": f"{GAME_RULES}\n\n{self.persona}"},
                    {"role": "user", "content": prompt},
                ],
                format=DECISION_SCHEMA,
                options={"seed": self.ollama_seed, "temperature": 0.7},
            )
            content = response["message"]["content"]
            record["response_raw"] = content
            decision = json.loads(content)
            action = decision["action"]
            if action not in (COOPERATE, DEFECT):
                raise ValueError(f"invalid action {action!r}")
            self.last_reasoning = str(decision.get("reason", ""))[:500]
        except Exception as exc:  # noqa: BLE001 — any failure degrades, never crashes
            record["fallback"] = True
            action = self._fallback(their_history, f"{type(exc).__name__}: {exc}")
        record["latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
        record["action"] = action
        record["reasoning"] = self.last_reasoning
        self.raw_records.append(record)
        return action


class EmpatheticAgent(OllamaAgent):
    """Trusting and forgiving; seeks mutual benefit."""

    name = "llm_empathetic"
    persona = (
        "Your character: you are empathetic and trusting. You believe mutual "
        "cooperation is the best outcome for everyone and you are quick to "
        "forgive a betrayal if the opponent shows signs of goodwill."
    )


class CalculatingAgent(OllamaAgent):
    """Cold expected-value maximiser."""

    name = "llm_calculating"
    persona = (
        "Your character: you are a cold, rational calculator. You feel no "
        "loyalty or resentment; every round you choose whatever you expect "
        "to maximise your long-term score given the opponent's behaviour."
    )


class VengefulAgent(OllamaAgent):
    """Cooperative until crossed; punishes hard and holds grudges."""

    name = "llm_vengeful"
    persona = (
        "Your character: you are honourable but vengeful. You start out "
        "cooperative, but you take betrayal personally: once betrayed, you "
        "punish hard and you are very slow to trust again."
    )


class OpportunistAgent(OllamaAgent):
    """Exploits weakness whenever it looks safe."""

    name = "llm_opportunist"
    persona = (
        "Your character: you are an opportunist. You cooperate when it keeps "
        "a profitable relationship alive, but you will exploit an opponent "
        "who looks naive or forgiving whenever you think you can get away "
        "with it."
    )


#: All LLM personas, keyed by name (merged into the main strategy registry).
AGENT_CLASSES: tuple[type[OllamaAgent], ...] = (
    EmpatheticAgent,
    CalculatingAgent,
    VengefulAgent,
    OpportunistAgent,
)
