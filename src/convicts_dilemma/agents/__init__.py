"""LLM agents (Ollama). Same Strategy interface as the coded roster."""

from convicts_dilemma.agents.ollama_agent import (
    AGENT_CLASSES,
    DEFAULT_MODEL,
    OllamaAgent,
)

__all__ = ["AGENT_CLASSES", "DEFAULT_MODEL", "OllamaAgent"]
