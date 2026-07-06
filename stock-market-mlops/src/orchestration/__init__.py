"""Orchestration package for retraining and workflow triggers."""

from importlib import import_module


def __getattr__(name):
	if name == "retraining_orchestrator":
		return import_module("src.orchestration.retraining_orchestrator")
	raise AttributeError(f"module 'src.orchestration' has no attribute {name!r}")
