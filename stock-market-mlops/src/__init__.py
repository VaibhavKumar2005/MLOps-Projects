"""Top-level source package."""

from importlib import import_module


def __getattr__(name):
	if name == "orchestration":
		return import_module("src.orchestration")
	raise AttributeError(f"module 'src' has no attribute {name!r}")
