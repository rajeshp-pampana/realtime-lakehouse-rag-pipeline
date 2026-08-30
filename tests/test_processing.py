"""Batch transform tests. Real coverage lands in Milestone 1 / Milestone 6."""

import importlib


def test_transform_module_imports():
    mod = importlib.import_module("src.processing.transform_spark")
    assert hasattr(mod, "run_transform")
