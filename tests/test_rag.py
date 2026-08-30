"""RAG index/retriever tests. Real coverage lands in Milestone 3 / Milestone 6."""

import importlib


def test_rag_modules_import():
    builder = importlib.import_module("src.rag.index_builder")
    retriever = importlib.import_module("src.rag.retriever")
    assert hasattr(builder, "build_index")
    assert hasattr(retriever, "retrieve")
