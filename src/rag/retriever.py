"""Retrieval step run before every LLM call.

Milestone 3 implements this: given a query (ticker + current context), return the
top-k relevant passages from the vector store so ``src/llm/briefing_generator.py``
can ground its output in retrieved context.

Placeholder only until Milestone 3.
"""

from __future__ import annotations

TOP_K = 4


def retrieve(query: str, k: int = TOP_K):
    raise NotImplementedError("Milestone 3: retriever not implemented yet")
