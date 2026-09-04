"""Windows compat shim - import this before ``chromadb`` anywhere.

chromadb 0.5.x evaluates its default (ONNX-based) embedding function as a
class-level default argument at ``import chromadb`` time, which needs
``onnxruntime`` to be importable. But onnxruntime's native extension is
unreliable in this process on Windows once other native libraries
(pandas/pyarrow, and apparently more so with pyspark also loaded) are already
present - confirmed two distinct failure modes: a clean "DLL load failed"
ImportError in isolation, and a full Windows access violation (native crash,
not a catchable Python exception) when more of the project's other native
deps are already loaded in the same process (e.g. running the full test
suite). A crash can't be reliably caught with try/except, so this doesn't
attempt the real import at all - unconditionally stubs it.

This project always supplies its own (Ollama) embeddings explicitly and
never calls chromadb's default embedding function, so the real onnxruntime
is never actually needed - only *importable*. A dummy module is enough:
``ONNXMiniLM_L6_V2.__init__`` only does ``importlib.import_module("onnxruntime")``
and stores the result; nothing else touches it unless the function is
actually called, which we never do.
"""

from __future__ import annotations

import sys
import types

if "onnxruntime" not in sys.modules:
    sys.modules["onnxruntime"] = types.ModuleType("onnxruntime")
