"""ApexBase usage-scenario examples -- shared helper module.

This module is **not** a runnable scenario. It provides the utilities that the
`pyNN_*.py` scenarios in this directory reuse: working-directory management,
reproducible randomness, and deterministic vector generation.

Design principles:
1. **Zero external dependencies**: only the standard library, numpy and
   apexbase are used. The examples never download data over the network.
2. **Repeatable runs**: every scenario owns an isolated output directory that is
   wiped and recreated at start-up, so running an example twice never fails with
   a "TableExists"-style error caused by leftover state.
3. **Determinism**: all random data uses a fixed seed, which keeps example
   output stable and comparable between runs.

When you run any scenario, Python adds the script's directory to `sys.path`, so
the scenario files can simply `from _demo_env import ...` without installing
anything or setting PYTHONPATH.
"""

from __future__ import annotations

import math
import random
import shutil
from pathlib import Path
from typing import Iterable, List, Sequence

# Scenario output directories all live under `_out/` next to this file, which
# keeps them easy to clean up and easy to gitignore as a group.
_OUT_ROOT = Path(__file__).resolve().parent / "_out"


def work_dir(slug: str) -> str:
    """Return a clean, isolated, writable working directory (as a string path).

    Every call deletes the old directory and recreates it, which is what makes a
    scenario safely re-runnable.

    Args:
        slug: Scenario identifier, e.g. ``"01"`` or ``"hybrid_rag"``. Used as the
            subdirectory name.

    Returns:
        The absolute path string owned exclusively by that scenario.
    """
    path = _OUT_ROOT / f"py_{slug}"
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def rng(seed: int = 42) -> random.Random:
    """Return an independent fixed-seed RNG (it does not touch global random state)."""
    return random.Random(seed)


def unit_vector(dim: int, rnd: random.Random) -> List[float]:
    """Build a ``dim``-dimensional unit vector (L2 norm is 1).

    Unit vectors are the usual convention in embedding retrieval: cosine distance
    and L2 distance are then monotonically equivalent, which lets an example
    cross-check the two metrics against each other.

    Args:
        dim: Vector dimension.
        rnd: Random number generator, used to stay reproducible.

    Returns:
        A Python float list of length ``dim``.
    """
    values = [rnd.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(v * v for v in values)) or 1.0
    return [v / norm for v in values]


def make_embeddings(
    count: int,
    dim: int,
    rnd: random.Random,
    cluster_centers: Sequence[Sequence[float]] | None = None,
    spread: float = 0.25,
) -> List[List[float]]:
    """Generate a batch of deterministic embedding vectors.

    When ``cluster_centers`` is given, each vector is a small perturbation around
    one of those centers, forming "semantic clusters". That makes the TopK results
    of RAG / retrieval examples interpretable instead of pure random noise.

    Args:
        count: How many vectors to generate.
        dim: Vector dimension.
        rnd: Random number generator.
        cluster_centers: Optional list of cluster centers, each a ``dim``-length vector.
        spread: Perturbation strength; larger values make clusters looser.

    Returns:
        A list of vectors shaped like ``[[float, ...], ...]``.
    """
    if cluster_centers is None:
        return [unit_vector(dim, rnd) for _ in range(count)]

    centers = [[float(x) for x in c] for c in cluster_centers]
    out: List[List[float]] = []
    for i in range(count):
        center = centers[i % len(centers)]
        vec = [c + rnd.gauss(0.0, spread) for c in center]
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        out.append([v / norm for v in vec])
    return out


def vector_literal(vec: Iterable[float], precision: int = 6) -> str:
    """Render a vector as an array literal ``[0.1,0.2,...]`` that can be inlined into SQL.

    Why this helper exists: ApexBase's vector distance functions
    (``cosine_distance`` / ``array_distance``) require an **array literal**
    argument. Binding a list through a ``?`` placeholder expands it into several
    scalar arguments instead (which fails with "requires exactly 2 arguments"),
    so the literal is built explicitly here.

    Args:
        vec: The vector values.
        precision: Number of decimal places, which shortens the SQL text.

    Returns:
        A string shaped like ``"[0.123456,0.654321]"``.
    """
    return "[" + ",".join(f"{float(x):.{precision}f}" for x in vec) + "]"


def section(title: str) -> None:
    """Print a section header in a consistent style so example output stays readable."""
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def show(label: str, value: object) -> None:
    """Print one ``label: value`` line, truncating oversized containers to keep output tidy."""
    text = repr(value)
    if len(text) > 300:
        text = text[:297] + "..."
    print(f"[{label}] {text}")


def assert_close(actual: float, expected: float, tol: float, label: str) -> None:
    """Assert two floats are equal within a tolerance; otherwise raise AssertionError.

    The examples use this as a self-check so readers can confirm the result really
    matches expectations instead of only reading printed numbers.
    """
    if abs(actual - expected) > tol:
        raise AssertionError(f"{label}: expected {expected}, got {actual} (tolerance {tol})")
    print(f"[OK] {label}: {actual} ~= {expected}")
