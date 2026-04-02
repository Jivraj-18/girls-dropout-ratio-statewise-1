from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class CorrResult:
    n: int
    corr: float
    p_perm: float | None


def _is_finite(x: float | None) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))


def pearson_corr(x: Iterable[float | None], y: Iterable[float | None]) -> float:
    xs: list[float] = []
    ys: list[float] = []
    for a, b in zip(x, y):
        if _is_finite(a) and _is_finite(b):
            xs.append(float(a))
            ys.append(float(b))

    n = len(xs)
    if n < 2:
        return float("nan")

    mx = sum(xs) / n
    my = sum(ys) / n

    sxx = sum((v - mx) ** 2 for v in xs)
    syy = sum((v - my) ** 2 for v in ys)
    if sxx == 0.0 or syy == 0.0:
        return float("nan")

    sxy = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    return float(sxy / math.sqrt(sxx * syy))


def permutation_pvalue_for_corr(
    x: list[float],
    y: list[float],
    n_perm: int = 5000,
    seed: int = 42,
) -> float | None:
    n = len(x)
    if n != len(y) or n < 5:
        return None

    observed = pearson_corr(x, y)
    if not math.isfinite(observed):
        return None

    rng = random.Random(seed)

    y_work = list(y)
    more_extreme = 0
    for _ in range(n_perm):
        rng.shuffle(y_work)
        c = pearson_corr(x, y_work)
        if not math.isfinite(c):
            continue
        if abs(c) >= abs(observed):
            more_extreme += 1

    return float((more_extreme + 1) / (n_perm + 1))


def corr_with_perm_test(
    x: Iterable[float | None],
    y: Iterable[float | None],
    n_perm: int = 5000,
    seed: int = 42,
) -> CorrResult:
    xs: list[float] = []
    ys: list[float] = []
    for a, b in zip(x, y):
        if _is_finite(a) and _is_finite(b):
            xs.append(float(a))
            ys.append(float(b))

    corr = pearson_corr(xs, ys)
    p = permutation_pvalue_for_corr(xs, ys, n_perm=n_perm, seed=seed)
    return CorrResult(n=len(xs), corr=float(corr), p_perm=p)
