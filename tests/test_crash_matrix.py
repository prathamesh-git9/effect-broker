from __future__ import annotations

from pathlib import Path

from examples.crash_matrix.runner import run_matrix

from effect_broker.models import EffectStatus, SafetyClass


def test_idempotent_crash_matrix_uses_real_killed_subprocess(tmp_path: Path) -> None:
    results = run_matrix(tmp_path)
    idempotent = next(
        result for result in results if result.scenario == SafetyClass.IDEMPOTENT.value
    )

    assert idempotent.passed
    assert idempotent.target_count == 1
    assert idempotent.broker_status is EffectStatus.SUCCEEDED
