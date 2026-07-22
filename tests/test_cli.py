from __future__ import annotations

import json

from typer.testing import CliRunner

from effect_broker.cli import _BROKER_CACHE, app

runner = CliRunner()


def _write_contracts(tmp_path) -> str:
    path = tmp_path / "contracts.yaml"
    path.write_text(
        """
tools:
  charge:
    version: v1
    safety: idempotent
    retry_limit: 3
    key_retention_seconds: 3600
  unsafe_charge:
    version: v1
    safety: unsafe
    retry_limit: 0
""",
        encoding="utf-8",
    )
    return str(path)


def _env(tmp_path) -> dict[str, str]:
    return {
        "EFFECT_BROKER_STORE": "memory",
        "EFFECT_BROKER_CONTRACTS_PATH": _write_contracts(tmp_path),
        "EFFECT_BROKER_BROKER_SECRET_HEX": "00" * 32,
    }


def test_submit_inspect_and_list(tmp_path) -> None:
    _BROKER_CACHE.clear()
    env = _env(tmp_path)
    submitted = runner.invoke(
        app,
        [
            "submit",
            "--operation-key",
            "order:42:charge:v1",
            "--tool",
            "charge",
            "--arguments",
            '{"amount": 100, "currency": "usd"}',
            "--json",
        ],
        env=env,
    )
    assert submitted.exit_code == 0, submitted.output
    effect_id = json.loads(submitted.output)["effect_id"]

    inspected = runner.invoke(app, ["inspect", effect_id, "--json"], env=env)
    listed = runner.invoke(
        app,
        ["list", "--status", "prepared", "--json"],
        env=env,
    )

    assert inspected.exit_code == 0, inspected.output
    assert listed.exit_code == 0, listed.output
    assert json.loads(inspected.output)["effect_id"] == effect_id
    assert len(json.loads(listed.output)["items"]) == 1


def test_contracts_validate_good_and_bad(tmp_path) -> None:
    good = _write_contracts(tmp_path)
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
tools:
  unsafe_charge:
    version: v1
    safety: unsafe
    retry_limit: 1
""",
        encoding="utf-8",
    )

    good_result = runner.invoke(app, ["contracts", "validate", good])
    bad_result = runner.invoke(app, ["contracts", "validate", str(bad)])

    assert good_result.exit_code == 0, good_result.output
    assert bad_result.exit_code == 1


def test_doctor(tmp_path) -> None:
    _BROKER_CACHE.clear()
    result = runner.invoke(app, ["doctor", "--json"], env=_env(tmp_path))

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["status"] == "ok"


def test_cli_cannot_force_unsafe_redispatch(tmp_path) -> None:
    _BROKER_CACHE.clear()
    result = runner.invoke(
        app,
        ["reconcile", "eff_000001", "--force"],
        env=_env(tmp_path),
    )

    assert result.exit_code != 0
    assert "No such option" in result.output
