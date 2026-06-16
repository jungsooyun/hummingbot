"""Regression guard for the executor import path.

The v2.14 upstream merge removed ``hummingbot.strategy.script_strategy_base``.
Three locally-ported executors (ladder_maker / xemm / inventory_rebalance) still
imported ``ScriptStrategyBase`` from that dead path. Because
``executor_orchestrator`` eagerly imports every executor at module load, a single
dead import broke the orchestrator import and therefore EVERY V2 strategy.

This test is intentionally pure-stdlib (no hummingbot import) so it runs in the
lean connector test environment too. It scans the executor source tree for the
removed module path.
"""
import pathlib

# <root>/test/hummingbot/strategy_v2/executors/<this file>
#   parents[4] == <root>; package lives at <root>/hummingbot/strategy_v2/executors
_ROOT = pathlib.Path(__file__).resolve().parents[4]
_EXECUTORS_DIR = _ROOT / "hummingbot" / "strategy_v2" / "executors"

DEAD_IMPORT = "hummingbot.strategy.script_strategy_base"


def test_no_executor_imports_removed_script_strategy_base():
    assert _EXECUTORS_DIR.is_dir(), f"executors dir not found: {_EXECUTORS_DIR}"
    offenders = [
        str(p.relative_to(_ROOT))
        for p in _EXECUTORS_DIR.rglob("*.py")
        if DEAD_IMPORT in p.read_text(encoding="utf-8")
    ]
    assert not offenders, (
        "Executor(s) import the removed module "
        f"'{DEAD_IMPORT}'; use hummingbot.strategy.strategy_v2_base.StrategyV2Base "
        f"instead: {offenders}"
    )
