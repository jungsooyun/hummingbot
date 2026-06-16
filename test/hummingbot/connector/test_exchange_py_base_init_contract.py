"""Upstream-merge guard for the ExchangePyBase.__init__ contract.

The v2.14 upstream merge added params to ``ExchangePyBase.__init__``
(``balance_asset_limit``, ``rate_limits_share_pct``). Custom connectors must
thread every such param through their own ``super().__init__()`` call, or
construction drifts silently. This guard pins the base signature (so an upstream
change forces a deliberate re-verification + snapshot bump) and statically
asserts each custom connector forwards the base params.

Two layers, both run locally under py312:
- signature snapshot: imports ExchangePyBase (no heavy/runtime deps) and pins the
  __init__ parameter names.
- forwarding check: pure ``ast`` parse of each custom ``*_exchange.py`` (no
  import), asserting the ``super().__init__(...)`` call forwards the base params
  (positionally or by keyword) or splats ``**kwargs``.

Run with:
    cd hummingbot && PYENV_VERSION=py312 python -m pytest \
        test/hummingbot/connector/test_exchange_py_base_init_contract.py -v
"""
import ast
import inspect
import pathlib

from hummingbot.connector.exchange_py_base import ExchangePyBase

# <root>/test/hummingbot/connector/<this file>; parents[3] == <root>
_ROOT = pathlib.Path(__file__).resolve().parents[3]
_PKG = _ROOT / "hummingbot"

# Custom connectors that subclass ExchangePyBase and are maintained in this fork.
_CUSTOM_CONNECTORS = ["lighter", "kis", "upbit", "bithumb"]

# Snapshot of the base __init__ parameter names (excluding self is NOT done here
# on purpose: keeping self makes the failure message unambiguous). Bump this list
# only after re-verifying every custom connector forwards any new/changed param.
_EXPECTED_BASE_INIT_PARAMS = ["self", "balance_asset_limit", "rate_limits_share_pct"]

# Params a custom connector's super().__init__() must forward (or pass **kwargs).
_REQUIRED_FORWARDED = {"balance_asset_limit", "rate_limits_share_pct"}


def test_exchange_py_base_init_param_names_snapshot():
    """Pin ExchangePyBase.__init__ param names; upstream drift must be deliberate."""
    names = list(inspect.signature(ExchangePyBase.__init__).parameters.keys())
    assert names == _EXPECTED_BASE_INIT_PARAMS, (
        "ExchangePyBase.__init__ parameters changed upstream.\n"
        f"  expected: {_EXPECTED_BASE_INIT_PARAMS}\n  got:      {names}\n"
        "Re-verify EVERY custom connector forwards the new/changed param through "
        "super().__init__() (see test_custom_connectors_forward_base_init_params), "
        "then update _EXPECTED_BASE_INIT_PARAMS to match."
    )


def _find_super_init_call(tree: ast.Module) -> ast.Call:
    """Locate the `super().__init__(...)` Call node anywhere in the module."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "__init__"
            and isinstance(func.value, ast.Call)
            and isinstance(func.value.func, ast.Name)
            and func.value.func.id == "super"
        ):
            return node
    raise AssertionError("no super().__init__(...) call found")


def _identifiers_in_call(call: ast.Call) -> set[str]:
    """Names referenced as positional/keyword args of a call."""
    idents: set[str] = set()
    for arg in call.args:
        if isinstance(arg, ast.Name):
            idents.add(arg.id)
        elif isinstance(arg, ast.Starred) and isinstance(arg.value, ast.Name):
            idents.add(arg.value.id)
    for kw in call.keywords:
        if kw.arg is not None:
            idents.add(kw.arg)
        if isinstance(kw.value, ast.Name):
            idents.add(kw.value.id)
    return idents


def _has_double_star_kwargs(call: ast.Call) -> bool:
    return any(kw.arg is None for kw in call.keywords)


def test_custom_connectors_forward_base_init_params():
    """Each custom connector must forward the base __init__ params (or **kwargs)."""
    offenders: list[str] = []
    for name in _CUSTOM_CONNECTORS:
        path = _PKG / "connector" / "exchange" / name / f"{name}_exchange.py"
        assert path.is_file(), f"connector source not found: {path}"
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        call = _find_super_init_call(tree)
        if _has_double_star_kwargs(call):
            continue
        forwarded = _identifiers_in_call(call)
        missing = _REQUIRED_FORWARDED - forwarded
        if missing:
            offenders.append(f"{name}: super().__init__ missing {sorted(missing)}")
    assert not offenders, (
        "custom connector(s) do not forward required ExchangePyBase.__init__ "
        f"params through super().__init__(): {offenders}"
    )
