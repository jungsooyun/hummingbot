"""Upstream-merge regression guards (JEP-140).

Companion to ``test_no_dead_script_strategy_base_import.py``. Those tests pin a
single concrete breakage the v2.14 merge introduced; these pin the *interfaces*
where a future upstream merge can silently break OUR custom code:

1. Constructor-binding guard (KIS / Lighter) — the connect-key config-map fields
   are forwarded verbatim as kwargs to the connector ``__init__`` by
   ``api_keys_from_connector_config_map``. If an upstream refactor renames a
   ``__init__`` parameter (or our config map drifts), the forwarded kwarg would
   raise ``TypeError`` only at live connect time. ``Signature.bind_partial``
   catches that drift at test time.

2. ``ExchangePyBase.__init__`` signature snapshot — every custom connector calls
   ``super().__init__(balance_asset_limit, rate_limits_share_pct)`` (the v2.14
   additions). If upstream changes that base signature, our connectors break.
   This snapshot fails loudly on any change so we re-audit deliberately.

3. Executor registration consistency — our custom ``ladder_maker_executor`` must
   be registered in ALL THREE sites that an upstream merge could partially
   clobber: the orchestrator eager-import map, the ``ExecutorConfigBase.type``
   Literal, and the ``AnyExecutorConfig`` Union. A merge that drops it from any
   one site breaks config (de)serialization or orchestration silently.

These import hummingbot (unlike the sibling pure-stdlib scan) because they assert
on live signatures / pydantic models — that is the point.
"""
import inspect
import typing

import pytest

from hummingbot.connector.exchange_py_base import ExchangePyBase

# --- Custom connectors under guard ----------------------------------------- #
from hummingbot.connector.exchange.kis import kis_utils
from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
from hummingbot.connector.exchange.lighter import lighter_utils
from hummingbot.connector.exchange.lighter.lighter_exchange import LighterExchange

# --- Executor registration sites ------------------------------------------- #
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderMakerExecutorConfig
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
from hummingbot.strategy_v2.models.executors_info import AnyExecutorConfig


# The custom executor's canonical registration name. Pinned here so a rename in
# any single site is caught against this constant rather than against itself.
CUSTOM_LADDER_TYPE = "ladder_maker_executor"


def _connect_key_fields(config_map_cls) -> list[str]:
    """Field names a config map forwards to the connector constructor.

    Mirrors ``api_keys_from_connector_config_map``: only ``is_connect_key`` fields
    are forwarded as kwargs to the trading-connector ``__init__``.
    """
    names = []
    for name, field in config_map_cls.model_fields.items():
        extra = field.json_schema_extra or {}
        if extra.get("is_connect_key"):
            names.append(name)
    return names


# --------------------------------------------------------------------------- #
# 1. Constructor-binding guard: config-map connect-keys must bind to __init__   #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "connector_cls, config_map_cls",
    [
        (KisExchange, kis_utils.KisConfigMap),
        (LighterExchange, lighter_utils.LighterConfigMap),
    ],
    ids=["kis", "lighter"],
)
def test_config_map_connect_keys_bind_to_constructor(connector_cls, config_map_cls):
    """Every forwarded connect-key field must be an accepted __init__ kwarg.

    The runtime path is: ``api_keys_from_connector_config_map`` builds a dict of
    {connect_key_field: value} and the factory calls
    ``connector_cls(**api_keys, trading_pairs=..., trading_required=...)``.
    If a forwarded key has no matching parameter, that call raises TypeError at
    live connect time. ``bind_partial`` reproduces that bind and fails here.
    """
    connect_keys = _connect_key_fields(config_map_cls)
    # Guard the guard: if this empties out, the config map structure changed and
    # the test would become a tautology — fail loudly instead.
    assert connect_keys, f"{config_map_cls.__name__} exposes no is_connect_key fields"

    sig = inspect.signature(connector_cls.__init__)
    # Build the kwargs exactly as the factory would (placeholder values).
    forwarded = {name: "x" for name in connect_keys}
    forwarded["trading_pairs"] = ["TEST-PAIR"]
    forwarded["trading_required"] = False

    try:
        sig.bind_partial(None, **forwarded)  # None == self
    except TypeError as exc:
        params = list(sig.parameters)
        raise AssertionError(
            f"{connector_cls.__name__}.__init__ cannot accept forwarded config-map "
            f"connect-key kwargs {sorted(forwarded)}. __init__ params={params}. "
            f"An upstream rename or config-map drift broke the connect path: {exc}"
        )


# --------------------------------------------------------------------------- #
# 2. ExchangePyBase.__init__ signature snapshot                                #
# --------------------------------------------------------------------------- #
def test_exchange_py_base_init_signature_snapshot():
    """Pin the base ctor signature our connectors call via super().__init__.

    v2.14 added ``balance_asset_limit`` and ``rate_limits_share_pct``. Our KIS and
    Lighter connectors forward both. If upstream adds/removes/renames a base
    parameter, this fails so we re-audit the super().__init__ call sites instead
    of discovering the break at runtime.
    """
    params = inspect.signature(ExchangePyBase.__init__).parameters
    assert list(params) == ["self", "balance_asset_limit", "rate_limits_share_pct"], (
        "ExchangePyBase.__init__ signature changed; custom connectors call "
        "super().__init__(balance_asset_limit, rate_limits_share_pct) and must be "
        f"re-audited. Observed params: {list(params)}"
    )
    # The v2.14 additions must be present AND optional (connectors that don't pass
    # them — and there are some — would otherwise break).
    for name in ("balance_asset_limit", "rate_limits_share_pct"):
        assert params[name].default is not inspect.Parameter.empty, (
            f"ExchangePyBase.__init__ param '{name}' lost its default; "
            "this breaks connectors that omit it."
        )


def test_custom_connectors_forward_v214_base_kwargs():
    """KIS and Lighter must keep accepting the v2.14 base kwargs.

    These are forwarded straight through to ``super().__init__`` and the factory's
    non_trading path forwards every config-map field. A drop here re-introduces
    the exact class of silent break JEP-140 guards against.
    """
    for cls in (KisExchange, LighterExchange):
        params = inspect.signature(cls.__init__).parameters
        for name in ("balance_asset_limit", "rate_limits_share_pct"):
            assert name in params, (
                f"{cls.__name__}.__init__ no longer accepts '{name}' (v2.14 "
                "ExchangePyBase addition); the super().__init__ forward will break."
            )


# --------------------------------------------------------------------------- #
# 3. Executor registration consistency across all three sites                  #
# --------------------------------------------------------------------------- #
def _literal_values(annotation) -> set:
    """Collect Literal string members from a (possibly nested) type annotation."""
    if typing.get_origin(annotation) is typing.Literal:
        return set(typing.get_args(annotation))
    return set()


def test_ladder_executor_registered_in_orchestrator_mapping():
    mapping = ExecutorOrchestrator._executor_mapping
    assert mapping.get(CUSTOM_LADDER_TYPE) is LadderMakerExecutor, (
        f"'{CUSTOM_LADDER_TYPE}' missing/mismatched in "
        "ExecutorOrchestrator._executor_mapping. An upstream merge dropping it "
        "breaks orchestration of the custom ladder executor."
    )


def test_ladder_executor_type_in_config_base_literal():
    type_field = ExecutorConfigBase.model_fields["type"]
    allowed = _literal_values(type_field.annotation)
    assert CUSTOM_LADDER_TYPE in allowed, (
        f"'{CUSTOM_LADDER_TYPE}' missing from ExecutorConfigBase.type Literal "
        f"(found {sorted(allowed)}). Config deserialization of the ladder "
        "executor would be rejected."
    )


def test_ladder_executor_config_in_any_executor_config_union():
    members = set(typing.get_args(AnyExecutorConfig))
    assert LadderMakerExecutorConfig in members, (
        "LadderMakerExecutorConfig missing from AnyExecutorConfig Union; "
        "ExecutorInfo.config (discriminated on 'type') cannot resolve the "
        "ladder executor's config."
    )


def test_ladder_executor_registration_three_sites_agree():
    """Cross-site agreement: the SAME canonical type string appears in all three.

    This is the consolidating assertion — the three sites individually checked
    above must agree on one identity so a partial upstream merge (e.g. updates
    the Union but not the orchestrator map) is caught.
    """
    in_mapping = CUSTOM_LADDER_TYPE in ExecutorOrchestrator._executor_mapping
    in_literal = CUSTOM_LADDER_TYPE in _literal_values(
        ExecutorConfigBase.model_fields["type"].annotation
    )
    # The Union member carries the type via its own Literal default field.
    config_type_default = LadderMakerExecutorConfig.model_fields["type"].default
    in_union = LadderMakerExecutorConfig in set(typing.get_args(AnyExecutorConfig))

    assert in_mapping and in_literal and in_union, (
        "Ladder executor registration is inconsistent across the 3 sites "
        f"(orchestrator_map={in_mapping}, config_literal={in_literal}, "
        f"any_config_union={in_union})."
    )
    assert config_type_default == CUSTOM_LADDER_TYPE, (
        f"LadderMakerExecutorConfig.type default '{config_type_default}' diverged "
        f"from canonical '{CUSTOM_LADDER_TYPE}'; the discriminator no longer "
        "matches the orchestrator-map key / Literal member."
    )
