"""JEP-258 — no-dependency Telegram safety-push sink (engine).

Mirrors the JEP-184 LatencyRecorder daemon-sink: notify() never blocks / never raises;
a daemon thread delivers via stdlib http.client; latch-once dedup so a safety latch that
re-asserts every control tick pushes exactly once; behavior-neutral when unconfigured.
"""
import json

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.safety_notifier import (
    SafetyNotifier,
    _post_telegram,
    telegram_path,
    telegram_payload,
)


def test_dedup_latch_once():
    sent = []
    n = SafetyNotifier(transport=sent.append)
    n.notify("kill", "first")
    n.notify("kill", "second")  # same key -> deduped (latch-once)
    assert n.flush()
    assert sent == ["first"]


def test_distinct_keys_each_delivered():
    sent = []
    n = SafetyNotifier(transport=sent.append)
    n.notify("pnl", "a")
    n.notify("rate", "b")
    assert n.flush()
    assert sorted(sent) == ["a", "b"]


def test_unconfigured_is_disabled_noop():
    # No transport + empty env -> disabled sink; notify is a silent no-op (behavior-neutral).
    n = SafetyNotifier(env={})
    assert n.enabled is False
    n.notify("kill", "x")  # must not raise, must not deliver


def test_env_configures_transport():
    # TELEGRAM_TOKEN + ADMIN_USER_ID present -> enabled (delivery not exercised here).
    n = SafetyNotifier(env={"TELEGRAM_TOKEN": "tok", "ADMIN_USER_ID": "123"})
    assert n.enabled is True
    n.close()


def test_partial_env_is_disabled():
    # Token without chat id (or vice versa) -> disabled, not a half-configured crash.
    assert SafetyNotifier(env={"TELEGRAM_TOKEN": "tok"}).enabled is False
    assert SafetyNotifier(env={"ADMIN_USER_ID": "123"}).enabled is False


def test_transport_error_swallowed_worker_survives():
    sent = []

    def flaky(msg):
        if msg == "boom":
            raise RuntimeError("telegram down")
        sent.append(msg)

    n = SafetyNotifier(transport=flaky)
    n.notify("k1", "boom")  # raises in the worker -> swallowed, thread survives
    n.notify("k2", "ok")
    assert n.flush()
    assert sent == ["ok"]


def test_notify_does_not_raise_on_disabled():
    n = SafetyNotifier(env={})
    # Even disabled, repeated notify must be a clean no-op.
    n.notify("a", "1")
    n.notify("a", "2")
    assert n.enabled is False


def test_reset_re_arms_key():
    sent = []
    n = SafetyNotifier(transport=sent.append)
    n.notify("kill", "1")
    assert n.flush()
    n.reset("kill")           # re-arm so a later trip can push again
    n.notify("kill", "2")
    assert n.flush()
    assert sent == ["1", "2"]


def test_telegram_path_and_payload_construction():
    assert telegram_path("TOK") == "/botTOK/sendMessage"
    assert json.loads(telegram_payload("999", "hello")) == {"chat_id": "999", "text": "hello"}


def test_get_instance_is_singleton():
    SafetyNotifier.set_instance(None)
    try:
        a = SafetyNotifier.get_instance()
        b = SafetyNotifier.get_instance()
        assert a is b
    finally:
        SafetyNotifier.set_instance(None)


def test_post_telegram_warns_on_non_2xx_without_leaking_token(monkeypatch, capsys):
    # A bad token/chat (non-2xx) is the worst silent-failure mode for an alerter: it must
    # surface (status only) and NEVER leak the token, and must not raise.
    class _Resp:
        status = 401

        def read(self):
            return b""

    class _Conn:
        def __init__(self, *a, **k):
            pass

        def request(self, *a, **k):
            pass

        def getresponse(self):
            return _Resp()

        def close(self):
            pass

    monkeypatch.setattr("http.client.HTTPSConnection", _Conn)
    _post_telegram("SECRET_TOKEN", "123", "hello")  # must not raise
    err = capsys.readouterr().err
    assert "HTTP 401" in err
    assert "SECRET_TOKEN" not in err  # token must never appear in any diagnostic
