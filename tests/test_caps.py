"""The two join links and what each one is allowed to do.

These tests talk to the Store directly, so they spawn no coordinator and no agents
and stay fast. The end-to-end behaviour over HTTP and the websocket is covered in
test_pool.py; what matters here is that the rules themselves can't drift.
"""

from pathlib import Path

from yourharness.coordinator.state import (
    CAP_ADMIN,
    CAP_FILES,
    CAP_SHELL,
    CAP_SUBMIT,
    LENDER_CAPS,
    OWNER_CAPS,
    Store,
    caps_of,
)


def test_the_two_links_are_different_secrets(tmp_path: Path):
    s = Store(tmp_path)
    assert s.join_token != s.lend_token
    assert len(s.lend_token) >= 16


def test_each_way_in_grants_the_right_powers(tmp_path: Path):
    s = Store(tmp_path)
    assert s.caps_for_join(s.join_token) == OWNER_CAPS
    assert s.caps_for_join(s.lend_token) == LENDER_CAPS
    assert s.caps_for_join(s.join_code) == OWNER_CAPS


def test_a_wrong_token_grants_nothing(tmp_path: Path):
    s = Store(tmp_path)
    for bad in ("nope", "", None, 1234, s.join_token + "x", s.lend_token[:-1]):
        assert s.caps_for_join(bad) is None, bad
        assert not s.join_ok(bad)


def test_an_online_pool_takes_links_only(tmp_path: Path):
    """A 4-digit code has 10,000 possibilities, which is far too few on the internet."""
    s = Store(tmp_path)
    assert s.caps_for_join(s.join_code, allow_code=False) is None
    assert s.caps_for_join(s.join_token, allow_code=False) == OWNER_CAPS
    assert s.caps_for_join(s.lend_token, allow_code=False) == LENDER_CAPS


def test_a_lender_contributes_but_controls_nothing(tmp_path: Path):
    s = Store(tmp_path)
    rec, _ = s.add_device("agent", "A friend's PC", LENDER_CAPS)
    caps = caps_of(rec)
    for cap in (CAP_SUBMIT, CAP_SHELL, CAP_FILES, CAP_ADMIN):
        assert cap not in caps, f"a lender must not hold {cap}"


def test_your_own_device_keeps_full_control(tmp_path: Path):
    s = Store(tmp_path)
    rec, _ = s.add_device("browser", "My laptop", OWNER_CAPS)
    caps = caps_of(rec)
    for cap in (CAP_SUBMIT, CAP_SHELL, CAP_FILES, CAP_ADMIN):
        assert cap in caps


def test_devices_signed_in_before_lending_existed_keep_what_they_had(tmp_path: Path):
    """An upgrade must never quietly take powers away from a device already in the pool."""
    legacy = {"id": "abc123", "kind": "browser", "name": "Old laptop", "secret": "x"}
    assert caps_of(legacy) == OWNER_CAPS
    assert caps_of(None) == ()


def test_an_invented_capability_is_ignored(tmp_path: Path):
    """Caps are filtered against the known set, so a tampered file can't mint a new power."""
    forged = {"id": "x", "kind": "browser", "name": "F", "caps": ["submit", "root", "admin"]}
    assert caps_of(forged) == (CAP_SUBMIT, CAP_ADMIN)


def test_rotating_the_lend_link_leaves_your_own_link_alone(tmp_path: Path):
    s = Store(tmp_path)
    old_lend, owner = s.lend_token, s.join_token
    s.rotate_lend_token()
    assert s.caps_for_join(old_lend) is None, "a lender you cut off must not get back in"
    assert s.caps_for_join(owner) == OWNER_CAPS


def test_rotating_your_own_link_leaves_the_lend_link_alone(tmp_path: Path):
    s = Store(tmp_path)
    old_owner, lend = s.join_token, s.lend_token
    s.rotate_join_token()
    assert s.caps_for_join(old_owner) is None
    assert s.caps_for_join(lend) == LENDER_CAPS


def test_links_and_device_powers_survive_a_restart(tmp_path: Path):
    s = Store(tmp_path)
    rec, _ = s.add_device("agent", "A friend's PC", LENDER_CAPS)
    again = Store(tmp_path)
    assert again.lend_token == s.lend_token
    assert again.join_token == s.join_token
    assert caps_of(again.data["devices"][rec["id"]]) == LENDER_CAPS
