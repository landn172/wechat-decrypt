import hashlib
import os
import sqlite3
from datetime import datetime

import pytest

GROUP = "12345@chatroom"
OTHER = "wxid_other"


def _table(wxid):
    return "Msg_" + hashlib.md5(wxid.encode()).hexdigest()


def _ts(text):
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M:%S").timestamp())


@pytest.fixture
def chats(win_backend, monkeypatch):
    import contacts
    import db

    path = os.path.join(win_backend, "wxid_test001_a2f4", "db_storage", "message", "message_0.db")
    con = sqlite3.connect(path)
    # Name2Id rowids: 1=wxid_friend001 (conftest), 2=group, 3=alice, 4=me, 5=other
    con.executemany("INSERT INTO Name2Id (user_name) VALUES (?);",
                    [(GROUP,), ("wxid_alice",), ("wxid_me",), (OTHER,)])
    for wxid, rows in {
        GROUP: [
            (1, _ts("2026-10-09 23:59:59"), 3, "wxid_alice:\nTrading before range"),
            (2, _ts("2026-10-10 00:00:00"), 3, "wxid_alice:\nTrading plan"),
            (3, _ts("2026-10-11 12:00:00"), 4, "my Trading note"),
            (4, _ts("2026-10-12 23:59:59"), 3, "wxid_alice:\nclosing"),
            (5, _ts("2026-10-13 00:00:00"), 3, "wxid_alice:\nTrading after range"),
        ],
        OTHER: [(1, _ts("2026-10-11 09:00:00"), 5, "Trading elsewhere")],
    }.items():
        con.execute(f"CREATE TABLE {_table(wxid)} (local_id INTEGER, server_id INTEGER, create_time INTEGER, "
                    "local_type INTEGER, real_sender_id INTEGER, message_content TEXT);")
        con.executemany(f"INSERT INTO {_table(wxid)} VALUES (?, ?, ?, 1, ?, ?);",
                        [(i, 9000 + i, ts, sender, text) for i, ts, sender, text in rows])
    con.commit()
    con.close()

    contact_db = os.path.join(win_backend, "wxid_test001_a2f4", "db_storage", "contact", "contact.db")
    con = sqlite3.connect(contact_db)
    con.executemany("INSERT INTO contact (username, alias, nick_name, remark, local_type) VALUES (?, '', ?, '', 3);",
                    [(GROUP, "Product Group"), ("wxid_alice", "Alice"), (OTHER, "Other")])
    con.commit()
    con.close()

    db.reset_caches()
    contacts.invalidate_cache()
    monkeypatch.setattr(db, "get_my_wxid", lambda: "wxid_me")
    return win_backend


def test_range_includes_both_end_days_and_names_group_senders(chats):
    import query

    result = query.read_chat_range("Product Group", "2026-10-10", "2026-10-12")
    assert result["is_group"] and result["total"] == 3
    assert [(m["sender"], m["content"]) for m in result["messages"]] == [
        ("Alice", "Trading plan"), ("我", "my Trading note"), ("Alice", "closing")]
    assert result["next_offset"] is None


def test_range_pages_oldest_first(chats):
    import query

    first = query.read_chat_range(GROUP, "2026-10-01", "2026-10-31", limit=2)
    second = query.read_chat_range(GROUP, "2026-10-01", "2026-10-31", limit=2, offset=first["next_offset"])
    assert first["total"] == 5 and first["next_offset"] == 2
    contents = [m["content"] for m in first["messages"] + second["messages"]]
    assert contents == ["Trading before range", "Trading plan", "my Trading note", "closing"]


def test_search_chat_stays_inside_one_chat_and_range(chats):
    import query

    result = query.search_chat("Product Group", "Trading", start="2026-10-10", end="2026-10-12")
    assert [m["content"] for m in result["messages"]] == ["my Trading note", "Trading plan"]
    assert {m["contact"] for m in result["messages"]} == {"Product Group"}


def test_global_search_still_spans_chats(chats):
    import query

    contacts = {m["contact"] for m in query.search("Trading", days=36500, limit=50)["messages"]}
    assert contacts == {"Product Group", "Other"}


@pytest.mark.parametrize("start,end", [("2026/10/10", ""), ("2026-10-12", "2026-10-10"), ("", "x")])
def test_invalid_dates_fail_before_database_discovery(monkeypatch, start, end):
    import query

    monkeypatch.setattr(query.db, "get_name2id", lambda: pytest.fail("must validate before reading"))
    with pytest.raises(ValueError):
        query.read_chat_range(GROUP, start, end)
    with pytest.raises(ValueError):
        query.search_chat(GROUP, "x", start=start, end=end)
