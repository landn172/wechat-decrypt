#!/usr/bin/env python3
"""wechat-decrypt 命令行查询入口（唯一 entry；MCP server 为可选薄层,内部调这里）。

子命令(统一 --json 可输出结构化):
  list                        列出所有会话
  read <contact> [-n N -d D]  读与某人的聊天
  search <kw> [-d D -n N]     全文搜索
  range <contact> --start YYYY-MM-DD [--end YYYY-MM-DD -n N --offset O]  按日期区间读某个群/联系人
  search-chat <contact> <kw> [--start --end | -d D] [-n N]              在某个群/联系人内搜索
  recent [-d D -n N]          最近动态
  summary [-d D]              结构化摘要(待办分析)
  events [-e EVENT -d D -n N] 系统事件（拍一拍/撤回/群事件等）

用法: python query.py <子命令> [参数] [--json]
"""
import sys
import os
import time
import json
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # skill 根
import config  # noqa: E402
import appmsg  # noqa: E402
import contacts  # noqa: E402
import db  # noqa: E402
import message  # noqa: E402


def _msg_dbs_tables():
    for db_path in db.get_message_dbs():
        tabs = {t.strip() for t in db.query_raw(db_path, "SELECT name FROM sqlite_master WHERE type='table';")}
        yield db_path, tabs


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _chunks(values: list[str], size: int = 200):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _iter_query_pages(db_path: str, selects: list[str], page_size: int = 500):
    union = " UNION ALL ".join(selects)
    if config.DB_BACKEND == 'sqlite3':
        # One sort/scan, with bounded Python batches, instead of repeating the
        # entire UNION and discarding an increasing OFFSET for each page.
        yield from db.iter_query(
            db_path,
            'SELECT * FROM (' + union + ') '
            'ORDER BY create_time DESC, source_table ASC, local_id DESC, server_id DESC;',
            batch_size=page_size,
        )
        return
    offset = 0
    while True:
        rows = db.query(
            db_path,
            "SELECT * FROM (" + union + ") "
            "ORDER BY create_time DESC, source_table ASC, local_id DESC, server_id DESC "
            f"LIMIT {page_size} OFFSET {offset};",
        )
        yield from rows
        if len(rows) < page_size:
            return
        offset += page_size


def _search_identity(db_path: str, row: dict) -> tuple:
    return (
        db_path,
        row.get("source_table", ""),
        row.get("local_id"),
        row.get("server_id"),
        row.get("create_time"),
    )


def list_chats() -> list[dict]:
    name2id = db.get_name2id()
    return [{"wxid": wxid, "display": contacts.resolve_contact_name(wxid)}
            for _table, wxid in sorted(name2id.items(), key=lambda x: x[1])]


def _validate_window(days: int, limit: int | None = None):
    if type(days) is not int or days < 1:
        raise ValueError('days must be a positive integer')
    if limit is not None and (type(limit) is not int or not 1 <= limit <= 2000):
        raise ValueError('limit must be an integer between 1 and 2000')


def list_chat_page(limit: int = 100, offset: int = 0) -> dict:
    """Return a bounded page with a continuation offset."""
    if type(limit) is not int or not 1 <= limit <= 2000:
        raise ValueError('limit must be an integer between 1 and 2000')
    if type(offset) is not int or offset < 0:
        raise ValueError('offset must be a nonnegative integer')
    chats = list_chats()
    page = chats[offset:offset + limit]
    following = offset + len(page)
    return {'chats': page, 'count': len(page), 'total': len(chats),
            'limit': limit, 'offset': offset,
            'next_offset': following if following < len(chats) else None}


def _match_one_chat(contact: str, name2id: dict[str, str]):
    """Return ((table, wxid, display), None) or (None, error dict)."""
    matched = contacts.find_contact(contact, name2id)
    if not matched:
        return None, {"error": f"未找到匹配 '{contact}' 的联系人", "candidates": []}
    if len(matched) > 1:
        return None, {"error": f"匹配 '{contact}' 的联系人不唯一({len(matched)})，请使用具体 wxid 选择",
                      "candidates": [{"wxid": w, "display": d} for _t, w, d in matched[:10]]}
    return matched[0], None


def _parse_date_range(start: str, end: str = "") -> tuple[int, int]:
    """Local-time [start 00:00, day after end 00:00); empty end means today."""
    try:
        first = datetime.strptime(start, "%Y-%m-%d")
        last = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0)
    except (TypeError, ValueError):
        raise ValueError('start/end must be YYYY-MM-DD dates') from None
    if first > last:
        raise ValueError('start must not be after end')
    return int(first.timestamp()), int((last + timedelta(days=1)).timestamp())


def _with_group_sender(msg: dict, row: dict, is_group: bool) -> dict:
    if not is_group or msg.get("is_system"):
        return msg
    sender = row.get("sender_wxid")
    msg["sender"] = ("我" if sender == db.get_my_wxid() else contacts.resolve_contact_name(sender)) if sender else "未知"
    prefix = f"{sender}:\n" if sender else ""
    if prefix and msg["content"].startswith(prefix):
        msg["content"] = msg["content"][len(prefix):]
    return msg


def read_chat(contact: str, limit: int = 50, days: int = 7) -> dict:
    _validate_window(days, limit)
    name2id = db.get_name2id()
    since = int(time.time()) - days * 86400
    matched, error = _match_one_chat(contact, name2id)
    if error:
        return error
    matched = [matched]
    out = []
    for table, wxid, display in matched:
        msgs = []
        for db_path, tabs in _msg_dbs_tables():
            if table not in tabs:
                continue
            rows = db.query(
                db_path,
                f"SELECT create_time, local_type, real_sender_id, "
                f"hex(message_content) AS message_hex, n.user_name AS sender_wxid "
                f"FROM {_quoted_identifier(table)} m "
                f"LEFT JOIN Name2Id n ON n.rowid = m.real_sender_id "
                f"WHERE create_time > {since} ORDER BY create_time DESC LIMIT {limit};")
            msgs.extend(_fmt_msg(row) for row in rows)
        newest = sorted(msgs, key=lambda x: x["_ts"], reverse=True)[:limit]
        out.append({"wxid": wxid, "display": display,
                    "messages": sorted(newest, key=lambda x: x["_ts"])})
    return {"chats": out}


def read_chat_range(contact: str, start: str, end: str = "", limit: int = 200, offset: int = 0) -> dict:
    """One chat's messages in a date range, oldest first, paged by offset."""
    _validate_window(1, limit)
    if type(offset) is not int or offset < 0:
        raise ValueError('offset must be a nonnegative integer')
    lower, upper = _parse_date_range(start, end)
    name2id = db.get_name2id()
    matched, error = _match_one_chat(contact, name2id)
    if error:
        return error
    table, wxid, display = matched
    window = f"create_time >= {lower} AND create_time < {upper}"
    total = 0
    rows = []
    for shard, (db_path, tabs) in enumerate(_msg_dbs_tables()):
        if table not in tabs:
            continue
        counted = db.query(db_path, f"SELECT count(*) AS n FROM {_quoted_identifier(table)} WHERE {window};")
        total += int(counted[0]["n"]) if counted else 0
        for row in db.query(
                db_path,
                f"SELECT create_time, local_id, local_type, real_sender_id, "
                f"hex(message_content) AS message_hex, n.user_name AS sender_wxid "
                f"FROM {_quoted_identifier(table)} m "
                f"LEFT JOIN Name2Id n ON n.rowid = m.real_sender_id "
                f"WHERE {window} ORDER BY create_time ASC, local_id ASC LIMIT {offset + limit};"):
            rows.append((int(row.get("create_time") or 0), shard, int(row.get("local_id") or 0), row))
    rows.sort(key=lambda item: item[:3])
    is_group = wxid.endswith("@chatroom")
    page = [_with_group_sender(_fmt_msg(row), row, is_group) for *_key, row in rows[offset:offset + limit]]
    following = offset + len(page)
    return {"wxid": wxid, "display": display, "is_group": is_group,
            "start": message.format_time(lower), "end": message.format_time(upper - 1),
            "total": total, "count": len(page), "offset": offset, "limit": limit,
            "next_offset": following if following < total else None, "messages": page}


def search(keyword: str, days: int = 30, limit: int = 50) -> dict:
    _validate_window(days, limit)
    scan_until = int(time.time())
    hits = _search(keyword, scan_until - days * 86400 + 1, scan_until + 1, limit)
    return {"keyword": keyword, "count": len(hits), "messages": hits}


def search_chat(contact: str, keyword: str, days: int = 30, limit: int = 50,
                start: str = "", end: str = "") -> dict:
    """Keyword search inside one chat, by recent days or by a start/end date range."""
    if start:
        _validate_window(1, limit)
        lower, upper = _parse_date_range(start, end)
    elif end:
        raise ValueError('end requires start')
    else:
        _validate_window(days, limit)
        now = int(time.time())
        lower, upper = now - days * 86400 + 1, now + 1
    matched, error = _match_one_chat(contact, db.get_name2id())
    if error:
        return error
    table, wxid, display = matched
    hits = _search(keyword, lower, upper, limit, table=table, is_group=wxid.endswith("@chatroom"))
    return {"keyword": keyword, "wxid": wxid, "display": display,
            "start": message.format_time(lower), "end": message.format_time(upper - 1),
            "count": len(hits), "limit": limit, "messages": hits}


def _search(keyword: str, lower: int, upper: int, limit: int,
            table: str | None = None, is_group: bool = False) -> list[dict]:
    """Matches with lower <= create_time < upper, newest first; table restricts to one chat."""
    name2id = db.get_name2id()
    since, scan_until = lower - 1, upper - 1
    kw = keyword.replace("'", "''")
    kw_hex = keyword.encode("utf-8").hex().upper()
    keyword_folded = keyword.casefold()
    hits = []
    seen = set()

    def append_if_match(db_path: str, row: dict) -> bool:
        identity = _search_identity(db_path, row)
        if identity in seen:
            return False
        if keyword_folded not in _searchable_text(row).casefold():
            return False
        seen.add(identity)
        table = row.get("source_table", "")
        formatted = _with_group_sender(_fmt_msg(row), row, is_group)
        formatted["contact"] = contacts.resolve_contact_name(name2id.get(table, table))
        hits.append(formatted)
        return True

    only_table = table
    for db_path, tabs in _msg_dbs_tables():
        tables = sorted(t for t in tabs if t.startswith("Msg_") and only_table in (None, t))
        for table_batch in _chunks(tables):
            selects = []
            fallback_selects = []
            for table in table_batch:
                source = table.replace("'", "''")
                selects.append(
                    f"SELECT '{source}' AS source_table, local_id, server_id, "
                    f"create_time, local_type, "
                    f"real_sender_id, hex(message_content) AS message_hex, n.user_name AS sender_wxid "
                    f"FROM {_quoted_identifier(table)} m "
                    f"LEFT JOIN Name2Id n ON n.rowid = m.real_sender_id WHERE create_time > {since} "
                    f"AND create_time <= {scan_until} "
                    f"AND (message_content LIKE '%{kw}%' "
                    f"OR instr(hex(message_content), '{kw_hex}') > 0)"
                )
                fallback_selects.append(
                    f"SELECT '{source}' AS source_table, local_id, server_id, "
                    f"create_time, local_type, "
                    f"real_sender_id, hex(message_content) AS message_hex, n.user_name AS sender_wxid "
                    f"FROM {_quoted_identifier(table)} m "
                    f"LEFT JOIN Name2Id n ON n.rowid = m.real_sender_id WHERE create_time > {since} "
                    f"AND create_time <= {scan_until} "
                    "AND (((CAST(local_type AS INTEGER) & 65535) = 49) "
                    "OR hex(substr(message_content, 1, 4)) = '28B52FFD')"
                )
            if not selects:
                continue
            matched_rows = db.query(
                db_path,
                "SELECT * FROM (" + " UNION ALL ".join(selects) + ") "
                f"ORDER BY create_time DESC LIMIT {limit};",
            )
            for row in matched_rows:
                append_if_match(db_path, row)

            fallback_matches = 0
            for row in _iter_query_pages(db_path, fallback_selects):
                if append_if_match(db_path, row):
                    fallback_matches += 1
                    if fallback_matches >= limit:
                        break
    hits.sort(key=lambda x: x["_ts"], reverse=True)
    return hits[:limit]


def recent(days: int = 3, limit: int = 100) -> dict:
    _validate_window(days, limit)
    name2id = db.get_name2id()
    since = int(time.time()) - days * 86400
    messages = []
    for db_path, tabs in _msg_dbs_tables():
        for table in (t for t in tabs if t.startswith("Msg_")):
            rows = db.query(
                db_path,
                f"SELECT create_time, local_type, real_sender_id, "
                f"hex(message_content) AS message_hex, n.user_name AS sender_wxid "
                f"FROM {_quoted_identifier(table)} m "
                f"LEFT JOIN Name2Id n ON n.rowid = m.real_sender_id "
                f"WHERE create_time > {since} ORDER BY create_time DESC LIMIT {limit};")
            if not rows:
                continue
            wxid = name2id.get(table, table)
            messages.extend((table, wxid, _fmt_msg(r)) for r in rows)
    messages.sort(key=lambda x: x[2]["_ts"], reverse=True)
    messages = messages[:limit]
    by_contact: dict[str, dict] = {}
    for table, wxid, formatted in messages:
        conv = by_contact.setdefault(
            table,
            {"wxid": wxid, "display": contacts.resolve_contact_name(wxid), "messages": []},
        )
        conv["messages"].append(formatted)
    convs = []
    for conv in by_contact.values():
        conv["count"] = len(conv["messages"])
        convs.append(conv)
    convs.sort(key=lambda c: c["messages"][0]["_ts"], reverse=True)
    return {"days": days, "total": len(messages), "conversations": convs}


def summary(days: int = 3) -> dict:
    _validate_window(days)
    name2id = db.get_name2id()
    since = int(time.time()) - days * 86400
    by_contact: dict[str, dict] = {}
    for db_path, tabs in _msg_dbs_tables():
        for table in (t for t in tabs if t.startswith("Msg_")):
            rows = db.query(
                db_path,
                f"SELECT create_time, local_type, real_sender_id, "
                f"hex(message_content) AS message_hex, n.user_name AS sender_wxid "
                f"FROM {_quoted_identifier(table)} m "
                f"LEFT JOIN Name2Id n ON n.rowid = m.real_sender_id "
                f"WHERE create_time > {since} "
                f"AND ((CAST(local_type AS INTEGER) & 65535) IN (1, 49, 10000, 10002)) "
                f"ORDER BY create_time DESC LIMIT 30;")
            text = [m for m in (_fmt_msg(r) for r in rows) if m["is_text"]]
            if text:
                wxid = name2id.get(table, table)
                conv = by_contact.setdefault(
                    table,
                    {"wxid": wxid, "display": contacts.resolve_contact_name(wxid), "messages": []},
                )
                conv["messages"].extend(text)
    convs = list(by_contact.values())
    for conv in convs:
        conv["messages"] = sorted(conv["messages"], key=lambda x: x["_ts"])[-20:]
    convs.sort(key=lambda c: max(m["_ts"] for m in c["messages"]), reverse=True)
    return {"days": days, "today": datetime.now().strftime("%Y-%m-%d %A"), "conversations": convs}


def system_events(event: str = "", days: int = 30, limit: int = 100) -> dict:
    _validate_window(days, limit)
    from collections import Counter

    name2id = db.get_name2id()
    since = int(time.time()) - days * 86400
    target = message.normalize_system_event_filter(event)
    strict_event = target in message.SYSTEM_EVENTS
    events = []
    for db_path, tabs in _msg_dbs_tables():
        tables = sorted(t for t in tabs if t.startswith("Msg_"))
        for table_batch in _chunks(tables):
            selects = []
            for table in table_batch:
                source = table.replace("'", "''")
                selects.append(
                    f"SELECT '{source}' AS source_table, create_time, local_type, "
                    f"real_sender_id, hex(message_content) AS message_hex "
                    f"FROM {_quoted_identifier(table)} "
                    f"WHERE create_time > {since} "
                    f"AND ((CAST(local_type AS INTEGER) & 65535) IN (10000, 10002))"
                )
            if not selects:
                continue
            row_limit = "" if target else f"LIMIT {limit}"
            rows = db.query(
                db_path,
                "SELECT * FROM (" + " UNION ALL ".join(selects) + ") "
                f"ORDER BY create_time DESC {row_limit};",
            )
            for row in rows:
                table = row.get("source_table", "")
                wxid = name2id.get(table, table)
                formatted = _fmt_msg(row)
                if (
                    target
                    and (
                        formatted["event"] != target
                        if strict_event
                        else target not in formatted["type"].lower()
                        and target not in formatted["content"].lower()
                    )
                ):
                    continue
                formatted["contact"] = contacts.resolve_contact_name(wxid)
                formatted["wxid"] = wxid
                events.append(formatted)
    events.sort(key=lambda item: item["_ts"], reverse=True)
    events = events[:limit]
    counts = Counter(item["event"] for item in events)
    return {
        "days": days,
        "filter": target,
        "count": len(events),
        "by_event": dict(counts),
        "events": events,
    }


def stats(days: int = 30) -> dict:
    _validate_window(days)
    from collections import Counter
    name2id = db.get_name2id()
    since = int(time.time()) - days * 86400
    by_contact = Counter()
    by_type = Counter()
    by_day = Counter()
    unknown_app_types = Counter()
    total = 0
    for db_path, tabs in _msg_dbs_tables():
        for table in (t for t in tabs if t.startswith("Msg_")):
            rows = db.query(
                db_path,
                f"SELECT create_time, local_type, hex(message_content) AS message_hex "
                f"FROM {table} "
                f"WHERE create_time > {since};",
            )
            disp = contacts.resolve_contact_name(name2id.get(table, table))
            for r in rows:
                total += 1
                by_contact[disp] += 1
                type_key = message.normalize_type(r.get("local_type", ""))
                if type_key in ("10000", "10002"):
                    content = _row_content(r)
                    type_label = message.parse_system_message(
                        content,
                        default_event="recall" if type_key == "10002" else "",
                    )["label"]
                elif type_key == "49":
                    parsed_app = appmsg.parse_app_message(
                        _row_content(r), r.get("local_type", "")
                    )
                    type_label = parsed_app["label"]
                    if parsed_app["app_type"] not in appmsg.APP_MESSAGE_TYPES:
                        unknown_app_types[parsed_app["app_type"]] += 1
                else:
                    type_label = message.MSG_TYPES.get(type_key, "其他")
                by_type[type_label] += 1
                ts = int(r.get("create_time", "0") or "0")
                if ts:
                    by_day[datetime.fromtimestamp(ts).strftime("%Y-%m-%d")] += 1
    return {
        "days": days,
        "total": total,
        "by_contact": by_contact.most_common(15),
        "by_type": by_type.most_common(),
        "by_day": sorted(by_day.items()),
        "unknown_app_types": [
            {"app_type": app_type, "count": count}
            for app_type, count in unknown_app_types.most_common()
        ],
    }


def media(out: str = "") -> dict:
    import export_media
    base = db.get_account_files_dir()
    out = out or os.path.expanduser("~/Desktop/wechat_media")
    return {"out": out, **export_media.export(base, out)}


def openfile(name: str, limit: int = 8000) -> dict:
    import glob as _g
    import read_doc
    base = db.get_account_files_dir()
    matches = [m for m in _g.glob(os.path.join(base, "msg", "file", "**", f"*{name}*"), recursive=True) if os.path.isfile(m)]
    if not matches:
        return {"error": f"未在 msg/file 找到含 '{name}' 的文档"}
    return {"path": matches[0], "matches": len(matches), "content": read_doc.read_file(matches[0], limit)}


def _decode_content(mc) -> str:
    return message.decode_message_content(mc)


def _row_content(row: dict) -> str:
    encoded = row.get("message_hex")
    if encoded is not None:
        try:
            return _decode_content(bytes.fromhex(str(encoded)))
        except ValueError:
            return ""
    return _decode_content(row.get("message_content"))


def _searchable_text(row: dict) -> str:
    type_key = message.normalize_type(row.get("local_type", ""))
    content = _row_content(row)
    if type_key in ("10000", "10002"):
        system = message.parse_system_message(
            content,
            default_event="recall" if type_key == "10002" else "",
        )
        return " ".join((system["label"], system["event"], system["text"], content))
    if type_key == "49":
        parsed = appmsg.parse_app_message(content, row.get("local_type", ""))
        return " ".join(
            [content, *(str(value) for value in parsed.values() if value is not None)]
        )
    return " ".join((message.MSG_TYPES.get(type_key, "其他"), content))


def _fmt_msg(row: dict) -> dict:
    ts = row.get("create_time", "0")
    try:
        ts_int = int(ts or "0")
    except (TypeError, ValueError):
        ts_int = 0
    type_key = message.normalize_type(row.get("local_type", ""))
    content = _row_content(row)
    if type_key in ("10000", "10002"):
        system = message.parse_system_message(
            content,
            default_event="recall" if type_key == "10002" else "",
        )
        return {
            "time": message.format_time(ts),
            "_ts": ts_int,
            "direction": f"[系统·{system['label']}]",
            "type": system["label"],
            "event": system["event"],
            "content": system["text"][:500].replace("\n", " "),
            "is_text": bool(system["text"]),
            "is_system": True,
        }
    # Name2Id row IDs are local to a shard; only wxids are portable.
    sender = row.get("sender_wxid")
    direction = "[未知]" if not sender else "[我]" if sender == db.get_my_wxid() else "[对方]"
    if type_key == "49":
        app = appmsg.parse_app_message(content, row.get("local_type", ""))
        quoted_wxid = app.get("quoted_sender_wxid")
        if quoted_wxid:
            name = "我" if quoted_wxid == db.get_my_wxid() else contacts.resolve_contact_name(quoted_wxid)
            if name != quoted_wxid or not app["quoted_sender"]:
                app["quoted_sender"] = name
            app["summary"] = appmsg.format_app_message(app)
        details = app["summary"].replace("\n", " ")[:500]
        display = f"[{app['label']}]" + (f" {details}" if details else "")
        return {
            "time": message.format_time(ts),
            "_ts": ts_int,
            "direction": direction,
            "type": app["label"],
            "event": None,
            "content": display,
            "is_text": True,
            "is_system": False,
            "app": app,
        }
    is_text = type_key == "1" and not content.startswith("<")
    return {
        "time": message.format_time(ts), "_ts": ts_int,
        "direction": direction,
        "type": message.MSG_TYPES.get(type_key, "其他"),
        "event": None,
        "content": content if is_text else "",
        "is_text": is_text,
        "is_system": False,
    }


# ── 人类可读渲染(非 --json 时) ─────────────────────────────────
def _human(cmd: str, r) -> str:
    if cmd == 'list-page':
        out = [f"显示 {r['count']} / {r['total']} 个对话，offset={r['offset']}:"]
        out.extend(f"  {c['display']} (wxid: {c['wxid']})" for c in r['chats'])
        if r['next_offset'] is not None:
            out.append(f"下一页 offset={r['next_offset']}，limit={r['limit']}")
        return '\n'.join(out)
    if isinstance(r, dict) and r.get("error"):
        return r["error"] + ("\n" + "\n".join(f"  {c['display']} (wxid: {c['wxid']})" for c in r.get("candidates", [])) if r.get("candidates") else "")
    if cmd == "list":
        return f"共 {len(r)} 个对话:\n" + "\n".join(f"  {c['display']} (wxid: {c['wxid']})" for c in r)
    if cmd == "read":
        out = []
        for chat in r["chats"]:
            out.append(f"\n=== 与 {chat['display']} 的对话 ===")
            for m in chat["messages"]:
                out.append(f"  [{m['time']}] {m['direction']} " + (m["content"] if m["is_text"] else f"[{m['type']}]"))
        return "\n".join(out) or "未找到消息"
    if cmd == "range":
        kind = "群聊" if r["is_group"] else "对话"
        out = [f"=== {kind} {r['display']}：{r['start']} ~ {r['end']}，共 {r['total']} 条，"
               f"本页第 {r['offset'] + 1}-{r['offset'] + r['count']} 条 ==="]
        for m in r["messages"]:
            who = m.get("sender") or m["direction"]
            out.append(f"  [{m['time']}] {who}: " + (m["content"] if m["is_text"] else f"[{m['type']}]"))
        if r["next_offset"] is not None:
            out.append(f"未读完：下一页 offset={r['next_offset']}，limit={r['limit']}")
        return "\n".join(out)
    if cmd == "search-chat":
        out = [f"在 {r['display']} 中搜索 '{r['keyword']}'（{r['start']} ~ {r['end']}）找到 {r['count']} 条"
               + ("，已达 limit，可能还有更早的结果" if r["count"] >= r["limit"] else "") + ":"]
        for m in r["messages"]:
            who = m.get("sender") or m["direction"]
            out.append(f"  [{m['time']}] {who}: " + (m["content"] if m["is_text"] else f"[{m['type']}]"))
        return "\n".join(out)
    if cmd == "search":
        out = [f"搜索 '{r['keyword']}' 找到 {r['count']} 条:\n"]
        for m in r["messages"]:
            out.append(f"  [{m['time']}] {m.get('contact','')} {m['direction']}: " + (m["content"] if m["is_text"] else f"[{m['type']}]"))
        return "\n".join(out)
    if cmd == "recent":
        out = [f"最近 {r['days']} 天共 {r['total']} 条,涉及 {len(r['conversations'])} 个对话:\n"]
        for c in r["conversations"]:
            out.append(f"\n--- {c['display']} ({c['count']} 条) ---")
            for m in c["messages"][:20]:
                if m["is_text"] or m["type"] in ("图片", "语音", "视频", "链接"):
                    out.append(f"  [{m['time']}] {m['direction']} " + (m["content"] if m["is_text"] else f"[{m['type']}]"))
        return "\n".join(out)
    if cmd == "summary":
        out = [f"=== 微信聊天摘要(最近 {r['days']} 天)===", f"今天 {r['today']}",
               f"涉及 {len(r['conversations'])} 个对话\n[我]=用户发, [对方]=联系人发\n分析: 待办/承诺/计划/待回复\n"]
        for c in r["conversations"]:
            out.append(f"\n--- {c['display']} ---")
            for m in c["messages"]:
                out.append(f"  [{m['time']}] {m['direction']} {m['content']}")
        return "\n".join(out)
    if cmd == "events":
        label = f"，筛选 {r['filter']}" if r["filter"] else ""
        out = [f"最近 {r['days']} 天系统事件 {r['count']} 条{label}:"]
        for item in r["events"]:
            content = f" {item['content']}" if item["content"] else ""
            out.append(
                f"  [{item['time']}] {item['contact']} {item['direction']}{content}"
            )
        return "\n".join(out)
    if cmd == "stats":
        out = [f"=== 统计(最近 {r['days']} 天, 共 {r['total']} 条) ===", "\n发言排行:"]
        out += [f"  {n:>5}  {c}" for c, n in r["by_contact"]]
        out.append("\n类型分布:")
        out += [f"  {n:>5}  {t}" for t, n in r["by_type"]]
        if r.get("unknown_app_types"):
            out.append("\n未识别分享子类型:")
            out += [
                f"  {item['count']:>5}  app_type={item['app_type']}"
                for item in r["unknown_app_types"]
            ]
        return "\n".join(out)
    if cmd == "media":
        return f"导出 → {r['out']}: {r['docs']} 文档, {r['videos']} 视频, {r['images']} 图片(跳过 {r['enc_dat']} 个 .dat 加密原图)"
    if cmd == "openfile":
        return r["error"] if r.get("error") else f"文档: {r['path']}(匹配 {r['matches']} 个)\n\n{r['content']}"
    return json.dumps(r, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser(description="wechat-decrypt 命令行查询")
    base = argparse.ArgumentParser(add_help=False)
    base.add_argument("--json", action="store_true", help="结构化 JSON 输出")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list", parents=[base]); p.add_argument('-n', '--limit', type=int); p.add_argument('--offset', type=int, default=0)
    p = sub.add_parser("read", parents=[base]); p.add_argument("contact"); p.add_argument("-n", "--limit", type=int, default=50); p.add_argument("-d", "--days", type=int, default=7)
    p = sub.add_parser("search", parents=[base]); p.add_argument("keyword"); p.add_argument("-d", "--days", type=int, default=30); p.add_argument("-n", "--limit", type=int, default=50)
    p = sub.add_parser("range", parents=[base]); p.add_argument("contact"); p.add_argument("--start", required=True); p.add_argument("--end", default=""); p.add_argument("-n", "--limit", type=int, default=200); p.add_argument("--offset", type=int, default=0)
    p = sub.add_parser("search-chat", parents=[base]); p.add_argument("contact"); p.add_argument("keyword"); p.add_argument("--start", default=""); p.add_argument("--end", default=""); p.add_argument("-d", "--days", type=int, default=30); p.add_argument("-n", "--limit", type=int, default=50)
    p = sub.add_parser("recent", parents=[base]); p.add_argument("-d", "--days", type=int, default=3); p.add_argument("-n", "--limit", type=int, default=100)
    p = sub.add_parser("summary", parents=[base]); p.add_argument("-d", "--days", type=int, default=3)
    p = sub.add_parser("events", parents=[base]); p.add_argument("-e", "--event", default=""); p.add_argument("-d", "--days", type=int, default=30); p.add_argument("-n", "--limit", type=int, default=100)
    p = sub.add_parser("stats", parents=[base]); p.add_argument("-d", "--days", type=int, default=30)
    p = sub.add_parser("media", parents=[base]); p.add_argument("-o", "--out", default="")
    p = sub.add_parser("openfile", parents=[base]); p.add_argument("name")
    a = ap.parse_args()
    if a.cmd == "list":
        if a.limit is not None or a.offset:
            r = list_chat_page(a.limit if a.limit is not None else 100, a.offset)
            a.cmd = 'list-page'
        else:
            r = list_chats()
    elif a.cmd == "read":
        r = read_chat(a.contact, a.limit, a.days)
    elif a.cmd == "range":
        r = read_chat_range(a.contact, a.start, a.end, a.limit, a.offset)
    elif a.cmd == "search-chat":
        r = search_chat(a.contact, a.keyword, a.days, a.limit, a.start, a.end)
    elif a.cmd == "search":
        r = search(a.keyword, a.days, a.limit)
    elif a.cmd == "recent":
        r = recent(a.days, a.limit)
    elif a.cmd == "summary":
        r = summary(a.days)
    elif a.cmd == "events":
        r = system_events(a.event, a.days, a.limit)
    elif a.cmd == "stats":
        r = stats(a.days)
    elif a.cmd == "media":
        r = media(a.out)
    elif a.cmd == "openfile":
        r = openfile(a.name)
    print(json.dumps(r, ensure_ascii=False, indent=2) if a.json else _human(a.cmd, r))


if __name__ == "__main__":
    main()
