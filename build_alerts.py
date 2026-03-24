import json
from datetime import datetime


def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def make_key(row: dict) -> str:
    return "|".join([
        str(row.get("watchlist_id", "")).strip(),
        str(row.get("debtor_code", "")).strip(),
        str(row.get("vp_ordernum", "")).strip(),
    ])


def brief(row: dict) -> str:
    debtor = str(row.get("debtor_name", "")).strip()
    code = str(row.get("debtor_code", "")).strip()
    vp = str(row.get("vp_ordernum", "")).strip()
    org_name = str(row.get("org_name", "")).strip()
    return f"{debtor} (код: {code}, ВП: {vp}, орган/виконавець: {org_name})"


def main():
    current_rows = load_json("erb_current.json", [])
    new_rows = load_json("filtered_erb.json", [])

    current_map = {make_key(r): r for r in current_rows}
    new_map = {make_key(r): r for r in new_rows}

    current_keys = set(current_map.keys())
    new_keys = set(new_map.keys())

    added_keys = sorted(new_keys - current_keys)
    removed_keys = sorted(current_keys - new_keys)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    alerts = []

    # 1. Оновлюємо first_seen / last_seen у новому snapshot
    enriched_rows = []
    for key, row in new_map.items():
        old_row = current_map.get(key)

        new_row = dict(row)

        if old_row:
            old_first_seen = str(old_row.get("first_seen", "")).strip()
            new_row["first_seen"] = old_first_seen if old_first_seen else str(new_row.get("source_date", "")).strip()
        else:
            new_row["first_seen"] = str(new_row.get("source_date", "")).strip()

        new_row["last_seen"] = str(new_row.get("source_date", "")).strip()
        enriched_rows.append(new_row)

    # 2. Формуємо alerts по нових записах
    for key in added_keys:
        row = new_map[key]
        alerts.append({
            "alert_date": now_str,
            "watchlist_id": row.get("watchlist_id", ""),
            "entity_label": row.get("debtor_name", ""),
            "alert_type": "added_to_erb",
            "source_name": "erb",
            "vp_ordernum": row.get("vp_ordernum", ""),
            "old_value": "",
            "new_value": brief(row),
            "summary": f"Новий запис у ЄРБ: {brief(row)}",
            "is_sent": "false",
            "sent_at": "",
        })

    # 3. Формуємо alerts по зниклих записах
    for key in removed_keys:
        row = current_map[key]
        alerts.append({
            "alert_date": now_str,
            "watchlist_id": row.get("watchlist_id", ""),
            "entity_label": row.get("debtor_name", ""),
            "alert_type": "removed_from_erb",
            "source_name": "erb",
            "vp_ordernum": row.get("vp_ordernum", ""),
            "old_value": brief(row),
            "new_value": "",
            "summary": f"Запис зник з ЄРБ: {brief(row)}",
            "is_sent": "false",
            "sent_at": "",
        })

    # 4. Перезаписуємо filtered_erb.json уже з правильними first_seen / last_seen
    save_json("filtered_erb.json", enriched_rows)
    save_json("alerts.json", alerts)

    print(f"Current rows: {len(current_rows)}")
    print(f"New rows: {len(new_rows)}")
    print(f"Added alerts: {len(added_keys)}")
    print(f"Removed alerts: {len(removed_keys)}")
    print("Updated filtered_erb.json with proper first_seen/last_seen")
    print("Saved alerts.json")


if __name__ == "__main__":
    main()
