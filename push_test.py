import json
import os
import urllib.request


WEBAPP_URL = os.environ["GSHEET_WEBAPP_URL"]
API_TOKEN = os.environ["GSHEET_API_TOKEN"]


def post_json(url: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, body


def main():
    payload = {
        "token": API_TOKEN,
        "action": "upsert_snapshot",
        "source_date": "2026-03-23",
        "erb_rows": [
            {
                "watchlist_id": "1",
                "match_strength": "strong",
                "debtor_name": "ТОВ ПРИКЛАД",
                "debtor_birthdate": "",
                "debtor_code": "12345678",
                "publisher": "Тестовий орган",
                "org_name": "Тестовий відділ ДВС",
                "org_phone_num": "+380441112233",
                "emp_full_fio": "Іваненко Іван Іванович",
                "emp_phone_num": "+380501112233",
                "email_addr": "test@example.com",
                "vp_ordernum": "999000111",
                "vd_cat": "Стягнення",
                "source_date": "2026-03-23",
                "row_hash": "erb_test_hash_1",
                "first_seen": "2026-03-23",
                "last_seen": "2026-03-23",
                "is_active": "true"
            }
        ],
        "asvp_rows": [
            {
                "watchlist_id": "1",
                "match_strength": "strong",
                "debtor_name": "ТОВ ПРИКЛАД",
                "debtor_birthdate": "",
                "debtor_code": "12345678",
                "creditor_name": "ТОВ КРЕДИТОР",
                "creditor_code": "87654321",
                "vp_ordernum": "999000111",
                "vp_begindate": "06.02.2026 00:00:00",
                "vp_state": "Відкрито",
                "org_name": "Тестовий відділ ДВС",
                "dvs_code": "DVS001",
                "phone_num": "+380441112233",
                "email_addr": "test@example.com",
                "bank_account": "UA123456789012345678901234567",
                "source_date": "2026-03-23",
                "row_hash": "asvp_test_hash_1",
                "first_seen": "2026-03-23",
                "last_seen": "2026-03-23",
                "is_active": "true"
            }
        ],
        "tech_rows": [
            {
                "run_at": "2026-03-23 10:00:00",
                "source_name": "test_push",
                "status": "ok",
                "rows_scanned": "2",
                "matches_found": "2",
                "notes": "Manual test push from GitHub Actions"
            }
        ]
    }

    status, body = post_json(WEBAPP_URL, payload)
    print("HTTP status:", status)
    print("Response body:", body)

    if status != 200:
        raise RuntimeError(f"Unexpected HTTP status: {status}")


if __name__ == "__main__":
    main()
