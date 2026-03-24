import json
import os
import urllib.request


WEBAPP_URL = os.environ["GSHEET_WEBAPP_URL"]
API_TOKEN = os.environ["GSHEET_API_TOKEN"]


def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def post_json(url: str, payload: dict):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, body


def main():
    erb_rows = load_json("filtered_erb.json", [])
    tech_rows = load_json("tech_rows.json", [])

    source_date = ""
    if tech_rows and isinstance(tech_rows, list):
        source_date = str(tech_rows[0].get("run_at", "")).strip()

    payload = {
        "token": API_TOKEN,
        "action": "upsert_snapshot",
        "source_date": source_date,
        "erb_rows": erb_rows,
        "tech_rows": tech_rows,
    }

    status, body = post_json(WEBAPP_URL, payload)

    print("HTTP status:", status)
    print("Response body:", body)

    if status != 200:
        raise RuntimeError(f"Unexpected HTTP status: {status}")

    try:
        parsed = json.loads(body)
    except Exception:
        raise RuntimeError(f"Invalid JSON response: {body}")

    if not parsed.get("ok"):
        raise RuntimeError(f"Apps Script returned error: {parsed}")


if __name__ == "__main__":
    main()
