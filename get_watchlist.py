import json
import os
import urllib.parse
import urllib.request


WEBAPP_URL = os.environ["GSHEET_WEBAPP_URL"]
API_TOKEN = os.environ["GSHEET_API_TOKEN"]


def main():
    params = urllib.parse.urlencode({
        "action": "get_watchlist",
        "token": API_TOKEN,
    })

    url = f"{WEBAPP_URL}?{params}"
    req = urllib.request.Request(url, method="GET")

    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
        print("HTTP status:", resp.status)
        print("Response body:", body)

        data = json.loads(body)
        if not data.get("ok"):
            raise RuntimeError(f"API returned error: {data}")

        with open("watchlist.json", "w", encoding="utf-8") as f:
            json.dump(data.get("rows", []), f, ensure_ascii=False, indent=2)

        print("Saved watchlist.json")


if __name__ == "__main__":
    main()
