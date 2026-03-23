import csv
import io
import json
import re
import zipfile
import time
from pathlib import Path
from urllib.request import urlopen, Request


ERB_DATAPACKAGE_URL = "https://data.gov.ua/dataset/506734bf-2480-448c-a2b4-90b6d06df11e/datapackage"
ERB_ZIP_PATH = Path("erb_debug.zip")
WATCHLIST_PATH = Path("watchlist.json")

ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "cp1251", "cp1252"]


def normalize_text(value: str) -> str:
    value = str(value or "").strip().upper()
    value = value.replace("’", "'").replace("`", "'").replace("Ё", "Е")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_code(value: str) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def truthy(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def fetch_text(url: str, retries: int = 4, timeout: int = 90) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
            with urlopen(req, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(5 * attempt)
    raise RuntimeError(f"Не вдалося завантажити текст із {url}: {last_error}")


def fetch_to_file(url: str, target_path: Path, retries: int = 3, timeout: int = 300) -> Path:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
            with urlopen(req, timeout=timeout) as response, open(target_path, "wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)

            with zipfile.ZipFile(target_path, "r") as zf:
                bad_member = zf.testzip()
                if bad_member is not None:
                    raise zipfile.BadZipFile(f"CRC failed for member: {bad_member}")

            return target_path
        except Exception as e:
            last_error = e
            if target_path.exists():
                target_path.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(f"Не вдалося коректно завантажити файл {url}: {last_error}")


def resolve_resource_from_datapackage(datapackage_url: str) -> dict:
    raw = fetch_text(datapackage_url)
    data = json.loads(raw)
    resources = data.get("resources", [])
    zip_resources = [
        r for r in resources
        if str(r.get("format", "")).upper() == "ZIP" or str(r.get("path", "")).lower().endswith(".zip")
    ]
    if not zip_resources:
        raise RuntimeError("У datapackage немає ZIP-ресурсу")

    resource = zip_resources[0]
    return {
        "dataset_title": data.get("title", ""),
        "resource_name": resource.get("name", ""),
        "resource_path": resource.get("path", ""),
    }


def decode_bytes(raw_bytes: bytes):
    last_error = None
    for enc in ENCODINGS_TO_TRY:
        try:
            return raw_bytes.decode(enc), enc
        except Exception as e:
            last_error = e
    raise RuntimeError(f"Не вдалося декодувати байти. Остання помилка: {last_error}")


def parse_header_and_delimiter_from_zip(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("У ZIP не знайдено CSV")

        csv_name = max(csv_names, key=lambda n: zf.getinfo(n).file_size)

        with zf.open(csv_name) as f:
            raw_bytes = f.read(500_000)

        text, encoding_used = decode_bytes(raw_bytes)
        candidates = [";", ",", "\t", "|"]

        best = None
        for delim in candidates:
            reader = csv.reader(io.StringIO(text), delimiter=delim)
            header = next(reader, [])
            rows = []
            for i, row in enumerate(reader):
                rows.append(row)
                if i >= 4:
                    break

            score = 0
            if len(header) > 1:
                score += len(header) * 10
            if rows:
                lengths = [len(r) for r in rows if r]
                if lengths:
                    common_len = max(set(lengths), key=lengths.count)
                    score += common_len * 5
                    if len(header) == common_len:
                        score += 20

            if best is None or score > best["score"]:
                best = {
                    "delimiter": delim,
                    "header": header,
                    "encoding": encoding_used,
                    "csv_name": csv_name,
                    "score": score,
                }

        if not best or len(best["header"]) <= 1:
            raise RuntimeError("Не вдалося визначити header/delimiter")

        return best


def iter_csv_rows_from_zip(zip_path: Path, encoding: str, delimiter: str, csv_name: str):
    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(csv_name) as f:
            text_stream = io.TextIOWrapper(f, encoding=encoding, newline="")
            reader = csv.DictReader(text_stream, delimiter=delimiter)
            for row in reader:
                yield {str(k): ("" if v is None else str(v)) for k, v in row.items()}


def load_watchlist(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    active = []
    for row in rows:
        if not truthy(row.get("is_active", "")):
            continue
        active.append({
            "id": str(row.get("id", "")).strip(),
            "label": str(row.get("label", "")).strip(),
            "entity_type": str(row.get("entity_type", "")).strip().lower(),
            "debtor_name": str(row.get("debtor_name", "")).strip(),
            "debtor_code_raw": str(row.get("debtor_code", "")).strip(),
            "debtor_code_norm": normalize_code(row.get("debtor_code", "")),
        })
    return active


def main():
    if not WATCHLIST_PATH.exists():
        raise FileNotFoundError("Не знайдено watchlist.json")

    watchlist = load_watchlist(WATCHLIST_PATH)
    watchlist_codes = {w["debtor_code_norm"] for w in watchlist if w["debtor_code_norm"]}

    erb_resource = resolve_resource_from_datapackage(ERB_DATAPACKAGE_URL)
    fetch_to_file(erb_resource["resource_path"], ERB_ZIP_PATH)
    meta = parse_header_and_delimiter_from_zip(ERB_ZIP_PATH)

    results = {
        "watchlist": watchlist,
        "watchlist_codes": sorted(watchlist_codes),
        "resource_name": erb_resource["resource_name"],
        "resource_path": erb_resource["resource_path"],
        "csv_name": meta["csv_name"],
        "delimiter": meta["delimiter"],
        "encoding": meta["encoding"],
        "header": meta["header"],
        "first_rows_raw": [],
        "rows_scanned": 0,
        "rows_with_nonempty_debtor_code": 0,
        "code_matches": [],
    }

    for i, row in enumerate(iter_csv_rows_from_zip(
        ERB_ZIP_PATH,
        meta["encoding"],
        meta["delimiter"],
        meta["csv_name"],
    )):
        results["rows_scanned"] += 1

        row_code_raw = row.get("DEBTOR_CODE", "")
        row_code_norm = normalize_code(row_code_raw)

        if row_code_norm:
            results["rows_with_nonempty_debtor_code"] += 1

        if i < 10:
            results["first_rows_raw"].append({
                "keys": list(row.keys()),
                "DEBTOR_NAME": row.get("DEBTOR_NAME", ""),
                "DEBTOR_CODE_raw": row_code_raw,
                "DEBTOR_CODE_norm": row_code_norm,
                "row": row,
            })

        if row_code_norm and row_code_norm in watchlist_codes:
            results["code_matches"].append({
                "DEBTOR_NAME": row.get("DEBTOR_NAME", ""),
                "DEBTOR_CODE_raw": row_code_raw,
                "DEBTOR_CODE_norm": row_code_norm,
                "VP_ORDERNUM": row.get("VP_ORDERNUM", ""),
                "ORG_NAME": row.get("ORG_NAME", ""),
                "PUBLISHER": row.get("PUBLISHER", ""),
                "VD_CAT": row.get("VD_CAT", ""),
            })

    with open("debug_erb_codes.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"rows_scanned={results['rows_scanned']}")
    print(f"rows_with_nonempty_debtor_code={results['rows_with_nonempty_debtor_code']}")
    print(f"watchlist_codes={len(results['watchlist_codes'])}")
    print(f"code_matches={len(results['code_matches'])}")
    print("Saved debug_erb_codes.json")


if __name__ == "__main__":
    main()
