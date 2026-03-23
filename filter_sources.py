import csv
import io
import json
import re
import zipfile
import hashlib
import time
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request


ERB_DATAPACKAGE_URL = "https://data.gov.ua/dataset/506734bf-2480-448c-a2b4-90b6d06df11e/datapackage"

WATCHLIST_PATH = Path("watchlist.json")
ERB_ZIP_PATH = Path("erb.zip")

ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "cp1251", "cp1252"]


def normalize_text(value: str) -> str:
    value = str(value or "").strip().upper()
    value = value.replace("’", "'").replace("`", "'").replace("Ё", "Е")
    value = value.replace('"', "")
    value = value.replace("«", "").replace("»", "")
    value = value.replace("-", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_code(value: str) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def normalize_birthdate(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    match = re.match(r"^(\d{2}\.\d{2}\.\d{4})", value)
    return match.group(1) if match else value


def truthy(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def fetch_text(url: str, retries: int = 4, timeout: int = 90) -> str:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"Fetching text (attempt {attempt}/{retries}): {url}")
            req = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                    "Cache-Control": "no-cache",
                },
            )
            with urlopen(req, timeout=timeout) as response:
                raw = response.read()
                return raw.decode("utf-8")
        except Exception as e:
            last_error = e
            print(f"Fetch failed: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(f"Не вдалося завантажити текст із {url}: {last_error}")


def fetch_to_file(url: str, target_path: Path, retries: int = 3, timeout: int = 300) -> Path:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"Downloading file (attempt {attempt}/{retries}): {url}")
            req = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                    "Cache-Control": "no-cache",
                },
            )
            with urlopen(req, timeout=timeout) as response, open(target_path, "wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)

            if target_path.suffix.lower() == ".zip":
                with zipfile.ZipFile(target_path, "r") as zf:
                    bad_member = zf.testzip()
                    if bad_member is not None:
                        raise zipfile.BadZipFile(f"CRC failed for member: {bad_member}")

            return target_path

        except Exception as e:
            last_error = e
            print(f"Download failed: {e}")
            if target_path.exists():
                target_path.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(f"Не вдалося коректно завантажити файл {url}: {last_error}")


def resolve_resource_from_datapackage(datapackage_url: str) -> dict:
    raw = fetch_text(datapackage_url)
    data = json.loads(raw)

    resources = data.get("resources", [])
    if not resources:
        raise RuntimeError(f"У datapackage немає resources: {datapackage_url}")

    zip_resources = [
        r for r in resources
        if str(r.get("format", "")).upper() == "ZIP" or str(r.get("path", "")).lower().endswith(".zip")
    ]
    if not zip_resources:
        raise RuntimeError(f"У datapackage немає ZIP-ресурсу: {datapackage_url}")

    resource = zip_resources[0]
    path = resource.get("path", "")
    name = resource.get("name", "")

    if not path:
        raise RuntimeError(f"У ресурсі немає path: {datapackage_url}")

    return {
        "dataset_title": data.get("title", ""),
        "resource_name": name,
        "resource_path": path,
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
            raise RuntimeError(f"У ZIP не знайдено CSV: {zip_path}")

        csv_name = max(csv_names, key=lambda n: zf.getinfo(n).file_size)

        with zf.open(csv_name) as f:
            raw_bytes = f.read(1_000_000)

        text, encoding_used = decode_bytes(raw_bytes)
        candidates = [";", ",", "\t", "|"]

        best = None

        for delim in candidates:
            reader = csv.reader(io.StringIO(text), delimiter=delim, quotechar='"')

            try:
                header = next(reader, [])
            except Exception:
                continue

            sample_rows = []
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i >= 19:
                    break

            if not header:
                continue

            header_len = len(header)
            if header_len <= 1:
                score = -1000
            else:
                row_lengths = [len(r) for r in sample_rows if r]
                same_len_count = sum(1 for x in row_lengths if x == header_len)
                nonempty_count = sum(1 for r in sample_rows if any(str(v).strip() for v in r))

                score = 0
                score += header_len * 20
                score += same_len_count * 50
                score += nonempty_count * 5

                # штраф, якщо рядок даних "злипся" в першу колонку
                if sample_rows:
                    suspicious = 0
                    for r in sample_rows[:10]:
                        if len(r) == header_len:
                            first_cell = str(r[0])
                            rest_nonempty = any(str(x).strip() for x in r[1:])
                            if (";" in first_cell or "," in first_cell) and not rest_nonempty:
                                suspicious += 1
                    score -= suspicious * 100

            candidate = {
                "delimiter": delim,
                "header": header,
                "encoding": encoding_used,
                "csv_name": csv_name,
                "score": score,
                "header_len": len(header),
                "sample_rows": sample_rows[:5],
            }

            if best is None or candidate["score"] > best["score"]:
                best = candidate

        if not best or best["header_len"] <= 1:
            raise RuntimeError(f"Не вдалося коректно визначити header/delimiter для {zip_path}")

        print(
            f"Chosen delimiter for {zip_path.name}: {repr(best['delimiter'])}; "
            f"header_len={best['header_len']}; score={best['score']}"
        )

        return {
            "delimiter": best["delimiter"],
            "header": best["header"],
            "encoding": best["encoding"],
            "csv_name": best["csv_name"],
        }


def iter_csv_rows_from_zip(zip_path: Path, encoding: str, delimiter: str, csv_name: str):
    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(csv_name) as f:
            text_stream = io.TextIOWrapper(f, encoding=encoding, newline="")
            reader = csv.reader(text_stream, delimiter=delimiter, quotechar='"')

            header = next(reader, None)
            if not header:
                return

            header = [str(h) for h in header]
            header_len = len(header)

            for row in reader:
                if row is None:
                    continue

                if not any(str(v).strip() for v in row):
                    continue

                if len(row) < header_len:
                    row = row + [""] * (header_len - len(row))
                elif len(row) > header_len:
                    row = row[:header_len - 1] + [delimiter.join(row[header_len - 1:])]

                yield {header[i]: ("" if row[i] is None else str(row[i])) for i in range(header_len)}


def load_watchlist(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    prepared = []
    for row in rows:
        entity_type = str(row.get("entity_type", "")).strip().lower()
        prepared.append({
            "id": str(row.get("id", "")).strip(),
            "is_active": truthy(row.get("is_active", "")),
            "entity_type": entity_type,
            "label": str(row.get("label", "")).strip(),
            "debtor_name": str(row.get("debtor_name", "")).strip(),
            "debtor_name_norm": normalize_text(row.get("debtor_name", "")),
            "debtor_code": normalize_code(row.get("debtor_code", "")),
            "birthdate": normalize_birthdate(row.get("birthdate", "")),
            "notes": str(row.get("notes", "")).strip(),
        })

    active = [r for r in prepared if r["is_active"]]
    print(f"Loaded watchlist rows: {len(active)} active")
    return active


def match_watchlist(row: dict, watchlist: list):
    row_name = normalize_text(row.get("DEBTOR_NAME", ""))
    row_code = normalize_code(row.get("DEBTOR_CODE", ""))
    row_birthdate = normalize_birthdate(row.get("DEBTOR_BIRTHDATE", ""))

    matches = []

    for w in watchlist:
        if w["entity_type"] == "company":
            if w["debtor_code"] and row_code and w["debtor_code"] == row_code:
                matches.append((w, "strong"))
                continue

            if w["debtor_name_norm"] and w["debtor_name_norm"] == row_name:
                matches.append((w, "weak"))
                continue

        elif w["entity_type"] == "person":
            if w["debtor_name_norm"] and w["debtor_name_norm"] == row_name:
                if w["birthdate"] and row_birthdate and w["birthdate"] == row_birthdate:
                    matches.append((w, "strong"))
                elif not w["birthdate"] or not row_birthdate:
                    matches.append((w, "weak"))

    return matches


def stable_hash(data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_erb_record(watchlist_item: dict, match_strength: str, row: dict, source_date: str):
    record = {
        "watchlist_id": watchlist_item["id"],
        "match_strength": match_strength,
        "debtor_name": row.get("DEBTOR_NAME", "").strip(),
        "debtor_birthdate": normalize_birthdate(row.get("DEBTOR_BIRTHDATE", "")),
        "debtor_code": normalize_code(row.get("DEBTOR_CODE", "")),
        "publisher": row.get("PUBLISHER", "").strip(),
        "org_name": row.get("ORG_NAME", "").strip(),
        "org_phone_num": row.get("ORG_PHONE_NUM", "").strip(),
        "emp_full_fio": row.get("EMP_FULL_FIO", "").strip(),
        "emp_phone_num": row.get("EMP_PHONE_NUM", "").strip(),
        "email_addr": row.get("EMAIL_ADDR", "").strip(),
        "vp_ordernum": row.get("VP_ORDERNUM", "").strip(),
        "vd_cat": row.get("VD_CAT", "").strip(),
        "source_date": source_date,
        "row_hash": "",
        "first_seen": source_date,
        "last_seen": source_date,
        "is_active": "true",
    }
    record["row_hash"] = stable_hash(record)
    return record


def dedupe_records(records: list):
    seen = set()
    result = []
    for r in records:
        h = r["row_hash"]
        if h not in seen:
            seen.add(h)
            result.append(r)
    return result


def process_erb(zip_path: Path, watchlist: list, source_date: str, resource_meta: dict):
    meta = parse_header_and_delimiter_from_zip(zip_path)
    matches = []
    scanned = 0

    print(
        f"Processing ERB: "
        f"csv_name={meta['csv_name']}, delimiter={meta['delimiter']}, encoding={meta['encoding']}"
    )
    print("HEADER:", meta["header"])

    for idx, row in enumerate(iter_csv_rows_from_zip(
        zip_path=zip_path,
        encoding=meta["encoding"],
        delimiter=meta["delimiter"],
        csv_name=meta["csv_name"],
    )):
        if idx < 3:
            print("ROW SAMPLE", idx + 1, row)

        scanned += 1
        found = match_watchlist(row, watchlist)
        if not found:
            continue

        for watchlist_item, match_strength in found:
            matches.append(build_erb_record(watchlist_item, match_strength, row, source_date))

    matches = dedupe_records(matches)

    tech_row = {
        "run_at": source_date,
        "source_name": "erb",
        "status": "ok",
        "rows_scanned": str(scanned),
        "matches_found": str(len(matches)),
        "notes": (
            f"dataset_title={resource_meta.get('dataset_title', '')}; "
            f"resource_name={resource_meta.get('resource_name', '')}; "
            f"delimiter={meta['delimiter']}; "
            f"encoding={meta['encoding']}; "
            f"csv_name={meta['csv_name']}"
        ),
    }

    return matches, tech_row


def main():
    if not WATCHLIST_PATH.exists():
        raise FileNotFoundError("Не знайдено watchlist.json. Спочатку запусти workflow отримання watchlist.")

    watchlist = load_watchlist(WATCHLIST_PATH)
    if not watchlist:
        raise RuntimeError("Watchlist порожній або немає активних записів.")

    source_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("Resolving datapackage for ERB...")
    erb_resource = resolve_resource_from_datapackage(ERB_DATAPACKAGE_URL)
    print(f"ERB resource: {erb_resource['resource_name']}")
    print(f"ERB ZIP URL: {erb_resource['resource_path']}")

    print("Downloading ERB ZIP...")
    fetch_to_file(erb_resource["resource_path"], ERB_ZIP_PATH)

    print("Filtering ERB...")
    erb_rows, erb_tech = process_erb(ERB_ZIP_PATH, watchlist, source_date, erb_resource)

    with open("filtered_erb.json", "w", encoding="utf-8") as f:
        json.dump(erb_rows, f, ensure_ascii=False, indent=2)

    with open("tech_rows.json", "w", encoding="utf-8") as f:
        json.dump([erb_tech], f, ensure_ascii=False, indent=2)

    print(f"ERB matches: {len(erb_rows)}")
    print("Saved filtered_erb.json, tech_rows.json")


if __name__ == "__main__":
    main()
