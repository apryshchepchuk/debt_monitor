from __future__ import annotations

import csv
import io
import json
import re
import zipfile
import hashlib
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import urlopen, Request


ERB_DATAPACKAGE_URL = "https://data.gov.ua/dataset/506734bf-2480-448c-a2b4-90b6d06df11e/datapackage"
ERB_FALLBACK_ZIP_URL = "https://data.gov.ua/dataset/783b9b50-faba-4cc9-a393-60485e395b1d/resource/e6ea76c1-01f4-4bd0-a282-7d92d6ecc2a1/download/31-ex_csv_erb.zip"
ERB_NAIS_PAGE_URL = "https://nais.gov.ua/m/ediniy-reestr-borjnikiv-549"

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


def pick_field(row: dict, *names: str) -> str:
    for name in names:
        if name in row:
            value = row.get(name, "")
            text = "" if value is None else str(value).strip()
            if text:
                return text
    return ""


def fetch_text(url: str, retries: int = 6, timeout: int = 180) -> str:
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
                return raw.decode("utf-8", errors="replace")
        except Exception as e:
            last_error = e
            print(f"Fetch failed: {e}")
            if attempt < retries:
                time.sleep(10 * attempt)

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

            with urlopen(req, timeout=timeout) as response:
                content_type = response.headers.get("Content-Type", "")
                print(f"Content-Type: {content_type}")
                data = response.read()

            with open(target_path, "wb") as out:
                out.write(data)

            if len(data) < 2 or data[:2] != b"PK":
                preview_path = target_path.with_suffix(".preview.txt")
                try:
                    preview_text = data[:3000].decode("utf-8", errors="replace")
                except Exception:
                    preview_text = repr(data[:3000])

                with open(preview_path, "w", encoding="utf-8") as f:
                    f.write(preview_text)

                raise RuntimeError(
                    f"Downloaded content is not a ZIP. "
                    f"Content-Type={content_type}. Preview saved to {preview_path}"
                )

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
                time.sleep(10 * attempt)

    raise RuntimeError(f"Не вдалося коректно завантажити файл {url}: {last_error}")


def resolve_resource_from_datapackage(datapackage_url: str) -> dict:
    raw = fetch_text(datapackage_url)
    data = json.loads(raw)

    resources = data.get("resources", [])
    if not resources:
        raise RuntimeError(f"У datapackage немає resources: {datapackage_url}")

    zip_resources = [
        r for r in resources
        if str(r.get("format", "")).upper() == "ZIP"
        or str(r.get("path", "")).lower().endswith(".zip")
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
        "resource_name": name or "datapackage_resource",
        "resource_path": path,
        "used_fallback": False,
        "fallback_source": "",
    }


def resolve_resource_from_nais_page(page_url: str) -> dict:
    html = fetch_text(page_url, retries=4, timeout=120)
    html = unescape(html)

    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)

    candidates = []
    for href in hrefs:
        full_url = urljoin(page_url, href)
        lower = full_url.lower()

        if "ex_csv_erb.zip" in lower and "struct" not in lower:
            candidates.append(full_url)

    if not candidates:
        text_candidates = re.findall(
            r'https?://[^\s"\'<>]+ex_csv_erb\.zip',
            html,
            flags=re.IGNORECASE,
        )
        candidates.extend(text_candidates)

    unique = []
    seen = set()
    for url in candidates:
        if url not in seen:
            seen.add(url)
            unique.append(url)

    if not unique:
        raise RuntimeError(f"На сторінці NAIS не знайдено актуального ZIP для ЄРБ: {page_url}")

    def score(url: str) -> tuple[int, int]:
        m = re.search(r'(\d+)-ex_csv_erb\.zip', url, flags=re.IGNORECASE)
        prefix_num = int(m.group(1)) if m else -1
        return (prefix_num, -len(url))

    best_url = sorted(unique, key=score, reverse=True)[0]
    best_name = best_url.rstrip("/").split("/")[-1]

    return {
        "dataset_title": "Єдиний реєстр боржників (NAIS page fallback)",
        "resource_name": best_name,
        "resource_path": best_url,
        "used_fallback": True,
        "fallback_source": "nais_page",
    }


def download_erb_zip(target_path: Path) -> dict:
    last_error = None

    try:
        print("Resolving datapackage for ERB...")
        resource = resolve_resource_from_datapackage(ERB_DATAPACKAGE_URL)
        print(f"ERB resource: {resource['resource_name']}")
        print(f"ERB ZIP URL: {resource['resource_path']}")
        print("Downloading ERB ZIP from data.gov.ua...")
        fetch_to_file(resource["resource_path"], target_path)
        return resource
    except Exception as e:
        last_error = e
        print(f"Primary data.gov.ua source failed: {e}")

    try:
        print(f"Trying old direct fallback ZIP URL: {ERB_FALLBACK_ZIP_URL}")
        fallback_resource = {
            "dataset_title": "ERB direct fallback resource",
            "resource_name": ERB_FALLBACK_ZIP_URL.rstrip('/').split('/')[-1],
            "resource_path": ERB_FALLBACK_ZIP_URL,
            "used_fallback": True,
            "fallback_source": "direct_data_gov_zip",
        }
        fetch_to_file(fallback_resource["resource_path"], target_path)
        return fallback_resource
    except Exception as e:
        last_error = e
        print(f"Old direct fallback ZIP failed: {e}")

    print("Trying NAIS page fallback...")
    nais_resource = resolve_resource_from_nais_page(ERB_NAIS_PAGE_URL)
    print(f"NAIS resource: {nais_resource['resource_name']}")
    print(f"NAIS ZIP URL: {nais_resource['resource_path']}")
    fetch_to_file(nais_resource["resource_path"], target_path)
    return nais_resource


def decode_bytes(raw_bytes: bytes):
    last_error = None
    for enc in ENCODINGS_TO_TRY:
        try:
            return raw_bytes.decode(enc), enc
        except Exception as e:
            last_error = e
    raise RuntimeError(f"Не вдалося декодувати байти. Остання помилка: {last_error}")


def parse_erb_layout_from_zip(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"У ZIP не знайдено CSV: {zip_path}")

        csv_name = max(csv_names, key=lambda n: zf.getinfo(n).file_size)

        with zf.open(csv_name) as f:
            raw_bytes = f.read(300_000)

        text, encoding_used = decode_bytes(raw_bytes)
        lines = text.splitlines()
        if len(lines) < 2:
            raise RuntimeError("У CSV недостатньо рядків для аналізу")

        header_line = lines[0]
        first_data_lines = [ln for ln in lines[1:6] if ln.strip()]

        header = next(csv.reader([header_line], delimiter=";", quotechar='"'))
        header_len = len(header)

        row_lengths_comma = []
        for ln in first_data_lines:
            parsed = next(csv.reader([ln], delimiter=",", quotechar='"'))
            row_lengths_comma.append(len(parsed))

        same_len_count = sum(1 for x in row_lengths_comma if x == header_len)

        if header_len <= 1 or same_len_count == 0:
            raise RuntimeError(
                f"Не вдалося підтвердити формат ЄРБ: header_len={header_len}, "
                f"row_lengths_comma={row_lengths_comma}"
            )

        print(
            f"ERB layout confirmed: header_delim=';'; row_delim=','; "
            f"header_len={header_len}; same_len_count={same_len_count}"
        )

        return {
            "csv_name": csv_name,
            "encoding": encoding_used,
            "header": header,
            "header_delimiter": ";",
            "row_delimiter": ",",
        }


def iter_erb_rows_from_zip(zip_path: Path, encoding: str, csv_name: str, header: list[str]):
    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(csv_name) as f:
            text_stream = io.TextIOWrapper(f, encoding=encoding, newline="")
            header_line = text_stream.readline()
            if not header_line:
                return

            header_len = len(header)
            reader = csv.reader(text_stream, delimiter=",", quotechar='"')

            for row in reader:
                if row is None:
                    continue

                if not any(str(v).strip() for v in row):
                    continue

                if len(row) < header_len:
                    row = row + [""] * (header_len - len(row))
                elif len(row) > header_len:
                    row = row[:header_len - 1] + [",".join(row[header_len - 1:])]

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


def build_watchlist_index(watchlist: list) -> dict:
    index = {
        "company_by_code": {},
        "company_by_name": {},
        "person_by_name_birthdate": {},
        "person_by_name": {},
    }

    for w in watchlist:
        entity_type = w.get("entity_type", "")
        name = w.get("debtor_name_norm", "")
        code = w.get("debtor_code", "")
        birthdate = w.get("birthdate", "")

        if entity_type == "company":
            if code:
                index["company_by_code"].setdefault(code, []).append(w)

            if name:
                index["company_by_name"].setdefault(name, []).append(w)

        elif entity_type == "person":
            if name and birthdate:
                key = f"{name}|{birthdate}"
                index["person_by_name_birthdate"].setdefault(key, []).append(w)

            if name:
                index["person_by_name"].setdefault(name, []).append(w)

    print(
        "Watchlist index built: "
        f"company_by_code={len(index['company_by_code'])}, "
        f"company_by_name={len(index['company_by_name'])}, "
        f"person_by_name_birthdate={len(index['person_by_name_birthdate'])}, "
        f"person_by_name={len(index['person_by_name'])}"
    )

    return index


def match_watchlist_indexed(row: dict, watchlist_index: dict):
    row_name = normalize_text(pick_field(row, "DEBTOR_NAME"))
    row_code = normalize_code(pick_field(row, "DEBTOR_CODE"))
    row_birthdate = normalize_birthdate(pick_field(row, "DEBTOR_BIRTHDATE", "BIRTHDATE"))

    matches = []
    matched_ids = set()

    def add_match(w, strength: str):
        key = str(w.get("id", ""))
        if key in matched_ids:
            return
        matched_ids.add(key)
        matches.append((w, strength))

    if row_code:
        for w in watchlist_index["company_by_code"].get(row_code, []):
            add_match(w, "strong")

    if row_name:
        for w in watchlist_index["company_by_name"].get(row_name, []):
            add_match(w, "weak")

    if row_name and row_birthdate:
        person_key = f"{row_name}|{row_birthdate}"
        for w in watchlist_index["person_by_name_birthdate"].get(person_key, []):
            add_match(w, "strong")

    if row_name:
        for w in watchlist_index["person_by_name"].get(row_name, []):
            w_birthdate = w.get("birthdate", "")

            if w_birthdate and row_birthdate and w_birthdate != row_birthdate:
                continue

            if not w_birthdate or not row_birthdate:
                add_match(w, "weak")

    return matches


def stable_hash(data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_erb_record(watchlist_item: dict, match_strength: str, row: dict, source_date: str):
    record = {
        "watchlist_id": watchlist_item["id"],
        "match_strength": match_strength,
        "debtor_name": pick_field(row, "DEBTOR_NAME"),
        "debtor_birthdate": normalize_birthdate(pick_field(row, "DEBTOR_BIRTHDATE", "BIRTHDATE")),
        "debtor_code": normalize_code(pick_field(row, "DEBTOR_CODE")),
        "publisher": pick_field(row, "PUBLISHER"),
        "org_name": pick_field(row, "ORG_NAME", "EMP_ORG"),
        "org_phone_num": pick_field(row, "ORG_PHONE_NUM", "ORG_PHONE"),
        "emp_full_fio": pick_field(row, "EMP_FULL_FIO"),
        "emp_phone_num": pick_field(row, "EMP_PHONE_NUM"),
        "email_addr": pick_field(row, "EMAIL_ADDR"),
        "vp_ordernum": pick_field(row, "VP_ORDERNUM", "VP_ORDER_NUM"),
        "vd_cat": pick_field(row, "VD_CAT"),
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
    meta = parse_erb_layout_from_zip(zip_path)
    matches = []
    scanned = 0
    watchlist_index = build_watchlist_index(watchlist)

    print(
        f"Processing ERB: csv_name={meta['csv_name']}, "
        f"encoding={meta['encoding']}, header_delim=';', row_delim=','"
    )
    print("HEADER:", meta["header"])

    for idx, row in enumerate(iter_erb_rows_from_zip(
        zip_path=zip_path,
        encoding=meta["encoding"],
        csv_name=meta["csv_name"],
        header=meta["header"],
    )):
        if idx < 3:
            print("ROW SAMPLE", idx + 1, row)

        scanned += 1
        found = match_watchlist_indexed(row, watchlist_index)
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
            f"resource_path={resource_meta.get('resource_path', '')}; "
            f"encoding={meta['encoding']}; "
            f"csv_name={meta['csv_name']}; "
            f"header_delim=; ; row_delim=,; "
            f"used_fallback={resource_meta.get('used_fallback', False)}; "
            f"fallback_source={resource_meta.get('fallback_source', '')}"
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

    erb_resource = download_erb_zip(ERB_ZIP_PATH)
    print(f"Final ERB resource: {erb_resource['resource_name']}")
    print(f"Final ERB ZIP URL: {erb_resource['resource_path']}")
    print(f"Used fallback: {erb_resource['used_fallback']}")
    print(f"Fallback source: {erb_resource.get('fallback_source', '')}")

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
