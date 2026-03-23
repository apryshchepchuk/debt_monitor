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


def short_name_variants(name: str) -> set[str]:
    """
    Дає кілька грубих нормалізованих варіантів назви юрособи
    для дебагу, а не для фінального продакшен-матчингу.
    """
    n = normalize_text(name)
    variants = {n}

    replacements = [
        ("ПРИВАТНЕ ПІДПРИЄМСТВО", "ПП"),
        ("ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ", "ТОВ"),
        ("АКЦІОНЕРНЕ ТОВАРИСТВО", "АТ"),
        ("ПУБЛІЧНЕ АКЦІОНЕРНЕ ТОВАРИСТВО", "ПАТ"),
        ("ПРИВАТНЕ АКЦІОНЕРНЕ ТОВАРИСТВО", "ПрАТ".upper()),
    ]

    for src, dst in replacements:
        if src in n:
            variants.add(n.replace(src, dst))

    # варіант без оргформи
    no_prefix = re.sub(
        r"^(ПРИВАТНЕ ПІДПРИЄМСТВО|ПП|ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ|ТОВ|АКЦІОНЕРНЕ ТОВАРИСТВО|АТ|ПУБЛІЧНЕ АКЦІОНЕРНЕ ТОВАРИСТВО|ПАТ|ПРИВАТНЕ АКЦІОНЕРНЕ ТОВАРИСТВО|ПРАТ)\s+",
        "",
        n,
    ).strip()
    if no_prefix:
        variants.add(no_prefix)

    return {v for v in variants if v}


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
                return response.read().decode("utf-8")
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
            raise RuntimeError(f"Не вдалося коректно визначити header/delimiter для {zip_path}")

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

    prepared = []
    for row in rows:
        entity_type = str(row.get("entity_type", "")).strip().lower()
        debtor_name = str(row.get("debtor_name", "")).strip()

        prepared.append({
            "id": str(row.get("id", "")).strip(),
            "is_active": truthy(row.get("is_active", "")),
            "entity_type": entity_type,
            "label": str(row.get("label", "")).strip(),
            "debtor_name": debtor_name,
            "debtor_name_norm": normalize_text(debtor_name),
            "debtor_name_variants": sorted(short_name_variants(debtor_name)),
            "debtor_code": normalize_code(row.get("debtor_code", "")),
            "birthdate": normalize_birthdate(row.get("birthdate", "")),
            "notes": str(row.get("notes", "")).strip(),
        })

    active = [r for r in prepared if r["is_active"]]
    print(f"Loaded watchlist rows: {len(active)} active")
    return active


def row_brief(row: dict) -> dict:
    return {
        "DEBTOR_NAME": row.get("DEBTOR_NAME", ""),
        "DEBTOR_BIRTHDATE": row.get("DEBTOR_BIRTHDATE", ""),
        "DEBTOR_CODE": row.get("DEBTOR_CODE", ""),
        "PUBLISHER": row.get("PUBLISHER", ""),
        "ORG_NAME": row.get("ORG_NAME", ""),
        "VP_ORDERNUM": row.get("VP_ORDERNUM", ""),
        "VD_CAT": row.get("VD_CAT", ""),
    }


def append_limited(bucket: list, item: dict, limit: int = 100):
    if len(bucket) < limit:
        bucket.append(item)


def main():
    if not WATCHLIST_PATH.exists():
        raise FileNotFoundError("Не знайдено watchlist.json. Спочатку запусти workflow отримання watchlist.")

    watchlist = load_watchlist(WATCHLIST_PATH)
    if not watchlist:
        raise RuntimeError("Watchlist порожній або немає активних записів.")

    print("Resolving datapackage for ERB...")
    erb_resource = resolve_resource_from_datapackage(ERB_DATAPACKAGE_URL)
    print(f"ERB resource: {erb_resource['resource_name']}")
    print(f"ERB ZIP URL: {erb_resource['resource_path']}")

    print("Downloading ERB ZIP...")
    fetch_to_file(erb_resource["resource_path"], ERB_ZIP_PATH)

    meta = parse_header_and_delimiter_from_zip(ERB_ZIP_PATH)
    print(
        f"Processing ERB: csv_name={meta['csv_name']}, "
        f"delimiter={meta['delimiter']}, encoding={meta['encoding']}"
    )

    results = {
        "resource_name": erb_resource["resource_name"],
        "resource_path": erb_resource["resource_path"],
        "csv_name": meta["csv_name"],
        "delimiter": meta["delimiter"],
        "encoding": meta["encoding"],
        "scanned_rows": 0,
        "watchlist_debug": [],
    }

    debug_map = {}
    for w in watchlist:
        debug_map[w["id"]] = {
            "watchlist_id": w["id"],
            "label": w["label"],
            "entity_type": w["entity_type"],
            "debtor_name": w["debtor_name"],
            "debtor_name_norm": w["debtor_name_norm"],
            "debtor_name_variants": w["debtor_name_variants"],
            "debtor_code": w["debtor_code"],
            "birthdate": w["birthdate"],
            "strong_code_matches_count": 0,
            "exact_name_matches_count": 0,
            "partial_name_matches_count": 0,
            "strong_code_matches_sample": [],
            "exact_name_matches_sample": [],
            "partial_name_matches_sample": [],
        }

    for row in iter_csv_rows_from_zip(
        zip_path=ERB_ZIP_PATH,
        encoding=meta["encoding"],
        delimiter=meta["delimiter"],
        csv_name=meta["csv_name"],
    ):
        results["scanned_rows"] += 1

        row_name_norm = normalize_text(row.get("DEBTOR_NAME", ""))
        row_code_norm = normalize_code(row.get("DEBTOR_CODE", ""))
        row_birthdate = normalize_birthdate(row.get("DEBTOR_BIRTHDATE", ""))

        for w in watchlist:
            dbg = debug_map[w["id"]]

            # 1. strong code match
            if w["entity_type"] == "company" and w["debtor_code"] and row_code_norm and w["debtor_code"] == row_code_norm:
                dbg["strong_code_matches_count"] += 1
                append_limited(
                    dbg["strong_code_matches_sample"],
                    {
                        "matched_by": "code",
                        **row_brief(row),
                    },
                )

            # 2. exact name match
            if w["debtor_name_norm"] and row_name_norm == w["debtor_name_norm"]:
                dbg["exact_name_matches_count"] += 1
                append_limited(
                    dbg["exact_name_matches_sample"],
                    {
                        "matched_by": "exact_name",
                        **row_brief(row),
                    },
                )

            # 3. partial name match
            partial_hit = False
            for variant in w["debtor_name_variants"]:
                if not variant:
                    continue

                if len(variant) >= 6 and (variant in row_name_norm or row_name_norm in variant):
                    partial_hit = True
                    break

            # для фізосіб не засмічуємо partial надто широко
            if w["entity_type"] == "person" and len(w["debtor_name_norm"]) < 10:
                partial_hit = False

            if partial_hit and row_name_norm != w["debtor_name_norm"]:
                dbg["partial_name_matches_count"] += 1
                append_limited(
                    dbg["partial_name_matches_sample"],
                    {
                        "matched_by": "partial_name",
                        **row_brief(row),
                    },
                )

    results["watchlist_debug"] = list(debug_map.values())

    with open("debug_erb_watchlist.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Scanned rows: {results['scanned_rows']}")
    print("Saved debug_erb_watchlist.json")

    for item in results["watchlist_debug"]:
        print(
            f"[{item['watchlist_id']}] {item['label']} | "
            f"code={item['strong_code_matches_count']} | "
            f"exact_name={item['exact_name_matches_count']} | "
            f"partial_name={item['partial_name_matches_count']}"
        )


if __name__ == "__main__":
    main()
