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

ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "cp1251", "cp1252"]

# Налаштуй тут свої пошукові параметри
SEARCH_CODES = [
    "31117042",
]

SEARCH_NAME_PARTS = [
    "САНА",
    "САНА КО",
]


def normalize_text(value: str) -> str:
    value = str(value or "").strip().upper()
    value = value.replace("’", "'").replace("`", "'").replace("Ё", "Е")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_code(value: str) -> str:
    return re.sub(r"\D+", "", str(value or ""))


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


def main():
    search_codes = {normalize_code(x) for x in SEARCH_CODES if str(x).strip()}
    search_name_parts = [normalize_text(x) for x in SEARCH_NAME_PARTS if str(x).strip()]

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

    matches = []
    scanned = 0

    for row in iter_csv_rows_from_zip(
        zip_path=ERB_ZIP_PATH,
        encoding=meta["encoding"],
        delimiter=meta["delimiter"],
        csv_name=meta["csv_name"],
    ):
        scanned += 1

        debtor_name_raw = row.get("DEBTOR_NAME", "")
        debtor_name_norm = normalize_text(debtor_name_raw)
        debtor_code_norm = normalize_code(row.get("DEBTOR_CODE", ""))

        matched_by = []

        if debtor_code_norm and debtor_code_norm in search_codes:
            matched_by.append("code")

        for part in search_name_parts:
            if part and part in debtor_name_norm:
                matched_by.append(f"name_part:{part}")

        if matched_by:
            matches.append({
                "matched_by": matched_by,
                "DEBTOR_NAME": row.get("DEBTOR_NAME", ""),
                "DEBTOR_BIRTHDATE": row.get("DEBTOR_BIRTHDATE", ""),
                "DEBTOR_CODE": row.get("DEBTOR_CODE", ""),
                "PUBLISHER": row.get("PUBLISHER", ""),
                "ORG_NAME": row.get("ORG_NAME", ""),
                "ORG_PHONE_NUM": row.get("ORG_PHONE_NUM", ""),
                "EMP_FULL_FIO": row.get("EMP_FULL_FIO", ""),
                "EMP_PHONE_NUM": row.get("EMP_PHONE_NUM", ""),
                "EMAIL_ADDR": row.get("EMAIL_ADDR", ""),
                "VP_ORDERNUM": row.get("VP_ORDERNUM", ""),
                "VD_CAT": row.get("VD_CAT", ""),
            })

    out = {
        "scanned_rows": scanned,
        "matches_found": len(matches),
        "search_codes": sorted(search_codes),
        "search_name_parts": search_name_parts,
        "resource_name": erb_resource["resource_name"],
        "resource_path": erb_resource["resource_path"],
        "csv_name": meta["csv_name"],
        "delimiter": meta["delimiter"],
        "encoding": meta["encoding"],
        "matches": matches,
    }

    with open("debug_erb_matches.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Scanned rows: {scanned}")
    print(f"Matches found: {len(matches)}")
    print("Saved debug_erb_matches.json")


if __name__ == "__main__":
    main()
