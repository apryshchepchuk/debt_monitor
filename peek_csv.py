import csv
import io
import json
import sys
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request


ROWS_TO_READ = 30
ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "cp1251", "cp1252"]
DEFAULT_URL = "https://data.gov.ua/dataset/783b9b50-faba-4cc9-a393-60485e395b1d/resource/e6ea76c1-01f4-4bd0-a282-7d92d6ecc2a1/download/29-ex_csv_erb.zip"


def detect_delimiter(sample_text: str) -> str:
    candidates = [",", ";", "\t", "|"]
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters="".join(candidates))
        return dialect.delimiter
    except Exception:
        counts = {d: sample_text.count(d) for d in candidates}
        return max(counts, key=counts.get)


def decode_bytes(raw_bytes: bytes):
    last_error = None
    for enc in ENCODINGS_TO_TRY:
        try:
            return raw_bytes.decode(enc), enc
        except Exception as e:
            last_error = e
    raise RuntimeError(f"Не вдалося декодувати байти. Остання помилка: {last_error}")


def fetch_to_file(url: str, target_path: Path) -> Path:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        },
    )
    with urlopen(req) as response, open(target_path, "wb") as out:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    return target_path


def read_preview_from_csv_bytes(raw_bytes: bytes, source_name: str):
    text, encoding_used = decode_bytes(raw_bytes)
    text_stream = io.StringIO(text)

    sample = text_stream.read(10000)
    text_stream.seek(0)

    delimiter = detect_delimiter(sample)
    reader = csv.reader(text_stream, delimiter=delimiter)

    header = next(reader, [])
    rows = []

    for i, row in enumerate(reader, start=1):
        rows.append(row)
        if i >= ROWS_TO_READ:
            break

    return {
        "source_name": source_name,
        "encoding": encoding_used,
        "delimiter": delimiter,
        "header": header,
        "rows": rows,
    }


def process_zip(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("У ZIP не знайдено CSV-файлів.")

        csv_name = max(csv_names, key=lambda n: zf.getinfo(n).file_size)

        with zf.open(csv_name) as f:
            raw_bytes = f.read(2_000_000)

        result = read_preview_from_csv_bytes(raw_bytes, csv_name)
        result["container"] = str(zip_path)
        result["zip_csv_candidates"] = csv_names
        return result


def save_outputs(result: dict):
    print("\n=== META ===")
    print(f"container: {result['container']}")
    print(f"source_name: {result['source_name']}")
    print(f"encoding: {result['encoding']}")
    print(f"delimiter: {repr(result['delimiter'])}")

    print("\n=== HEADER ===")
    for idx, col in enumerate(result["header"], start=1):
        print(f"{idx:02d}. {col}")

    print("\n=== FIRST ROWS ===")
    for i, row in enumerate(result["rows"], start=1):
        print(f"\nRow {i}:")
        for j, value in enumerate(row, start=1):
            col_name = result["header"][j - 1] if j - 1 < len(result["header"]) else f"col_{j}"
            print(f"  {j:02d}. {col_name}: {value}")

    out_json = {
        "container": result["container"],
        "source_name": result["source_name"],
        "encoding": result["encoding"],
        "delimiter": result["delimiter"],
        "header": result["header"],
        "rows": result["rows"],
        "zip_csv_candidates": result.get("zip_csv_candidates", []),
    }

    with open("peek_result.json", "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    with open("peek_sample.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=result["delimiter"])
        if result["header"]:
            writer.writerow(result["header"])
        writer.writerows(result["rows"])

    print("\nСтворено файли:")
    print("- peek_result.json")
    print("- peek_sample.csv")


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL

    download_path = Path("source.zip")
    print(f"Завантаження: {url}")
    fetch_to_file(url, download_path)
    print(f"Файл завантажено: {download_path}")

    result = process_zip(download_path)
    save_outputs(result)


if __name__ == "__main__":
    main()
