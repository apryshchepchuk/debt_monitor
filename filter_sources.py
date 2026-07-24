from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import time
import zipfile
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen


ERB_DATAPACKAGE_URL = (
    "https://data.gov.ua/dataset/"
    "506734bf-2480-448c-a2b4-90b6d06df11e/datapackage"
)
ERB_NAIS_PAGE_URL = (
    "https://nais.gov.ua/m/ediniy-reestr-borjnikiv-549"
)

WATCHLIST_PATH = Path("watchlist.json")
ERB_ZIP_PATH = Path("erb.zip")

ENCODINGS_TO_TRY = [
    "utf-8-sig",
    "utf-8",
    "cp1251",
    "cp1252",
]

DELIMITERS_TO_TRY = [
    ",",
    ";",
    "\t",
    "|",
]

EXPECTED_ERB_HEADER_FIELDS = {
    "DEBTOR_NAME",
    "DEBTOR_CODE",
    "DEBTOR_BIRTHDATE",
    "BIRTHDATE",
    "PUBLISHER",
    "EMP_FULL_FIO",
    "EMP_ORG",
    "ORG_NAME",
    "ORG_PHONE",
    "ORG_PHONE_NUM",
    "EMAIL_ADDR",
    "VP_ORDERNUM",
    "VP_ORDER_NUM",
    "VD_CAT",
}


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
    return str(value).strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
    }


def pick_field(row: dict, *names: str) -> str:
    for name in names:
        if name not in row:
            continue

        value = row.get(name, "")
        text = "" if value is None else str(value).strip()

        if text:
            return text

    return ""


def fetch_text(
    url: str,
    retries: int = 3,
    timeout: int = 180,
) -> str:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(
                f"Fetching text "
                f"(attempt {attempt}/{retries}): {url}"
            )

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
                return raw.decode(
                    "utf-8",
                    errors="replace",
                )

        except Exception as error:
            last_error = error
            print(f"Fetch failed: {error}")

            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(
        f"Не вдалося завантажити текст із {url}: "
        f"{last_error}"
    )


def fetch_to_file(
    url: str,
    target_path: Path,
    retries: int = 3,
    timeout: int = 300,
) -> Path:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(
                f"Downloading file "
                f"(attempt {attempt}/{retries}): {url}"
            )

            req = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                    "Cache-Control": "no-cache",
                },
            )

            with urlopen(req, timeout=timeout) as response:
                content_type = response.headers.get(
                    "Content-Type",
                    "",
                )

                print(f"Content-Type: {content_type}")
                data = response.read()

            with open(target_path, "wb") as output_file:
                output_file.write(data)

            if len(data) < 2 or data[:2] != b"PK":
                preview_path = target_path.with_suffix(
                    ".preview.txt"
                )

                try:
                    preview_text = data[:3000].decode(
                        "utf-8",
                        errors="replace",
                    )
                except Exception:
                    preview_text = repr(data[:3000])

                with open(
                    preview_path,
                    "w",
                    encoding="utf-8",
                ) as preview_file:
                    preview_file.write(preview_text)

                raise RuntimeError(
                    "Downloaded content is not a ZIP. "
                    f"Content-Type={content_type}. "
                    f"Preview saved to {preview_path}"
                )

            # Легка перевірка ZIP без повного testzip().
            # Якщо архів пошкоджений настільки, що його неможливо
            # прочитати, помилка виникне тут або під час читання CSV.
            with zipfile.ZipFile(target_path, "r") as archive:
                entries = archive.namelist()

                if not entries:
                    raise RuntimeError(
                        "Завантажений ZIP-архів порожній"
                    )

            return target_path

        except Exception as error:
            last_error = error
            print(f"Download failed: {error}")

            if target_path.exists():
                target_path.unlink(missing_ok=True)

            if attempt < retries:
                time.sleep(10 * attempt)

    raise RuntimeError(
        f"Не вдалося коректно завантажити файл {url}: "
        f"{last_error}"
    )


def resolve_resource_from_datapackage(
    datapackage_url: str,
) -> dict:
    # Лише одна спроба для data.gov.ua/datapackage.
    raw = fetch_text(
        datapackage_url,
        retries=1,
        timeout=90,
    )

    data = json.loads(raw)

    resources = data.get("resources", [])

    if not resources:
        raise RuntimeError(
            f"У datapackage немає resources: "
            f"{datapackage_url}"
        )

    zip_resources = [
        resource
        for resource in resources
        if (
            str(resource.get("format", "")).upper() == "ZIP"
            or str(resource.get("path", ""))
            .lower()
            .endswith(".zip")
        )
    ]

    if not zip_resources:
        raise RuntimeError(
            f"У datapackage немає ZIP-ресурсу: "
            f"{datapackage_url}"
        )

    resource = zip_resources[0]

    path = str(resource.get("path", "")).strip()
    name = str(resource.get("name", "")).strip()

    if not path:
        raise RuntimeError(
            f"У ресурсі немає path: {datapackage_url}"
        )

    return {
        "dataset_title": data.get("title", ""),
        "resource_name": (
            name or "datapackage_resource"
        ),
        "resource_path": path,
        "used_fallback": False,
        "fallback_source": "",
    }


def resolve_resource_from_nais_page(
    page_url: str,
) -> dict:
    html = fetch_text(
        page_url,
        retries=3,
        timeout=120,
    )
    html = unescape(html)

    candidates: list[tuple[str, str]] = []

    # 1. Пріоритетний варіант:
    # текст посилання містить ex_csv_erb.zip.
    anchor_pattern = re.compile(
        r'<a[^>]+href=["\']'
        r'([^"\']+\.zip[^"\']*)'
        r'["\'][^>]*>'
        r'\s*([^<]+)\s*</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )

    for href, text in anchor_pattern.findall(html):
        full_url = urljoin(
            page_url,
            href.strip(),
        )

        clean_text = " ".join(
            text.strip().split()
        )
        text_normalized = clean_text.lower()

        if (
            "ex_csv_erb.zip" in text_normalized
            and "struct" not in text_normalized
        ):
            candidates.append(
                (full_url, clean_text)
            )

    # 2. Резерв:
    # ZIP у блоці "гіперпосилання на набір даних".
    if not candidates:
        block_pattern = re.compile(
            r"гіперпосилання\s+на\s+набір\s+даних"
            r'.*?<a[^>]+href=["\']'
            r'([^"\']+\.zip[^"\']*)'
            r'["\'][^>]*>'
            r"\s*([^<]+)\s*</a>",
            flags=re.IGNORECASE | re.DOTALL,
        )

        for href, text in block_pattern.findall(html):
            full_url = urljoin(
                page_url,
                href.strip(),
            )

            clean_text = " ".join(
                text.strip().split()
            )
            text_normalized = clean_text.lower()

            if "struct" not in text_normalized:
                candidates.append(
                    (full_url, clean_text)
                )

    # 3. Додатковий резерв:
    # будь-який ZIP у files/general, крім structure.
    if not candidates:
        generic_zip_pattern = re.compile(
            r'href=["\']'
            r'([^"\']*files/general/[^"\']+\.zip[^"\']*)'
            r'["\']',
            flags=re.IGNORECASE,
        )

        for href in generic_zip_pattern.findall(html):
            full_url = urljoin(
                page_url,
                href.strip(),
            )

            if "struct" in full_url.lower():
                continue

            candidates.append(
                (
                    full_url,
                    full_url.rstrip("/").split("/")[-1],
                )
            )

    unique_candidates: list[tuple[str, str]] = []
    seen_urls = set()

    for href, text in candidates:
        if href in seen_urls:
            continue

        seen_urls.add(href)
        unique_candidates.append(
            (href, text)
        )

    if not unique_candidates:
        raise RuntimeError(
            "На сторінці NAIS не знайдено "
            f"актуального ZIP для ЄРБ: {page_url}"
        )

    def candidate_score(
        item: tuple[str, str],
    ) -> tuple[int, int]:
        href, text = item

        text_lower = text.lower()
        href_lower = href.lower()

        score_value = 0

        if (
            "ex_csv_erb.zip" in text_lower
            and "struct" not in text_lower
        ):
            score_value += 100

        if (
            "struct" in text_lower
            or "struct" in href_lower
        ):
            score_value -= 1000

        return (
            score_value,
            -len(href),
        )

    best_url, best_text = sorted(
        unique_candidates,
        key=candidate_score,
        reverse=True,
    )[0]

    return {
        "dataset_title": (
            "Єдиний реєстр боржників "
            "(NAIS page fallback)"
        ),
        "resource_name": (
            best_text
            or best_url.rstrip("/").split("/")[-1]
        ),
        "resource_path": best_url,
        "used_fallback": True,
        "fallback_source": "nais_page",
    }


def download_erb_zip(
    target_path: Path,
) -> dict:
    try:
        print("Resolving datapackage for ERB...")

        resource = resolve_resource_from_datapackage(
            ERB_DATAPACKAGE_URL
        )

        print(
            f"ERB resource: "
            f"{resource['resource_name']}"
        )
        print(
            f"ERB ZIP URL: "
            f"{resource['resource_path']}"
        )
        print(
            "Downloading ERB ZIP from data.gov.ua..."
        )

        fetch_to_file(
            resource["resource_path"],
            target_path,
            retries=2,
        )

        return resource

    except Exception as error:
        print(
            "Primary data.gov.ua source failed: "
            f"{error}"
        )

    # Старий прямий ZIP data.gov.ua не використовується.
    # Після збою одразу переходимо на NAIS.
    print("Trying NAIS page fallback...")

    nais_resource = resolve_resource_from_nais_page(
        ERB_NAIS_PAGE_URL
    )

    print(
        f"NAIS resource: "
        f"{nais_resource['resource_name']}"
    )
    print(
        f"NAIS ZIP URL: "
        f"{nais_resource['resource_path']}"
    )

    fetch_to_file(
        nais_resource["resource_path"],
        target_path,
        retries=2,
    )

    return nais_resource


def decode_bytes(
    raw_bytes: bytes,
) -> tuple[str, str]:
    last_error = None

    for encoding in ENCODINGS_TO_TRY:
        try:
            return (
                raw_bytes.decode(encoding),
                encoding,
            )
        except Exception as error:
            last_error = error

    raise RuntimeError(
        "Не вдалося декодувати байти. "
        f"Остання помилка: {last_error}"
    )


def normalize_header_name(
    value: str,
) -> str:
    return (
        str(value or "")
        .replace("\ufeff", "")
        .strip()
        .strip('"')
        .upper()
    )


def parse_csv_line(
    line: str,
    delimiter: str,
) -> list[str]:
    return next(
        csv.reader(
            [line],
            delimiter=delimiter,
            quotechar='"',
        )
    )


def detect_header_delimiter(
    header_line: str,
) -> tuple[str, list[str]]:
    best_delimiter = None
    best_header = None
    best_score = None

    for priority, delimiter in enumerate(
        DELIMITERS_TO_TRY
    ):
        try:
            parsed = parse_csv_line(
                header_line,
                delimiter,
            )
        except csv.Error:
            continue

        header = [
            normalize_header_name(value)
            for value in parsed
        ]

        non_empty_count = sum(
            1
            for value in header
            if value
        )

        recognized_fields_count = sum(
            1
            for value in header
            if value in EXPECTED_ERB_HEADER_FIELDS
        )

        score = (
            recognized_fields_count,
            len(header),
            non_empty_count,
            -priority,
        )

        if best_score is None or score > best_score:
            best_score = score
            best_delimiter = delimiter
            best_header = header

    if (
        best_delimiter is None
        or best_header is None
        or len(best_header) <= 1
    ):
        raise RuntimeError(
            "Не вдалося автоматично визначити "
            "роздільник заголовка CSV"
        )

    recognized_fields = [
        field
        for field in best_header
        if field in EXPECTED_ERB_HEADER_FIELDS
    ]

    if not recognized_fields:
        raise RuntimeError(
            "Роздільник заголовка визначено, "
            "але в заголовку не знайдено "
            "очікуваних полів ЄРБ. "
            f"Header={best_header}"
        )

    return (
        best_delimiter,
        best_header,
    )


def detect_row_delimiter(
    data_lines: list[str],
    header_len: int,
) -> tuple[str, list[int], int]:
    best_delimiter = None
    best_lengths = None
    best_exact_count = 0
    best_score = None

    for priority, delimiter in enumerate(
        DELIMITERS_TO_TRY
    ):
        row_lengths = []

        for line in data_lines:
            try:
                parsed = parse_csv_line(
                    line,
                    delimiter,
                )
                row_lengths.append(len(parsed))
            except csv.Error:
                row_lengths.append(0)

        exact_count = sum(
            1
            for length in row_lengths
            if length == header_len
        )

        total_deviation = sum(
            abs(length - header_len)
            for length in row_lengths
        )

        score = (
            exact_count,
            -total_deviation,
            -priority,
        )

        if best_score is None or score > best_score:
            best_score = score
            best_delimiter = delimiter
            best_lengths = row_lengths
            best_exact_count = exact_count

    if (
        best_delimiter is None
        or best_lengths is None
        or best_exact_count == 0
    ):
        raise RuntimeError(
            "Не вдалося автоматично визначити "
            "роздільник рядків CSV. "
            f"Очікувана кількість колонок: {header_len}. "
            f"Найкращі довжини рядків: {best_lengths}"
        )

    return (
        best_delimiter,
        best_lengths,
        best_exact_count,
    )


def parse_erb_layout_from_zip(
    zip_path: Path,
) -> dict:
    with zipfile.ZipFile(zip_path, "r") as archive:
        csv_names = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_names:
            raise RuntimeError(
                f"У ZIP не знайдено CSV: {zip_path}"
            )

        csv_name = max(
            csv_names,
            key=lambda name: archive.getinfo(
                name
            ).file_size,
        )

        with archive.open(csv_name) as source_file:
            raw_bytes = source_file.read(300_000)

    text, encoding_used = decode_bytes(
        raw_bytes
    )
    lines = text.splitlines()

    if len(lines) < 2:
        raise RuntimeError(
            "У CSV недостатньо рядків для аналізу"
        )

    header_line = lines[0]

    first_data_lines = [
        line
        for line in lines[1:21]
        if line.strip()
    ]

    if not first_data_lines:
        raise RuntimeError(
            "У CSV не знайдено рядків даних "
            "для визначення формату"
        )

    (
        header_delimiter,
        header,
    ) = detect_header_delimiter(
        header_line
    )

    header_len = len(header)

    (
        row_delimiter,
        row_lengths,
        same_len_count,
    ) = detect_row_delimiter(
        first_data_lines,
        header_len,
    )

    print(
        "ERB layout confirmed: "
        f"header_delim={repr(header_delimiter)}; "
        f"row_delim={repr(row_delimiter)}; "
        f"header_len={header_len}; "
        f"same_len_count={same_len_count}; "
        f"sample_row_lengths={row_lengths}"
    )

    return {
        "csv_name": csv_name,
        "encoding": encoding_used,
        "header": header,
        "header_delimiter": header_delimiter,
        "row_delimiter": row_delimiter,
    }


def iter_erb_rows_from_zip(
    zip_path: Path,
    encoding: str,
    csv_name: str,
    header: list[str],
    row_delimiter: str,
):
    with zipfile.ZipFile(zip_path, "r") as archive:
        with archive.open(csv_name) as source_file:
            text_stream = io.TextIOWrapper(
                source_file,
                encoding=encoding,
                newline="",
            )

            header_line = text_stream.readline()

            if not header_line:
                return

            header_len = len(header)

            reader = csv.reader(
                text_stream,
                delimiter=row_delimiter,
                quotechar='"',
            )

            for row in reader:
                if row is None:
                    continue

                if not any(
                    str(value).strip()
                    for value in row
                ):
                    continue

                if len(row) < header_len:
                    row = row + (
                        [""] * (header_len - len(row))
                    )

                elif len(row) > header_len:
                    row = (
                        row[:header_len - 1]
                        + [
                            row_delimiter.join(
                                row[header_len - 1:]
                            )
                        ]
                    )

                yield {
                    header[index]: (
                        ""
                        if row[index] is None
                        else str(row[index])
                    )
                    for index in range(header_len)
                }


def load_watchlist(
    path: Path,
) -> list[dict]:
    with open(
        path,
        "r",
        encoding="utf-8",
    ) as source_file:
        rows = json.load(source_file)

    prepared = []

    for row in rows:
        entity_type = str(
            row.get("entity_type", "")
        ).strip().lower()

        prepared.append(
            {
                "id": str(
                    row.get("id", "")
                ).strip(),
                "is_active": truthy(
                    row.get("is_active", "")
                ),
                "entity_type": entity_type,
                "label": str(
                    row.get("label", "")
                ).strip(),
                "debtor_name": str(
                    row.get("debtor_name", "")
                ).strip(),
                "debtor_name_norm": normalize_text(
                    row.get("debtor_name", "")
                ),
                "debtor_code": normalize_code(
                    row.get("debtor_code", "")
                ),
                "birthdate": normalize_birthdate(
                    row.get("birthdate", "")
                ),
                "notes": str(
                    row.get("notes", "")
                ).strip(),
            }
        )

    active = [
        row
        for row in prepared
        if row["is_active"]
    ]

    print(
        f"Loaded watchlist rows: "
        f"{len(active)} active"
    )

    return active


def build_watchlist_index(
    watchlist: list,
) -> dict:
    index = {
        "company_by_code": {},
        "company_by_name": {},
        "person_by_name_birthdate": {},
        "person_by_name": {},
    }

    for watchlist_item in watchlist:
        entity_type = watchlist_item.get(
            "entity_type",
            "",
        )
        name = watchlist_item.get(
            "debtor_name_norm",
            "",
        )
        code = watchlist_item.get(
            "debtor_code",
            "",
        )
        birthdate = watchlist_item.get(
            "birthdate",
            "",
        )

        if entity_type == "company":
            if code:
                index["company_by_code"].setdefault(
                    code,
                    [],
                ).append(watchlist_item)

            if name:
                index["company_by_name"].setdefault(
                    name,
                    [],
                ).append(watchlist_item)

        elif entity_type == "person":
            if name and birthdate:
                key = f"{name}|{birthdate}"

                index[
                    "person_by_name_birthdate"
                ].setdefault(
                    key,
                    [],
                ).append(watchlist_item)

            if name:
                index["person_by_name"].setdefault(
                    name,
                    [],
                ).append(watchlist_item)

    print(
        "Watchlist index built: "
        f"company_by_code="
        f"{len(index['company_by_code'])}, "
        f"company_by_name="
        f"{len(index['company_by_name'])}, "
        f"person_by_name_birthdate="
        f"{len(index['person_by_name_birthdate'])}, "
        f"person_by_name="
        f"{len(index['person_by_name'])}"
    )

    return index


def match_watchlist_indexed(
    row: dict,
    watchlist_index: dict,
):
    row_name = normalize_text(
        pick_field(
            row,
            "DEBTOR_NAME",
        )
    )

    row_code = normalize_code(
        pick_field(
            row,
            "DEBTOR_CODE",
        )
    )

    row_birthdate = normalize_birthdate(
        pick_field(
            row,
            "DEBTOR_BIRTHDATE",
            "BIRTHDATE",
        )
    )

    matches = []
    matched_ids = set()

    def add_match(
        watchlist_item: dict,
        strength: str,
    ):
        watchlist_id = str(
            watchlist_item.get("id", "")
        )

        if watchlist_id in matched_ids:
            return

        matched_ids.add(watchlist_id)

        matches.append(
            (
                watchlist_item,
                strength,
            )
        )

    # 1. Юридичні особи: код.
    if row_code:
        for watchlist_item in watchlist_index[
            "company_by_code"
        ].get(row_code, []):
            add_match(
                watchlist_item,
                "strong",
            )

    # 2. Юридичні особи: точна нормалізована назва.
    if row_name:
        for watchlist_item in watchlist_index[
            "company_by_name"
        ].get(row_name, []):
            add_match(
                watchlist_item,
                "weak",
            )

    # 3. Фізичні особи: ПІБ + дата народження.
    if row_name and row_birthdate:
        person_key = (
            f"{row_name}|{row_birthdate}"
        )

        for watchlist_item in watchlist_index[
            "person_by_name_birthdate"
        ].get(person_key, []):
            add_match(
                watchlist_item,
                "strong",
            )

    # 4. Фізичні особи: ПІБ, якщо дата відсутня
    # хоча б з одного боку.
    if row_name:
        for watchlist_item in watchlist_index[
            "person_by_name"
        ].get(row_name, []):
            watchlist_birthdate = (
                watchlist_item.get(
                    "birthdate",
                    "",
                )
            )

            if (
                watchlist_birthdate
                and row_birthdate
                and watchlist_birthdate
                != row_birthdate
            ):
                continue

            if (
                not watchlist_birthdate
                or not row_birthdate
            ):
                add_match(
                    watchlist_item,
                    "weak",
                )

    return matches


def stable_hash(
    data: dict,
) -> str:
    payload = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def build_erb_record(
    watchlist_item: dict,
    match_strength: str,
    row: dict,
    source_date: str,
) -> dict:
    record = {
        "watchlist_id": watchlist_item["id"],
        "match_strength": match_strength,
        "debtor_name": pick_field(
            row,
            "DEBTOR_NAME",
        ),
        "debtor_birthdate": normalize_birthdate(
            pick_field(
                row,
                "DEBTOR_BIRTHDATE",
                "BIRTHDATE",
            )
        ),
        "debtor_code": normalize_code(
            pick_field(
                row,
                "DEBTOR_CODE",
            )
        ),
        "publisher": pick_field(
            row,
            "PUBLISHER",
        ),
        "org_name": pick_field(
            row,
            "ORG_NAME",
            "EMP_ORG",
        ),
        "org_phone_num": pick_field(
            row,
            "ORG_PHONE_NUM",
            "ORG_PHONE",
        ),
        "emp_full_fio": pick_field(
            row,
            "EMP_FULL_FIO",
        ),
        "emp_phone_num": pick_field(
            row,
            "EMP_PHONE_NUM",
        ),
        "email_addr": pick_field(
            row,
            "EMAIL_ADDR",
        ),
        "vp_ordernum": pick_field(
            row,
            "VP_ORDERNUM",
            "VP_ORDER_NUM",
        ),
        "vd_cat": pick_field(
            row,
            "VD_CAT",
        ),
        "source_date": source_date,
        "row_hash": "",
        "first_seen": source_date,
        "last_seen": source_date,
        "is_active": "true",
    }

    record["row_hash"] = stable_hash(record)

    return record


def dedupe_records(
    records: list,
) -> list:
    seen = set()
    result = []

    for record in records:
        row_hash = record["row_hash"]

        if row_hash in seen:
            continue

        seen.add(row_hash)
        result.append(record)

    return result


def process_erb(
    zip_path: Path,
    watchlist: list,
    source_date: str,
    resource_meta: dict,
):
    meta = parse_erb_layout_from_zip(
        zip_path
    )

    matches = []
    scanned = 0

    watchlist_index = build_watchlist_index(
        watchlist
    )

    print(
        f"Processing ERB: "
        f"csv_name={meta['csv_name']}, "
        f"encoding={meta['encoding']}, "
        f"header_delim="
        f"{repr(meta['header_delimiter'])}, "
        f"row_delim="
        f"{repr(meta['row_delimiter'])}"
    )

    print(
        "HEADER:",
        meta["header"],
    )

    rows_iterator = iter_erb_rows_from_zip(
        zip_path=zip_path,
        encoding=meta["encoding"],
        csv_name=meta["csv_name"],
        header=meta["header"],
        row_delimiter=meta["row_delimiter"],
    )

    for index, row in enumerate(
        rows_iterator
    ):
        if index < 3:
            print(
                "ROW SAMPLE",
                index + 1,
                row,
            )

        scanned += 1

        found = match_watchlist_indexed(
            row,
            watchlist_index,
        )

        if not found:
            continue

        for (
            watchlist_item,
            match_strength,
        ) in found:
            matches.append(
                build_erb_record(
                    watchlist_item,
                    match_strength,
                    row,
                    source_date,
                )
            )

    matches = dedupe_records(
        matches
    )

    tech_row = {
        "run_at": source_date,
        "source_name": "erb",
        "status": "ok",
        "rows_scanned": str(scanned),
        "matches_found": str(len(matches)),
        "notes": (
            f"dataset_title="
            f"{resource_meta.get('dataset_title', '')}; "
            f"resource_name="
            f"{resource_meta.get('resource_name', '')}; "
            f"resource_path="
            f"{resource_meta.get('resource_path', '')}; "
            f"encoding={meta['encoding']}; "
            f"csv_name={meta['csv_name']}; "
            f"header_delim="
            f"{repr(meta['header_delimiter'])}; "
            f"row_delim="
            f"{repr(meta['row_delimiter'])}; "
            f"used_fallback="
            f"{resource_meta.get('used_fallback', False)}; "
            f"fallback_source="
            f"{resource_meta.get('fallback_source', '')}"
        ),
    }

    return (
        matches,
        tech_row,
    )


def main():
    if not WATCHLIST_PATH.exists():
        raise FileNotFoundError(
            "Не знайдено watchlist.json. "
            "Спочатку запусти workflow "
            "отримання watchlist."
        )

    watchlist = load_watchlist(
        WATCHLIST_PATH
    )

    if not watchlist:
        raise RuntimeError(
            "Watchlist порожній або "
            "немає активних записів."
        )

    source_date = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    erb_resource = download_erb_zip(
        ERB_ZIP_PATH
    )

    print(
        f"Final ERB resource: "
        f"{erb_resource['resource_name']}"
    )
    print(
        f"Final ERB ZIP URL: "
        f"{erb_resource['resource_path']}"
    )
    print(
        f"Used fallback: "
        f"{erb_resource['used_fallback']}"
    )
    print(
        f"Fallback source: "
        f"{erb_resource.get('fallback_source', '')}"
    )

    print("Filtering ERB...")

    erb_rows, erb_tech = process_erb(
        ERB_ZIP_PATH,
        watchlist,
        source_date,
        erb_resource,
    )

    with open(
        "filtered_erb.json",
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            erb_rows,
            output_file,
            ensure_ascii=False,
            indent=2,
        )

    with open(
        "tech_rows.json",
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            [erb_tech],
            output_file,
            ensure_ascii=False,
            indent=2,
        )

    print(
        f"ERB matches: {len(erb_rows)}"
    )
    print(
        "Saved filtered_erb.json, "
        "tech_rows.json"
    )


if __name__ == "__main__":
    main()
