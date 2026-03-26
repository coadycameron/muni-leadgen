import os
import time
from typing import List, Set, Tuple

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def _get_sheets_service():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _a1(tab: str, rng: str) -> str:
    return f"{tab}!{rng}"


def _get_or_create_sheet_id(service, spreadsheet_id: str, tab: str) -> int:
    meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title))",
    ).execute()

    for s in meta.get("sheets", []):
        props = s.get("properties", {}) or {}
        if props.get("title") == tab:
            return int(props["sheetId"])

    # create tab
    body = {"requests": [{"addSheet": {"properties": {"title": tab}}}]}
    resp = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    replies = resp.get("replies", []) or []
    sheet_id = replies[0].get("addSheet", {}).get("properties", {}).get("sheetId")
    if sheet_id is None:
        raise RuntimeError(f"Failed to create tab: {tab}")
    return int(sheet_id)


def _insert_rows_at_top(service, spreadsheet_id: str, sheet_id: int, n_rows: int) -> None:
    if n_rows <= 0:
        return
    body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1,  # row 2 (0 based)
                        "endIndex": 1 + n_rows,
                    },
                    "inheritFromBefore": False,
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def _read_existing_emails(service, spreadsheet_id: str, tab: str, max_rows: int) -> Set[str]:
    if max_rows <= 0:
        return set()

    rng = _a1(tab, f"A2:A{max_rows}")
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=rng,
        majorDimension="COLUMNS",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return set()

    col = values[0]
    return {str(v).strip().lower() for v in col if v and "@" in str(v)}


def _ensure_header(service, spreadsheet_id: str, tab: str, headers: List[str]) -> None:
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=_a1(tab, "A1:ZZ1"),
    ).execute()

    row = (resp.get("values", []) or [[]])[0]
    existing = [str(x).strip() for x in row]

    # If row 1 is empty, write headers
    if not any(existing):
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=_a1(tab, "A1"),
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()
        return

    # If row 1 exists but mismatched, overwrite it (safer than silently misaligning)
    expected = [str(x).strip() for x in headers]
    existing_trimmed = [x for x in existing if x]

    if existing_trimmed != expected:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=_a1(tab, "A1"),
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()


def append_leads_to_sheet(headers: List[str], rows: List[List[str]], run_id: str) -> Tuple[int, int]:
    spreadsheet_id = os.environ.get("SHEETS_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing SHEETS_SPREADSHEET_ID env var")

    tab = os.environ.get("SHEETS_TAB_NAME", "qaqc_leads").strip()
    max_rows = int(os.environ.get("SHEETS_DEDUPE_MAX_ROWS", "20000"))
    write_newlines = os.environ.get("SHEETS_WRITE_NEWLINES", "1").strip().lower() in {"1", "true", "yes"}
    insert_at_top = os.environ.get("SHEETS_INSERT_AT_TOP", "1").strip().lower() in {"1", "true", "yes"}

    service = _get_sheets_service()

    # Ensure tab exists and get id for insert logic
    sheet_id = _get_or_create_sheet_id(service, spreadsheet_id, tab)

    sheet_headers = headers + ["run_id", "pushed_at_utc"]
    _ensure_header(service, spreadsheet_id, tab, sheet_headers)

    existing = _read_existing_emails(service, spreadsheet_id, tab, max_rows)

    pushed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    to_write: List[List[str]] = []
    skipped = 0

    for r in rows:
        if not r:
            continue

        email = str(r[0]).strip().lower()
        if not email or "@" not in email:
            skipped += 1
            continue

        if email in existing:
            skipped += 1
            continue

        out_row = list(r) + [run_id, pushed_at]
        if write_newlines:
            out_row = [str(x).replace("<LB>", "\n") for x in out_row]

        to_write.append(out_row)
        existing.add(email)

    if not to_write:
        return 0, skipped

    if insert_at_top:
        _insert_rows_at_top(service, spreadsheet_id, sheet_id, len(to_write))
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=_a1(tab, "A2"),
            valueInputOption="RAW",
            body={"values": to_write},
        ).execute()
        return len(to_write), skipped

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=_a1(tab, "A1"),
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": to_write},
    ).execute()
    return len(to_write), skipped