# app.py
# Tranship Ops Dashboard
# Streamlit + SQLite + POP PDF parser

from __future__ import annotations

import io
import os
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Iterable
from datetime import datetime, date, timedelta
from streamlit_autorefresh import st_autorefresh

import pandas as pd
import pdfplumber
import streamlit as st

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

DB_PATH = "tranship_ops.db"
ADMIN_PIN = os.getenv("TRANSHIP_ADMIN_PIN", "0000")

SKIP_REASONS = [
    "No Transfers",
    "Not Prioritised Due Peak",
    "Other",
]

STATUS_ORDER = {
    "Unassigned": 1,
    "Assigned": 2,
    "AC Met": 3,
    "Completed": 4,
    "Skipped": 5,
}

TYPE_LABELS = {
    "I": "INT",
    "D": "DOM",
    "R": "QLK",
}

# ------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                pin TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_date TEXT NOT NULL,
                flight TEXT NOT NULL,
                flight_type TEXT NOT NULL,
                route TEXT NOT NULL,
                sta TEXT NOT NULL,
                gate TEXT,
                source_file TEXT,
                status TEXT NOT NULL DEFAULT 'Unassigned',

                assigned_to TEXT,
                assigned_at TEXT,

                ac_met_by TEXT,
                ac_met_at TEXT,

                completed_by TEXT,
                completed_at TEXT,

                skipped_by TEXT,
                skipped_at TEXT,
                skip_reason TEXT,
                skip_other_reason TEXT,

                notes TEXT,
                created_at TEXT NOT NULL,

                UNIQUE(task_date, flight, flight_type, sta)
            )
            """
        )

        conn.commit()


# ------------------------------------------------------------
# DATA MODELS
# ------------------------------------------------------------

@dataclass
class ParsedArrival:
    task_date: str
    flight: str
    flight_type: str
    route: str
    sta: str
    source_file: str


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------




def should_show_on_admin_active(task: sqlite3.Row) -> bool:
    """
    Admin active board rules:
    - Unassigned, Assigned, AC Met, Skipped always show
    - Completed shows for 5 minutes after completion, then disappears
    """
    status = task["status"]

    if status in ("Unassigned", "Assigned", "AC Met", "Skipped"):
        return True

    if status == "Completed":
        completed_at = task["completed_at"]
        if not completed_at:
            return False
        try:
            completed_dt = datetime.strptime(completed_at, "%Y-%m-%d %H:%M:%S")
            return datetime.now() <= completed_dt + timedelta(minutes=5)
        except ValueError:
            return False

    return False


def admin_row_colour(status: str) -> str:
    if status == "Unassigned":
        return "#dbeafe"   # blue
    if status in ("Assigned", "AC Met"):
        return "#fef3c7"   # amber
    if status == "Completed":
        return "#dcfce7"   # green
    if status == "Skipped":
        return "#fee2e2"   # red
    return "#ffffff"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return date.today().isoformat()


def normalize_flight(prefix: str, raw_number: str) -> str:
    number = raw_number.strip()
    prefix = prefix.strip().upper()

    if prefix == "QFA":
        prefix = "QF"

    try:
        number_int = int(number)
        return f"{prefix}{number_int}"
    except ValueError:
        return f"{prefix}{number}".replace(" ", "")


def sort_sta_value(sta: str) -> tuple[int, int]:
    try:
        hh, mm = sta.split(":")
        return int(hh), int(mm)
    except Exception:
        return 99, 99


def status_rank(status: str) -> int:
    return STATUS_ORDER.get(status, 99)


def ensure_session_defaults() -> None:
    if "role" not in st.session_state:
        st.session_state.role = None
    if "username" not in st.session_state:
        st.session_state.username = None
    if "pin" not in st.session_state:
        st.session_state.pin = ""
    if "confirm_clear_data" not in st.session_state:
        st.session_state.confirm_clear_data = False
    if "confirm_delete_active" not in st.session_state:
        st.session_state.confirm_delete_active = False


# ------------------------------------------------------------
# PDF PARSING
# ------------------------------------------------------------

def extract_pdf_text(file_bytes: bytes) -> str:
    text_chunks: list[str] = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text_chunks.append(page_text)
    return "\n".join(text_chunks)


def detect_pop_date(text: str) -> str:
    m = re.search(r"Date:\s*(\d{2}\.\d{2}\.\d{4})", text)
    if not m:
        return today_str()

    raw = m.group(1)
    try:
        return datetime.strptime(raw, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return today_str()


def clean_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").strip()


def looks_like_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}", value.strip()))


def parse_arrival_row_from_left_side(
    cells: list[str],
    task_date: str,
    source_file: str,
) -> ParsedArrival | None:
    """
    Expected arrivals side layout:
    [Flight, HFT, Route, STA, ATA, REG, Pax, ...]
    Example:
    ['QFA 0036', 'I', 'SIN', '05:35', '', 'VHEBK', '269', ...]
    """
    if len(cells) < 4:
        return None

    flight_cell = clean_cell(cells[0]).upper()
    flight_type_code = clean_cell(cells[1]).upper()
    route = clean_cell(cells[2]).upper()
    sta = clean_cell(cells[3])

    # Skip junk/header rows
    if not flight_cell:
        return None
    if flight_cell in {"PORT OPERATING PLAN", "ARRIVALS", "FLIGHT"}:
        return None
    if "DATE:" in flight_cell or "FLIGHT TYPE:" in flight_cell:
        return None

    # Must look like "QFA 0036" or "QF 36"
    m = re.fullmatch(r"(QFA|QF)\s*(\d{3,4})", flight_cell)
    if not m:
        return None

    if flight_type_code not in TYPE_LABELS:
        return None
    if not re.fullmatch(r"[A-Z]{3}", route):
        return None
    if not looks_like_time(sta):
        return None

    prefix, raw_number = m.groups()

    return ParsedArrival(
        task_date=task_date,
        flight=normalize_flight(prefix, raw_number),
        flight_type=TYPE_LABELS[flight_type_code],
        route=route,
        sta=sta,
        source_file=source_file,
    )


def parse_arrivals_from_tables(file_bytes: bytes, source_file: str) -> list[ParsedArrival]:
    arrivals: list[ParsedArrival] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        full_text = "\n".join((page.extract_text() or "") for page in pdf.pages)
        task_date = detect_pop_date(full_text)

        for page in pdf.pages:
            tables = page.extract_tables() or []

            for table in tables:
                for row in table:
                    if not row:
                        continue

                    # Keep raw column positions so we can split left/right halves
                    row_cells = [clean_cell(c) for c in row]

                    # Most POP tables are 16 columns wide:
                    # left 8 = arrivals, right 8 = departures
                    #
                    # We ONLY want the left side.
                    if len(row_cells) >= 8:
                        left_side = row_cells[:8]
                        parsed = parse_arrival_row_from_left_side(
                            left_side,
                            task_date,
                            source_file,
                        )
                        if parsed:
                            arrivals.append(parsed)
                    else:
                        # fallback for odd rows
                        parsed = parse_arrival_row_from_left_side(
                            row_cells,
                            task_date,
                            source_file,
                        )
                        if parsed:
                            arrivals.append(parsed)

    # de-duplicate
    deduped = {
        (a.task_date, a.flight, a.flight_type, a.route, a.sta): a
        for a in arrivals
    }

    return sorted(
        deduped.values(),
        key=lambda x: (x.task_date, sort_sta_value(x.sta), x.flight),
    )


def parse_arrivals_from_text(text: str, source_file: str) -> list[ParsedArrival]:
    """
    Fallback only.
    """
    task_date = detect_pop_date(text)
    arrivals: list[ParsedArrival] = []

    line_regex = re.compile(
        r"^(QFA|QF)\s+(\d{3,4})\s+([IDR])\s+([A-Z]{3})\s+(\d{2}:\d{2})\s+[A-Z0-9]+\s+\d+$"
    )

    for raw_line in text.splitlines():
        line = raw_line.strip()
        m = line_regex.match(line)
        if not m:
            continue

        prefix, raw_flight_no, flight_type_code, route, sta = m.groups()

        arrivals.append(
            ParsedArrival(
                task_date=task_date,
                flight=normalize_flight(prefix, raw_flight_no),
                flight_type=TYPE_LABELS.get(flight_type_code, flight_type_code),
                route=route,
                sta=sta,
                source_file=source_file,
            )
        )

    deduped = {
        (a.task_date, a.flight, a.flight_type, a.route, a.sta): a
        for a in arrivals
    }

    return sorted(
        deduped.values(),
        key=lambda x: (x.task_date, sort_sta_value(x.sta), x.flight),
    )


def parse_pop_pdf(uploaded_file) -> list[ParsedArrival]:
    file_bytes = uploaded_file.read()

    # Primary parser
    arrivals = parse_arrivals_from_tables(file_bytes, uploaded_file.name)
    if arrivals:
        return arrivals

    # Fallback parser
    text = extract_pdf_text(file_bytes)
    return parse_arrivals_from_text(text, uploaded_file.name)
# ------------------------------------------------------------
# DATABASE ACTIONS
# ------------------------------------------------------------

def create_user(username: str, pin: str) -> tuple[bool, str]:
    username = username.strip()
    pin = pin.strip()

    if not username:
        return False, "Username is required."
    if not re.fullmatch(r"\d{4}", pin):
        return False, "PIN must be exactly 4 digits."

    with closing(get_conn()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO users (username, pin, created_at)
                VALUES (?, ?, ?)
                """,
                (username, pin, now_str()),
            )
            conn.commit()
            return True, f"User '{username}' created."
        except sqlite3.IntegrityError:
            return False, "Username or PIN already exists."


def delete_user(user_id: int) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()


def get_all_users() -> list[sqlite3.Row]:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users ORDER BY username")
        return cur.fetchall()


def get_user_by_pin(pin: str) -> sqlite3.Row | None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE pin = ?", (pin,))
        return cur.fetchone()


def insert_tasks(parsed_arrivals: Iterable[ParsedArrival]) -> tuple[int, int]:
    inserted = 0
    skipped = 0

    with closing(get_conn()) as conn:
        cur = conn.cursor()

        for item in parsed_arrivals:
            try:
                cur.execute(
                    """
                    INSERT INTO tasks (
                        task_date, flight, flight_type, route, sta, gate,
                        source_file, status, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'Unassigned', ?)
                    """,
                    (
                        item.task_date,
                        item.flight,
                        item.flight_type,
                        item.route,
                        item.sta,
                        None,
                        item.source_file,
                        now_str(),
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1

        conn.commit()

    return inserted, skipped


def get_tasks(task_date: str | None = None, include_history: bool = True) -> list[sqlite3.Row]:
    sql = "SELECT * FROM tasks"
    params: list[Any] = []

    if task_date:
        sql += " WHERE task_date = ?"
        params.append(task_date)

    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not include_history:
        rows = [r for r in rows if r["status"] not in ("Completed", "Skipped")]

    return sorted(
        rows,
        key=lambda r: (
            r["task_date"],
            sort_sta_value(r["sta"]),
            status_rank(r["status"]),
            r["flight_type"],
            r["flight"],
        ),
    )


def assign_task_to_user(task_id: int, username: str) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tasks
            SET status = 'Assigned',
                assigned_to = ?,
                assigned_at = ?
            WHERE id = ?
              AND status = 'Unassigned'
            """,
            (username, now_str(), task_id),
        )
        conn.commit()


def mark_ac_met(task_id: int, username: str) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tasks
            SET status = 'AC Met',
                ac_met_by = ?,
                ac_met_at = ?
            WHERE id = ?
              AND status IN ('Assigned', 'AC Met')
              AND assigned_to = ?
            """,
            (username, now_str(), task_id, username),
        )
        conn.commit()


def complete_task(task_id: int, username: str, notes: str) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tasks
            SET status = 'Completed',
                completed_by = ?,
                completed_at = ?,
                notes = CASE
                    WHEN notes IS NULL OR notes = '' THEN ?
                    WHEN ? IS NULL OR ? = '' THEN notes
                    ELSE notes || CHAR(10) || ?
                END
            WHERE id = ?
              AND status IN ('Assigned', 'AC Met')
              AND assigned_to = ?
            """,
            (
                username,
                now_str(),
                notes.strip(),
                notes.strip(),
                notes.strip(),
                notes.strip(),
                task_id,
                username,
            ),
        )
        conn.commit()


def skip_task(
    task_id: int,
    username: str,
    reason: str,
    other_reason: str,
    notes: str,
) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tasks
            SET status = 'Skipped',
                skipped_by = ?,
                skipped_at = ?,
                skip_reason = ?,
                skip_other_reason = ?,
                notes = CASE
                    WHEN notes IS NULL OR notes = '' THEN ?
                    WHEN ? IS NULL OR ? = '' THEN notes
                    ELSE notes || CHAR(10) || ?
                END
            WHERE id = ?
              AND status IN ('Unassigned', 'Assigned', 'AC Met')
            """,
            (
                username,
                now_str(),
                reason,
                other_reason.strip(),
                notes.strip(),
                notes.strip(),
                notes.strip(),
                notes.strip(),
                task_id,
            ),
        )
        conn.commit()


def recall_task(task_id: int) -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tasks
            SET status = 'Unassigned',
                assigned_to = NULL,
                assigned_at = NULL,
                ac_met_by = NULL,
                ac_met_at = NULL,
                completed_by = NULL,
                completed_at = NULL,
                skipped_by = NULL,
                skipped_at = NULL,
                skip_reason = NULL,
                skip_other_reason = NULL
            WHERE id = ?
              AND status IN ('Completed', 'Skipped')
            """,
            (task_id,),
        )
        conn.commit()


def get_report_dataframe() -> pd.DataFrame:
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                task_date,
                flight,
                flight_type,
                route,
                sta,
                gate,
                source_file,
                status,
                assigned_to,
                assigned_at,
                ac_met_by,
                ac_met_at,
                completed_by,
                completed_at,
                skipped_by,
                skipped_at,
                skip_reason,
                skip_other_reason,
                notes,
                created_at
            FROM tasks
            ORDER BY task_date, sta, flight
            """,
            conn,
        )
    return df


def clear_all_task_data() -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM tasks")
        conn.commit()


def delete_all_active_tasks() -> None:
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM tasks WHERE status NOT IN ('Completed', 'Skipped')")
        conn.commit()


# ------------------------------------------------------------
# UI STYLES CSS
# ------------------------------------------------------------
def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stButton > button {
            height: 80px;
            front-size: 1.5rem;
            border-radius: 16px;
            font-weight:700;
        }
        
        .stApp {
            background-color: #ffffff;
        }

        .muted {
            color: #94a3b8;
            font-size: 0.92rem;
        }

        .pill {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 0.82rem;
            font-weight: 700;
            margin-right: 6px;
            margin-bottom: 6px;
        }
        .pill-unassigned { background: #dbeafe; color: #1d4ed8; }
        .pill-assigned   { background: #fef3c7; color: #b45309; }
        .pill-met        { background: #fde68a; color: #92400e; }
        .pill-done       { background: #dcfce7; color: #15803d; }
        .pill-skip       { background: #fee2e2; color: #b91c1c; }

        .summary-box {
            border: 1px solid #243041;
            border-radius: 14px;
            padding: 14px;
            background: #111827;
            color: #e5e7eb;
            min-height: 90px;
        }

        .board-wrap {
            margin-top: 8px;
        }

        .board-card {
            border-radius: 16px;
            padding: 14px 16px;
            margin-bottom: 12px;
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 4px 16px rgba(0,0,0,0.16);
        }

        .board-card.unassigned {
            background: linear-gradient(135deg, #0f172a 0%, #172554 100%);
            border-left: 8px solid #3b82f6;
        }

        .board-card.assigned {
            background: linear-gradient(135deg, #1f2937 0%, #78350f 100%);
            border-left: 8px solid #f59e0b;
        }

        .board-card.met {
            background: linear-gradient(135deg, #292524 0%, #a16207 100%);
            border-left: 8px solid #fbbf24;
        }

        .board-card.completed {
            background: linear-gradient(135deg, #0f172a 0%, #14532d 100%);
            border-left: 8px solid #22c55e;
        }

        .board-card.skipped {
            background: linear-gradient(135deg, #1f1722 0%, #7f1d1d 100%);
            border-left: 8px solid #ef4444;
        }

        .board-top {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 8px;
        }

        .board-flight {
            font-size: 1.2rem;
            font-weight: 800;
            color: white;
            line-height: 1.2;
        }

        .board-status {
            font-size: 0.85rem;
            font-weight: 800;
            padding: 5px 10px;
            border-radius: 999px;
            white-space: nowrap;
        }

        .status-unassigned { background: #dbeafe; color: #1d4ed8; }
        .status-assigned   { background: #fef3c7; color: #b45309; }
        .status-met        { background: #fde68a; color: #92400e; }
        .status-completed  { background: #dcfce7; color: #15803d; }
        .status-skipped    { background: #fee2e2; color: #b91c1c; }

        .board-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-bottom: 10px;
        }

        .meta-chip {
            background: rgba(255,255,255,0.08);
            color: #e5e7eb;
            padding: 6px 10px;
            border-radius: 10px;
            font-size: 0.85rem;
            font-weight: 600;
        }

        .board-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 8px;
            margin-top: 8px;
        }

        .board-field {
            background: rgba(255,255,255,0.06);
            border-radius: 10px;
            padding: 8px 10px;
        }

        .board-label {
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: #cbd5e1;
            margin-bottom: 2px;
            font-weight: 700;
        }

        .board-value {
            color: white;
            font-size: 0.95rem;
            font-weight: 600;
        }

        .board-note {
            margin-top: 10px;
            padding: 10px 12px;
            border-radius: 10px;
            background: rgba(255,255,255,0.08);
            color: #f8fafc;
            font-size: 0.9rem;
        }

        div[data-testid="stTabs"] button {
            font-weight: 700;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
def status_pill_html(status: str) -> str:
    mapping = {
        "Unassigned": "pill pill-unassigned",
        "Assigned": "pill pill-assigned",
        "AC Met": "pill pill-met",
        "Completed": "pill pill-done",
        "Skipped": "pill pill-skip",
    }
    css = mapping.get(status, "pill")
    return f"<span class='{css}'>{status}</span>"


# ------------------------------------------------------------
# LOGIN
# ------------------------------------------------------------
def render_login():
    st.title("Enter PIN")

    if "pin_input" not in st.session_state:
        st.session_state.pin_input = ""

    # Display entered PIN
    st.markdown(
        f"""
        <div style="
            font-size: 2rem;
            text-align: center;
            margin-bottom: 20px;
            letter-spacing: 10px;
        ">
            {"●" * len(st.session_state.pin_input)}
        </div>
        """,
        unsafe_allow_html=True,
    )

    def add_digit(d):
        if len(st.session_state.pin_input) < 4:
            st.session_state.pin_input += str(d)

    def clear_pin():
        st.session_state.pin_input = ""

    def submit_pin():
        pin = st.session_state.pin_input

        if pin == ADMIN_PIN:
            st.session_state.role = "admin"
            st.session_state.username = "Admin"
            st.session_state.pin = pin
            st.session_state.pin_input = ""
            st.rerun()

        user = get_user_by_pin(pin)
        if user:
            st.session_state.role = "user"
            st.session_state.username = user["username"]
            st.session_state.pin = pin
            st.session_state.pin_input = ""
            st.rerun()

        st.error("Invalid PIN")
        st.session_state.pin_input = ""

    keypad = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
        ["C", 0, "OK"],
    ]

    for row in keypad:
        cols = st.columns(3)
        for i, val in enumerate(row):
            with cols[i]:
                if val == "C":
                    if st.button("Clear", key=f"key_{val}_{i}_{len(st.session_state.pin_input)}", use_container_width=True):
                        clear_pin()
                        st.rerun()
                elif val == "OK":
                    if st.button("Enter", key=f"key_{val}_{i}_{len(st.session_state.pin_input)}", use_container_width=True):
                        submit_pin()
                else:
                    if st.button(str(val), key=f"key_{val}_{i}_{len(st.session_state.pin_input)}", use_container_width=True):
                        add_digit(val)
                        if len(st.session_state.pin_input) == 4:
                            submit_pin()
                        else:
                            st.rerun()

def logout() -> None:
    st.session_state.role = None
    st.session_state.username = None
    st.session_state.pin = ""
    st.rerun()


# ------------------------------------------------------------
# ADMIN UI
# ------------------------------------------------------------

def render_admin_active_flights() -> None:
    st.subheader("Live Active Flights - Tranship")

    # Auto refresh every 15 seconds
    st_autorefresh(interval=5000, key="admin_live_refresh")

    all_tasks = get_tasks()
    active_tasks = [t for t in all_tasks if should_show_on_admin_active(t)]

    render_summary_boxes(active_tasks)

    if not active_tasks:
        st.info("No active flights loaded.")
        return

    # Sort by date, STA, then flight
    active_tasks = sorted(
        active_tasks,
        key=lambda r: (
            r["task_date"],
            sort_sta_value(r["sta"]),
            r["flight_type"],
            r["flight"],
        ),
    )

    for task in active_tasks:
        bg = admin_row_colour(task["status"])

        assigned_text = task["assigned_to"] if task["assigned_to"] else "-"
        ac_met_text = task["ac_met_by"] if task["ac_met_by"] else "-"
        completed_text = task["completed_by"] if task["completed_by"] else "-"
        skipped_text = task["skipped_by"] if task["skipped_by"] else "-"

        extra_line = ""
        if task["status"] == "Skipped":
            reason = task["skip_reason"] or ""
            if reason == "Other" and task["skip_other_reason"]:
                reason = f"{reason} - {task['skip_other_reason']}"
            extra_line = f"<div style='margin-top:6px; font-size:0.9rem; color:#444;'>Skip reason: {reason}</div>"

        if task["status"] == "Completed" and task["completed_at"]:
            extra_line = f"<div style='margin-top:6px; font-size:0.9rem; color:#444;'>Completed at: {task['completed_at']}</div>"

        st.markdown(
            f"""
            <div style="
                background:{bg};
                border:1px solid #d6dbe1;
                border-radius:12px;
                padding:14px;
                margin-bottom:12px;
            ">
                <div style="font-size:1.1rem; font-weight:700; margin-bottom:6px;">
                    {task['flight']} {task['route']} - MEL
                </div>
                <div style="font-size:0.95rem; margin-bottom:6px;">
                    <strong>Status:</strong> {task['status']} |
                    <strong>Type:</strong> {task['flight_type']} |
                    <strong>STA:</strong> {task['sta']} |
                    <strong>Date:</strong> {task['task_date']}
                </div>
                <div style="font-size:0.92rem; color:#333;">
                    <strong>Assigned:</strong> {assigned_text} |
                    <strong>AC Met:</strong> {ac_met_text} |
                    <strong>Completed:</strong> {completed_text} |
                    <strong>Skipped:</strong> {skipped_text}
                </div>
                {extra_line}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("### Delete All Active Flights")
    st.warning("This removes all live active tasks. Completed/skipped history remains available in reports.")

    if not st.session_state.confirm_delete_active:
        if st.button("Delete All Active Flights", use_container_width=True):
            st.session_state.confirm_delete_active = True
            st.rerun()
    else:
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Confirm Delete Active Flights", use_container_width=True):
                delete_all_active_tasks()
                st.session_state.confirm_delete_active = False
                st.success("Active flights deleted.")
                st.rerun()
        with c2:
            if st.button("Cancel", key="cancel_delete_active", use_container_width=True):
                st.session_state.confirm_delete_active = False
                st.rerun()


def render_admin() -> None:
    st.title("Admin")
    st.caption("Upload POP sheets, manage users, monitor active flights, and download reports.")

    col1, col2 = st.columns([4, 1])
    with col1:
        st.write(f"Logged in as **{st.session_state.username}**")
    with col2:
        if st.button("Logout", use_container_width=True):
            logout()

    tabs = st.tabs(["POP Upload", "Users", "Active Flights", "Reports"])

    with tabs[0]:
        st.subheader("Upload OCC POP PDFs")
        uploaded_files = st.file_uploader(
            "Upload one or more POP PDF files",
            type=["pdf"],
            accept_multiple_files=True,
        )

        if uploaded_files and st.button("Generate Tasks from POP Sheets", use_container_width=True):
            total_inserted = 0
            total_skipped = 0
            all_preview_rows: list[dict[str, str]] = []

            for uploaded_file in uploaded_files:
                try:
                    arrivals = parse_pop_pdf(uploaded_file)
                    inserted, skipped = insert_tasks(arrivals)
                    total_inserted += inserted
                    total_skipped += skipped

                    for a in arrivals:
                        all_preview_rows.append(
                            {
                                "Date": a.task_date,
                                "Flight": a.flight,
                                "Type": a.flight_type,
                                "Route": f"{a.route} - MEL",
                                "STA": a.sta,
                                "Source": a.source_file,
                            }
                        )
                except Exception as exc:
                    st.error(f"Failed to process {uploaded_file.name}: {exc}")

            st.success(
                f"Task generation complete. Inserted: {total_inserted} | Duplicates skipped: {total_skipped}"
            )

            if all_preview_rows:
                preview_df = pd.DataFrame(all_preview_rows)
                preview_df = preview_df.sort_values(by=["Date", "STA", "Flight"])
                st.dataframe(preview_df, use_container_width=True)

    with tabs[1]:
        st.subheader("Create User")

        with st.form("create_user_form"):
            username = st.text_input("Username")
            pin = st.text_input("4-digit PIN", max_chars=4)
            submitted = st.form_submit_button("Create User", use_container_width=True)

        if submitted:
            ok, msg = create_user(username, pin)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

        st.subheader("Existing Users")
        users = get_all_users()

        if not users:
            st.info("No users created yet.")
        else:
            for user in users:
                c1, c2, c3 = st.columns([4, 2, 1])
                with c1:
                    st.write(f"**{user['username']}**")
                with c2:
                    st.code(user["pin"])
                with c3:
                    if st.button("Delete", key=f"delete_user_{user['id']}", use_container_width=True):
                        delete_user(user["id"])
                        st.success(f"Deleted {user['username']}.")
                        st.rerun()

    with tabs[2]:
        render_admin_active_flights()

    with tabs[3]:
        st.subheader("Download Report")
        report_df = get_report_dataframe()

        if report_df.empty:
            st.info("No task data available yet.")
        else:
            st.dataframe(report_df, use_container_width=True, height=400)

            csv_bytes = report_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download CSV Report",
                data=csv_bytes,
                file_name=f"tranship_report_{today_str()}.csv",
                mime="text/csv",
                use_container_width=True,
            )

            st.markdown("### Clear Flight Data")
            st.warning("Use this after downloading the report if you want to reset all task data.")

            if not st.session_state.confirm_clear_data:
                if st.button("Clear Flight Data", use_container_width=True):
                    st.session_state.confirm_clear_data = True
                    st.rerun()
            else:
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Confirm Clear All Flight Data", use_container_width=True):
                        clear_all_task_data()
                        st.session_state.confirm_clear_data = False
                        st.success("All flight data cleared.")
                        st.rerun()
                with c2:
                    if st.button("Cancel", key="cancel_clear_data", use_container_width=True):
                        st.session_state.confirm_clear_data = False
                        st.rerun()


# ------------------------------------------------------------
# USER UI
# ------------------------------------------------------------

def render_summary_boxes(tasks: list[sqlite3.Row]) -> None:
    total = len(tasks)
    assigned = sum(1 for t in tasks if t["status"] == "Assigned")
    met = sum(1 for t in tasks if t["status"] == "AC Met")
    completed = sum(1 for t in tasks if t["status"] == "Completed")
    skipped = sum(1 for t in tasks if t["status"] == "Skipped")
    unassigned = sum(1 for t in tasks if t["status"] == "Unassigned")

    cols = st.columns(6)
    stats = [
        ("Total", total),
        ("Unassigned", unassigned),
        ("Assigned", assigned),
        ("AC Met", met),
        ("Completed", completed),
        ("Skipped", skipped),
    ]
    for col, (label, value) in zip(cols, stats):
        with col:
            st.markdown(
                f"<div class='summary-box'><strong>{label}</strong><br>{value}</div>",
                unsafe_allow_html=True,
            )


def render_task_card(task: sqlite3.Row, current_user: str, history_mode: bool = False) -> None:
    st.markdown("<div class='task-card'>", unsafe_allow_html=True)

    st.markdown(
        f"<div class='task-head'>{task['flight']} {task['route']} - MEL</div>",
        unsafe_allow_html=True,
    )

    meta_bits = [
        f"Type: {task['flight_type']}",
        f"STA: {task['sta']}",
        f"Date: {task['task_date']}",
    ]
    if task["gate"]:
        meta_bits.append(f"Gate: {task['gate']}")

    st.markdown(
        f"{status_pill_html(task['status'])}"
        f"<span class='muted'>{' | '.join(meta_bits)}</span>",
        unsafe_allow_html=True,
    )

    if task["assigned_to"]:
        st.caption(f"Assigned to: {task['assigned_to']}")
    if task["ac_met_by"]:
        st.caption(f"AC Met by: {task['ac_met_by']}")
    if task["completed_by"]:
        st.caption(f"Completed by: {task['completed_by']}")
    if task["skipped_by"]:
        skip_text = task["skip_reason"] or "Skipped"
        if task["skip_reason"] == "Other" and task["skip_other_reason"]:
            skip_text = f"{skip_text} - {task['skip_other_reason']}"
        st.caption(f"Skipped by: {task['skipped_by']} | Reason: {skip_text}")
    if task["notes"]:
        st.caption(f"Notes: {task['notes']}")

    if history_mode:
        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("Recall Task", key=f"recall_{task['id']}", use_container_width=True):
                recall_task(task["id"])
                st.rerun()
        with c2:
            st.empty()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if task["status"] == "Unassigned":
        c1, c2 = st.columns([1, 1])

        with c1:
            if st.button("Assign to Self", key=f"assign_{task['id']}", use_container_width=True):
                assign_task_to_user(task["id"], current_user)
                st.rerun()

        with c2:
            with st.popover("Skip Flight", use_container_width=True):
                skip_reason = st.selectbox(
                    "Reason",
                    SKIP_REASONS,
                    key=f"skip_reason_unassigned_{task['id']}",
                )
                other_reason = ""
                if skip_reason == "Other":
                    other_reason = st.text_input(
                        "Custom reason",
                        key=f"skip_other_unassigned_{task['id']}",
                    )
                notes = st.text_area(
                    "Notes (optional)",
                    key=f"skip_notes_unassigned_{task['id']}",
                )
                if st.button("Confirm Skip", key=f"confirm_skip_unassigned_{task['id']}", use_container_width=True):
                    skip_task(task["id"], current_user, skip_reason, other_reason, notes)
                    st.rerun()

    elif task["status"] in ("Assigned", "AC Met") and task["assigned_to"] == current_user:
        c1, c2, c3 = st.columns([1, 1, 1])

        with c1:
            if task["status"] == "Assigned":
                if st.button("AC Met", key=f"met_{task['id']}", use_container_width=True):
                    mark_ac_met(task["id"], current_user)
                    st.rerun()
            else:
                st.info("Aircraft met")

        with c2:
            with st.popover("Complete", use_container_width=True):
                complete_notes = st.text_area(
                    "Completion notes",
                    key=f"complete_notes_{task['id']}",
                    placeholder="Tail-to-tail drop, issues, comments...",
                )
                if st.button("Confirm Complete", key=f"confirm_complete_{task['id']}", use_container_width=True):
                    complete_task(task["id"], current_user, complete_notes)
                    st.rerun()

        with c3:
            with st.popover("Skip Flight", use_container_width=True):
                skip_reason = st.selectbox(
                    "Reason",
                    SKIP_REASONS,
                    key=f"skip_reason_assigned_{task['id']}",
                )
                other_reason = ""
                if skip_reason == "Other":
                    other_reason = st.text_input(
                        "Custom reason",
                        key=f"skip_other_assigned_{task['id']}",
                    )
                notes = st.text_area(
                    "Notes (optional)",
                    key=f"skip_notes_assigned_{task['id']}",
                )
                if st.button("Confirm Skip", key=f"confirm_skip_assigned_{task['id']}", use_container_width=True):
                    skip_task(task["id"], current_user, skip_reason, other_reason, notes)
                    st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def render_user() -> None:
    st.title("Live Flight Board")

    col1, col2 = st.columns([4, 1])
    with col1:
        st.write(f"Logged in as **{st.session_state.username}**")
    with col2:
        if st.button("Logout", use_container_width=True):
            logout()

    all_tasks = get_tasks()
    if not all_tasks:
        st.info("No tasks loaded yet. Admin needs to upload POP sheets first.")
        return

    tabs = st.tabs(["Active Flights", "History"])

    current_user = st.session_state.username

    with tabs[0]:
        st_autorefresh(interval=5000, key="user_active_refresh")
        active_tasks = get_tasks(include_history=False)

    
        c1, c2, c3 = st.columns(3)
        with c1:
            selected_date = st.selectbox(
                "Date",
                options=sorted({t["task_date"] for t in all_tasks}),
                index=0,
                key="active_date",
            )
        with c2:
            selected_type = st.selectbox("Flight Type", ["ALL", "INT", "DOM", "QLK"], index=0, key="active_type")
        with c3:
            selected_view = st.selectbox(
                "View",
                ["ALL", "UNASSIGNED", "MY TASKS"],
                index=0,
                key="active_view",
            )

        active_tasks = [
            t for t in all_tasks
            if t["task_date"] == selected_date and t["status"] not in ("Completed", "Skipped")
        ]

        if selected_type != "ALL":
            active_tasks = [t for t in active_tasks if t["flight_type"] == selected_type]

        if selected_view == "UNASSIGNED":
            active_tasks = [t for t in active_tasks if t["status"] == "Unassigned"]
        elif selected_view == "MY TASKS":
            active_tasks = [
                t for t in active_tasks
                if t["assigned_to"] == current_user and t["status"] in ("Assigned", "AC Met")
            ]


        active_tasks = sorted(
            active_tasks,
            key=lambda r: (
                0 if r["assigned_to"] == current_user and r["status"] in ("Assigned", "AC Met") else 1,
                sort_sta_value(r["sta"]),
                status_rank(r["status"]),
                r["flight_type"],
                r["flight"],
            ),
        )

        if not active_tasks:
            st.info("No active tasks match the current filters.")
        else:
            for task in active_tasks:
                render_task_card(task, current_user, history_mode=False)

    with tabs[1]:
        c1, c2, c3 = st.columns(3)
        with c1:
            selected_date_hist = st.selectbox(
                "Date",
                options=sorted({t["task_date"] for t in all_tasks}),
                index=0,
                key="history_date",
            )
        with c2:
            selected_type_hist = st.selectbox("Flight Type", ["ALL", "INT", "DOM", "QLK"], index=0, key="history_type")
        with c3:
            selected_history_view = st.selectbox(
                "History View",
                ["ALL HISTORY", "COMPLETED", "SKIPPED", "MY HISTORY"],
                index=0,
                key="history_view",
            )

        history_tasks = [
            t for t in all_tasks
            if t["task_date"] == selected_date_hist and t["status"] in ("Completed", "Skipped")
        ]

        if selected_type_hist != "ALL":
            history_tasks = [t for t in history_tasks if t["flight_type"] == selected_type_hist]

        if selected_history_view == "COMPLETED":
            history_tasks = [t for t in history_tasks if t["status"] == "Completed"]
        elif selected_history_view == "SKIPPED":
            history_tasks = [t for t in history_tasks if t["status"] == "Skipped"]
        elif selected_history_view == "MY HISTORY":
            history_tasks = [
                t for t in history_tasks
                if t["assigned_to"] == current_user
                or t["completed_by"] == current_user
                or t["skipped_by"] == current_user
            ]

        history_tasks = sorted(
            history_tasks,
            key=lambda r: (
                sort_sta_value(r["sta"]),
                status_rank(r["status"]),
                r["flight_type"],
                r["flight"],
            ),
        )

        if not history_tasks:
            st.info("No history items match the current filters.")
        else:
            for task in history_tasks:
                render_task_card(task, current_user, history_mode=True)


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="Tranship Ops Dashboard", layout="wide")
    init_db()
    ensure_session_defaults()
    inject_styles()

    if st.session_state.role is None:
        render_login()
    elif st.session_state.role == "admin":
        render_admin()
    else:
        render_user()


if __name__ == "__main__":
    main()
