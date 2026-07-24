import os
import sys
import sqlite3
from datetime import datetime

from PySide6.QtGui import QFont
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QMessageBox, QFormLayout, QSpinBox, QTextEdit, QComboBox, QFileDialog
)

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

import barcode
from barcode.writer import ImageWriter
from PIL import Image, ImageDraw, ImageFont


APP_TITLE = "stock management"
BASE_DIR = r"\\dionas\kanri\bihin\system"
os.makedirs(BASE_DIR, exist_ok=True)
DB_File = os.path.join(BASE_DIR, "inventory.db")
LABEL_DIR = os.path.join(BASE_DIR, "labels")
os.makedirs(LABEL_DIR, exist_ok=True)


class DB:
    def __init__(self, path: str = DB_File):
        self.path = path
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout = 30000;")
        try:
            self.conn.execute("PRAGMA journal_mode = WAL;")
        except Exception:
            self.conn.execute("PRAGMA journal_mode = DELETE;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self._init_schema()

    def _init_schema(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            location TEXT,
            unit TEXT,
            safety_stock INTEGER DEFAULT 0,
            note TEXT, 
            is_active INTEGER DEFAULT 1
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            item_id INTEGER PRIMARY KEY,
            qty INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            type TEXT NOT NULL,
            item_id INTEGER NOT NULL,
            qty INTEGER NOT NULL,
            supplier TEXT,
            user TEXT,
            destination TEXT,
            requester TEXT,
            admin_handler, TEXT,
            memo TEXT,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def get_next_code(self) -> str:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT code FROM items
            WHERE code GLOB '[0-9]*';
        """)
        used = set()
        for row in cur.fetchall():
            try:
                used.add(int(row["code"]))
            except:
                pass

        n = 10001
        while n in used:
            n += 1
        return str(n)

    def deactivate_item_free_code(self, item_id: int):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT code FROM items
            WHERE id=?;
        """, (item_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError("item not found")

        old_code = row["code"]
        date_str = datetime.now().strftime("%Y%m")
        retired_code = f"X{old_code}_{date_str}"

        cur.execute("""
            UPDATE items
            SET is_active=0,
                code=?
            WHERE id=?;
        """, (retired_code, item_id))
        self.conn.commit()

    def add_item(self, code: str, name: str, location: str, unit: str, safety_stock: int, note: str):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO items (code, name, location, unit, safety_stock, note, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1);
        """, (code, name, location, unit, safety_stock, note))
        self.conn.commit()

    def update_item(self, item_id: int, code: str, name: str, location: str, unit: str,
                    safety_stock: int, note: str, is_active: int = 1):
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE items
            SET code=?,
                name=?,
                location=?,
                unit=?,
                safety_stock=?,
                note=?,
                is_active=?
            WHERE id=?;
        """, (code, name, location, unit, safety_stock, note, is_active, item_id))
        self.conn.commit()

    def get_item_by_code(self, code):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT i.*, COALESCE(s.qty,0) as qty
            FROM items i
            LEFT JOIN (
                SELECT item_id,
                       SUM(CASE WHEN type='IN' THEN qty ELSE -qty END) AS qty
                FROM transactions
                GROUP BY item_id
            ) s ON s.item_id = i.id
            WHERE i.code=? AND i.is_active=1;
        """, (code,))
        return cur.fetchone()

    def list_items(self, keyword: str = ""):
        kw = f"%{(keyword or '').strip()}%"
        cur = self.conn.cursor()
        cur.execute("""
            SELECT
                i.*,
                COALESCE(s.qty, 0) AS qty,
                CASE
                    WHEN COALESCE (s.qty, 0) >= COALESCE(i.safety_stock, 0)
                    THEN 'OK'
                    ELSE 'LACK OF STOCK'
                END AS status
            FROM items i
            LEFT JOIN (
                SELECT item_id, SUM(CASE WHEN type='IN' THEN qty ELSE -qty END) AS qty
                FROM transactions
                GROUP BY item_id
            ) s ON s.item_id = i.id
            WHERE i.is_active=1
              AND (
                    i.code LIKE ?
                 OR i.name LIKE ?
                 OR COALESCE(i.location,'') LIKE ?
                 OR COALESCE(i.unit,'') LIKE ?
                 OR COALESCE(i.note,'') LIKE ?
                 OR status LIKE ?
              )
            ORDER BY CAST(i.code AS INTEGER) ASC;
        """, (kw, kw, kw, kw, kw, kw))
        return cur.fetchall()

    def list_transactions_by_type(self, tx_type: str, keyword: str = "", limit: int = 5000,
                                  start_ts: str | None = None, end_ts: str | None = None):
        kw = f"%{(keyword or '').strip()}%"
        cur = self.conn.cursor()

        where_ts = ""
        params = [tx_type]

        if start_ts and end_ts:
            where_ts = " AND t.ts >= ? AND t.ts < ? "
            params += [start_ts, end_ts]

        params += [
            kw, kw, kw, kw,
            kw, kw,
            kw, kw, kw, kw,
            int(limit)
        ]

        cur.execute(f"""
            SELECT t.*, i.code, i.name,i.unit
            FROM transactions t
            JOIN items i ON i.id = t.item_id
            WHERE t.type = ?
              {where_ts}
              AND (
                    i.code LIKE ?
                 OR i.name LIKE ?
                 OR COALESCE(i.location,'') LIKE ?
                 OR COALESCE(i.note,'') LIKE ?
                 OR COALESCE(t.user,'') LIKE ?
                 OR COALESCE(t.destination,'') LIKE ?
                 OR COALESCE(t.memo,'') LIKE ?
                 OR COALESCE(t.supplier,'') LIKE ?
                 OR COALESCE(t.requester,'') LIKE ?
                 OR COALESCE(t.admin_handler,'') LIKE ?
              )
            ORDER BY t.ts DESC
            LIMIT ?;
        """, tuple(params))
        return cur.fetchall()

    def add_in_tx(self, item_id: int, qty: int, supplier: str, user: str, memo: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO transactions
            (ts, type, item_id, qty, supplier, user, memo)
            VALUES (?, 'IN', ?, ?, ?, ?, ?);
        """, (ts, item_id, qty, supplier, user, memo))
        self.conn.commit()

    def add_out_tx(self, item_id: int, qty: int, destination: str, requester: str, admin_handler: str, memo: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO transactions
            (ts, type, item_id, qty, destination, requester, admin_handler, memo)
            VALUES (?, 'OUT', ?, ?, ?, ?, ?, ?);
        """, (ts, item_id, qty, destination, requester, admin_handler, memo))
        self.conn.commit()




