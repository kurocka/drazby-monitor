import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "drazby.db")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS auctions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT,
            subject_type TEXT,
            subject_subtype TEXT,
            region TEXT,
            district TEXT,
            city TEXT,
            address TEXT,
            auction_date TEXT,
            price REAL,
            currency TEXT DEFAULT 'EUR',
            status TEXT,
            description TEXT,
            url TEXT,
            lat REAL,
            lon REAL,
            raw_data TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_auctions_source ON auctions(source);
        CREATE INDEX IF NOT EXISTS idx_auctions_subject_type ON auctions(subject_type);
        CREATE INDEX IF NOT EXISTS idx_auctions_region ON auctions(region);
        CREATE INDEX IF NOT EXISTS idx_auctions_district ON auctions(district);
        CREATE INDEX IF NOT EXISTS idx_auctions_status ON auctions(status);
        CREATE INDEX IF NOT EXISTS idx_auctions_auction_date ON auctions(auction_date);
        CREATE INDEX IF NOT EXISTS idx_auctions_price ON auctions(price);

        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            sync_type TEXT,
            started_at TEXT DEFAULT (datetime('now')),
            finished_at TEXT,
            records_fetched INTEGER DEFAULT 0,
            records_new INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subject_types TEXT,
            regions TEXT,
            districts TEXT,
            price_min REAL,
            price_max REAL,
            keywords TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()


def upsert_auction(conn, auction):
    conn.execute("""
        INSERT INTO auctions (id, source, title, subject_type, subject_subtype,
            region, district, city, address, auction_date, price, currency,
            status, description, url, lat, lon, raw_data, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, status=excluded.status, price=excluded.price,
            auction_date=excluded.auction_date, description=excluded.description,
            raw_data=excluded.raw_data, updated_at=datetime('now')
    """, (
        auction["id"], auction["source"], auction.get("title"),
        auction.get("subject_type"), auction.get("subject_subtype"),
        auction.get("region"), auction.get("district"),
        auction.get("city"), auction.get("address"),
        auction.get("auction_date"), auction.get("price"),
        auction.get("currency", "EUR"), auction.get("status"),
        auction.get("description"), auction.get("url"),
        auction.get("lat"), auction.get("lon"),
        auction.get("raw_data")
    ))
