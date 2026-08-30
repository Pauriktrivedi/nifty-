import os
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker

from database.models import Base

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///trading.db")
_connect_args = (
    {
        "check_same_thread": False,
        "timeout": float(os.getenv("SQLITE_BUSY_TIMEOUT_SEC", "12")),
    }
    if DATABASE_URL.startswith("sqlite")
    else {}
)
engine = create_engine(DATABASE_URL, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        # Improve concurrent read/write behavior under websocket tick load.
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=12000")
        cursor.close()

def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_marketdata_indexes()
    _ensure_strategy_columns()
    _ensure_snapshot_columns()


def _ensure_strategy_columns():
    inspector = inspect(engine)
    connection = engine.connect()
    try:
        if inspector.has_table("orders"):
            order_columns = {col["name"] for col in inspector.get_columns("orders")}
            if "strategy_id" not in order_columns:
                connection.execute(text("ALTER TABLE orders ADD COLUMN strategy_id VARCHAR"))
            if "broker_order_id" not in order_columns:
                connection.execute(text("ALTER TABLE orders ADD COLUMN broker_order_id VARCHAR"))
                if "kotak_order_id" in order_columns:
                    connection.execute(text("UPDATE orders SET broker_order_id = kotak_order_id WHERE broker_order_id IS NULL"))

        if inspector.has_table("trades"):
            trade_columns = {col["name"] for col in inspector.get_columns("trades")}
            if "strategy_id" not in trade_columns:
                connection.execute(text("ALTER TABLE trades ADD COLUMN strategy_id VARCHAR"))
    finally:
        connection.close()


def _ensure_marketdata_indexes():
    inspector = inspect(engine)
    connection = engine.connect()
    try:
        if inspector.has_table("market_data"):
            existing_indexes = {idx["name"] for idx in inspector.get_indexes("market_data")}
            if "idx_marketdata_timestamp" not in existing_indexes:
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_marketdata_timestamp ON market_data (timestamp)"))
            if "idx_marketdata_instrument_timestamp" not in existing_indexes:
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_marketdata_instrument_timestamp ON market_data (instrument_type, timestamp)"))
            if "idx_marketdata_trading_symbol_timestamp" not in existing_indexes:
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_marketdata_trading_symbol_timestamp ON market_data (trading_symbol, timestamp)"))
    finally:
        connection.close()


def _ensure_snapshot_columns():
    inspector = inspect(engine)
    connection = engine.connect()
    try:
        if inspector.has_table("oi_snapshots"):
            snapshot_columns = {col["name"] for col in inspector.get_columns("oi_snapshots")}
            if "row_details_json" not in snapshot_columns:
                connection.execute(text("ALTER TABLE oi_snapshots ADD COLUMN row_details_json VARCHAR"))
    finally:
        connection.close()

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
