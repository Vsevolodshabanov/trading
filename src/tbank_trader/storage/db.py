from __future__ import annotations

import time

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from tbank_trader.storage.models import Base


def build_engine(database_url: str) -> Engine:
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(database_url, future=True, connect_args=connect_args)


def build_session_factory(database_url: str) -> sessionmaker[Session]:
    engine = build_engine(database_url)
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_database(engine: Engine, retries: int = 20, delay_seconds: float = 1.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                connection.commit()
            Base.metadata.create_all(engine)
            return
        except Exception:
            if attempt == retries:
                raise
            time.sleep(delay_seconds)
