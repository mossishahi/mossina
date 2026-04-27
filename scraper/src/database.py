"""Synchronous SQLAlchemy engine and session factory."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mossina_db.models import (  # noqa: F401
    Airport,
    Base,
    Country,
    ExchangeRate,
    Fare,
    PriceHistory,
    Route,
    RouteHistory,
    Schedule,
)

from src.config import DATABASE_URL_SYNC

engine = create_engine(DATABASE_URL_SYNC, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
