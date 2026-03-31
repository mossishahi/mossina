"""Route ORM model."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Route(Base):
    __tablename__ = "routes"
    __table_args__ = (
        UniqueConstraint("origin", "destination", "airline", name="uq_routes_origin_destination_airline"),
        Index("ix_routes_origin", "origin"),
        Index("ix_routes_destination", "destination"),
        Index("ix_routes_airline", "airline"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    origin: Mapped[str] = mapped_column(
        String(3),
        ForeignKey("airports.iata_code"),
        nullable=False,
    )
    destination: Mapped[str] = mapped_column(
        String(3),
        ForeignKey("airports.iata_code"),
        nullable=False,
    )
    airline: Mapped[str] = mapped_column(String(2), nullable=False)
    is_connecting: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    new_route: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
