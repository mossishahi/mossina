"""Cached exchange rate endpoints."""

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from mossina_db.models import ExchangeRate

router = APIRouter(prefix="/exchange-rates", tags=["exchange-rates"])


class ExchangeRateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    currency: str
    rate_to_eur: Decimal
    updated_at: datetime | None = None


@router.get("", response_model=list[ExchangeRateOut])
async def list_exchange_rates(db: AsyncSession = Depends(get_db)):
    stmt = select(ExchangeRate).order_by(ExchangeRate.currency)
    rows = (await db.scalars(stmt)).all()
    return [
        ExchangeRateOut(
            currency=r.currency,
            rate_to_eur=r.rate_to_eur,
            updated_at=r.updated_at,
        )
        for r in rows
    ]


@router.post("/refresh")
async def refresh_rates(db: AsyncSession = Depends(get_db)):
    from app.services.exchange_rates import refresh_exchange_rates
    rates = await refresh_exchange_rates(db)
    return {"currencies": len(rates), "status": "ok"}
