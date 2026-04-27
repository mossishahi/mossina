"""Exchange rates: fetch external API and convert prices to EUR."""

from datetime import datetime, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from mossina_db.models import ExchangeRate

ER_API_URL = "https://open.er-api.com/v6/latest/EUR"


async def refresh_exchange_rates(db: AsyncSession) -> dict[str, float]:
    """
    Fetch latest EUR-based rates from open.er-api.com and upsert ExchangeRate rows.
    API gives foreign currency units per 1 EUR; we store multiply-by rate to get EUR from that currency.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(ER_API_URL, timeout=60.0)
        resp.raise_for_status()
        payload = resp.json()
    if payload.get("result") != "success":
        raise RuntimeError("exchange rate API returned non-success result")
    rates_raw = payload.get("rates") or {}
    now = datetime.now(timezone.utc)
    out: dict[str, float] = {"EUR": 1.0}

    for code, rate_val in rates_raw.items():
        cur = str(code).upper()
        if cur == "EUR":
            rate_to_eur = Decimal("1")
        else:
            api_rate = Decimal(str(rate_val))
            if api_rate == 0:
                continue
            # 1 EUR = api_rate units of foreign; foreign_to_eur = 1 / api_rate
            rate_to_eur = Decimal("1") / api_rate
        out[cur] = float(rate_to_eur)
        ins = pg_insert(ExchangeRate).values(
            currency=cur, rate_to_eur=rate_to_eur, updated_at=now
        )
        stmt = ins.on_conflict_do_update(
            index_elements=["currency"],
            set_={
                "rate_to_eur": ins.excluded.rate_to_eur,
                "updated_at": ins.excluded.updated_at,
            },
        )
        await db.execute(stmt)

    await db.commit()
    return out


async def get_rates(db: AsyncSession) -> dict[str, float]:
    """Load currency -> rate_to_eur from the database (amount in currency * rate = EUR)."""
    result = await db.execute(select(ExchangeRate.currency, ExchangeRate.rate_to_eur))
    rows = result.all()
    rates: dict[str, float] = {"EUR": 1.0}
    for currency, rate_to_eur in rows:
        rates[str(currency).upper()] = float(rate_to_eur)
    return rates


def convert_to_eur(price: float, currency: str, rates: dict) -> float | None:
    """Convert a price to EUR using preloaded rates. Returns None if currency is missing."""
    cur = str(currency).upper()
    if cur == "EUR":
        return float(price)
    r = rates.get(cur)
    if r is None:
        return None
    return float(price) * r
