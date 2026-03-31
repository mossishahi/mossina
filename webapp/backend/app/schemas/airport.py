"""Airport and country API schemas."""

from pydantic import BaseModel, ConfigDict, Field


class AirportOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    iata_code: str
    name: str | None = None
    city: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class CountryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    name: str | None = None
    currency: str | None = None
    airport_count: int | None = None


class AirportSearchResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    airports: list[AirportOut] = Field(default_factory=list)
    countries: list[CountryOut] = Field(default_factory=list)
