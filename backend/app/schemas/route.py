"""Route API schemas."""

from pydantic import BaseModel, ConfigDict


class RouteOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    origin: str
    destination: str
    airline: str
    airline_name: str | None = None
