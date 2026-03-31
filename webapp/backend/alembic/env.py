"""Alembic environment configuration.

Reads DATABASE_URL_SYNC from the environment so the same migration can
run in any deployment without editing alembic.ini.
"""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database import Base  # noqa: E402
from app.models import (  # noqa: E402, F401
    Airport,
    Country,
    ExchangeRate,
    Fare,
    Route,
    Schedule,
)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

DATABASE_URL_SYNC = os.getenv(
    "DATABASE_URL_SYNC",
    config.get_main_option("sqlalchemy.url",
                           "postgresql://mossina:mossina@localhost:5432/mossina"),
)


def run_migrations_offline():
    """Run migrations in 'offline' mode -- emits SQL to stdout."""
    context.configure(
        url=DATABASE_URL_SYNC,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode -- connects to the database."""
    connectable = create_engine(DATABASE_URL_SYNC)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
