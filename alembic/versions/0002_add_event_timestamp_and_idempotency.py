from __future__ import annotations

import sqlalchemy as sa

from alembic import op  # type: ignore[attr-defined]

revision = "0002_event_timestamp_idempotency"
down_revision = "0001_create_events_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("events", sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=True))
    op.execute("UPDATE events SET event_timestamp = created_at WHERE event_timestamp IS NULL")
    op.execute(
        """
        DELETE FROM events
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY id) AS row_num
                FROM events
                WHERE event_id IS NOT NULL
            ) ranked
            WHERE ranked.row_num > 1
        )
        """
    )
    op.alter_column("events", "event_timestamp", nullable=False)
    op.create_unique_constraint("uq_events_event_id", "events", ["event_id"])


def downgrade() -> None:
    op.drop_constraint("uq_events_event_id", "events", type_="unique")
    op.drop_column("events", "event_timestamp")
