from __future__ import annotations

import sqlalchemy as sa

from alembic import op  # type: ignore[attr-defined]

revision = "0003_event_required_fields"
down_revision = "0002_event_timestamp_idempotency"
branch_labels = None
depends_on = None

_INVALID_ROWS_QUERY = sa.text(
    """
    SELECT COUNT(*)
    FROM events
    WHERE event_id IS NULL
       OR btrim(event_id) = ''
       OR event_timestamp IS NULL
       OR type IS NULL
       OR btrim(type) = ''
       OR source IS NULL
       OR btrim(source) = ''
       OR payload IS NULL
    """
)

_INVALID_ROW_IDS_QUERY = sa.text(
    """
    SELECT id
    FROM events
    WHERE event_id IS NULL
       OR btrim(event_id) = ''
       OR event_timestamp IS NULL
       OR type IS NULL
       OR btrim(type) = ''
       OR source IS NULL
       OR btrim(source) = ''
       OR payload IS NULL
    ORDER BY id
    LIMIT 5
    """
)


def upgrade() -> None:
    conn = op.get_bind()
    invalid_rows = int(conn.execute(_INVALID_ROWS_QUERY).scalar_one())

    if invalid_rows:
        sample_ids = [str(row_id) for row_id in conn.execute(_INVALID_ROW_IDS_QUERY).scalars()]
        sample_text = ", ".join(sample_ids) if sample_ids else "unknown"
        raise RuntimeError(
            "Migration aborted: found "
            f"{invalid_rows} event row(s) with NULL or blank values in required columns "
            f"(sample ids: {sample_text}). "
            "Clean up legacy rows so event_id, event_timestamp, type, source, and payload "
            "are all present, and event_id/type/source are non-blank, then rerun "
            "`alembic upgrade head`."
        )

    op.alter_column("events", "event_id", existing_type=sa.Text(), nullable=False)
    op.alter_column(
        "events",
        "event_timestamp",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )
    op.alter_column("events", "type", existing_type=sa.Text(), nullable=False)
    op.alter_column("events", "source", existing_type=sa.Text(), nullable=False)
    op.alter_column("events", "payload", existing_type=sa.JSON(), nullable=False)
    op.create_check_constraint("ck_events_event_id_not_blank", "events", "btrim(event_id) <> ''")
    op.create_check_constraint("ck_events_type_not_blank", "events", "btrim(type) <> ''")
    op.create_check_constraint("ck_events_source_not_blank", "events", "btrim(source) <> ''")


def downgrade() -> None:
    op.drop_constraint("ck_events_source_not_blank", "events", type_="check")
    op.drop_constraint("ck_events_type_not_blank", "events", type_="check")
    op.drop_constraint("ck_events_event_id_not_blank", "events", type_="check")
    op.alter_column("events", "payload", existing_type=sa.JSON(), nullable=True)
    op.alter_column("events", "source", existing_type=sa.Text(), nullable=True)
    op.alter_column("events", "type", existing_type=sa.Text(), nullable=True)
    op.alter_column(
        "events",
        "event_timestamp",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )
    op.alter_column("events", "event_id", existing_type=sa.Text(), nullable=True)
