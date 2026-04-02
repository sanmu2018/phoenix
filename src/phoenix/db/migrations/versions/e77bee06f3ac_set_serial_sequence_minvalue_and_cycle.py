"""set_serial_sequence_minvalue_and_cycle

Revision ID: e77bee06f3ac
Revises: aba52fffe1a1
Create Date: 2026-04-01 20:59:18.906153

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e77bee06f3ac"
down_revision: Union[str, None] = "aba52fffe1a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# All tables using serial (32-bit) primary keys.
_SERIAL_TABLES = (
    "access_tokens",
    "annotation_configs",
    "api_keys",
    "dataset_example_revisions",
    "dataset_examples",
    "dataset_versions",
    "datasets",
    "document_annotations",
    "experiment_run_annotations",
    "experiment_runs",
    "experiments",
    "password_reset_tokens",
    "project_annotation_configs",
    "project_sessions",
    "project_trace_retention_policies",
    "projects",
    "prompt_labels",
    "prompt_version_tags",
    "prompt_versions",
    "prompts",
    "prompts_prompt_labels",
    "refresh_tokens",
    "span_annotations",
    "spans",
    "trace_annotations",
    "traces",
    "user_roles",
    "users",
)


def upgrade() -> None:
    # Set MINVALUE to the smallest 32-bit integer and enable CYCLE so sequences
    # wrap around instead of erroring when they reach MAXVALUE. This is idempotent
    # — safe to run on both new and existing deployments.
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        for table in _SERIAL_TABLES:
            op.execute(sa.text(f"ALTER SEQUENCE {table}_id_seq MINVALUE -2147483648 CYCLE"))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        for table in _SERIAL_TABLES:
            # Use RESTART WITH 1 in the same statement so PostgreSQL doesn't
            # reject the new MINVALUE due to a negative current position.
            op.execute(sa.text(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1 MINVALUE 1 NO CYCLE"))
