"""doc_chunk for rag

Revision ID: b7d41c0a9e21
Revises: 8aafcffee773
Create Date: 2026-10-23 18:00:00

"""

from typing import Sequence, Union

from alembic import op
import pgvector.sqlalchemy
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b7d41c0a9e21"
down_revision: Union[str, Sequence[str], None] = "8aafcffee773"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Расширение ставится в базу один раз; нужен образ PostgreSQL с pgvector
    # (pgvector/pgvector:0.8.7-pg16 в compose и тестах).
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "doc_chunk",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("anchor", sa.String(length=128), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("embedding", pgvector.sqlalchemy.VECTOR(dim=768), nullable=False),
        sa.Column("embedding_model", sa.String(length=128), nullable=False),
        sa.Column("source_version", sa.String(length=64), nullable=False),
        sa.Column(
            "tsv",
            postgresql.TSVECTOR(),
            sa.Computed("to_tsvector('russian', title || ' ' || text)", persisted=True),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_doc_chunk")),
        sa.UniqueConstraint("source", "anchor", name="uq_doc_chunk_source_anchor"),
    )
    op.create_index("ix_doc_chunk_tsv", "doc_chunk", ["tsv"], unique=False, postgresql_using="gin")
    op.create_index(
        "ix_doc_chunk_embedding",
        "doc_chunk",
        ["embedding"],
        unique=False,
        postgresql_using="hnsw",
        postgresql_ops={"embedding": "vector_cosine_ops"},
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_doc_chunk_embedding", table_name="doc_chunk", postgresql_using="hnsw")
    op.drop_index("ix_doc_chunk_tsv", table_name="doc_chunk", postgresql_using="gin")
    op.drop_table("doc_chunk")
    # Расширение не удаляем: им могут пользоваться и другие таблицы.
