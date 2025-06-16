"""Add composite index on Organo niveles

Revision ID: d2611d6eb6f5
Revises: 5598308c1cc1
Create Date: 2025-06-16 17:10:39.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'd2611d6eb6f5'
down_revision: Union[str, None] = '5598308c1cc1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create index ix_organo_nivel1_nivel2_nivel3"""
    op.create_index(
        'ix_organo_nivel1_nivel2_nivel3',
        'organo',
        ['nivel1', 'nivel2', 'nivel3'],
        unique=False,
    )


def downgrade() -> None:
    """Drop index ix_organo_nivel1_nivel2_nivel3"""
    op.drop_index('ix_organo_nivel1_nivel2_nivel3', table_name='organo')
