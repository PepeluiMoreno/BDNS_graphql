"""inicial

Revision ID: 53b0a952fdec
Revises: 
Create Date: 2025-06-25 01:14:42.968797

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '53b0a952fdec'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('anuncio',
    sa.Column('num_anuncio', sa.Integer(), nullable=False),
    sa.Column('titulo', sa.Text(), nullable=True),
    sa.Column('titulo_leng', sa.Text(), nullable=True),
    sa.Column('texto', sa.Text(), nullable=True),
    sa.Column('texto_leng', sa.Text(), nullable=True),
    sa.Column('url', sa.String(), nullable=True),
    sa.Column('des_diario_oficial', sa.String(), nullable=True),
    sa.Column('fecha_publicacion', sa.Date(), nullable=True),
    sa.PrimaryKeyConstraint('num_anuncio')
    )
    op.create_table('documento',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('nombre_fic', sa.String(), nullable=True),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('longitud', sa.Integer(), nullable=True),
    sa.Column('fecha_modificacion', sa.Date(), nullable=True),
    sa.Column('fecha_publicacion', sa.Date(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('finalidad',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('fondo',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('instrumento',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('objetivo',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('organo',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('id_padre', sa.String(), nullable=True),
    sa.Column('nombre', sa.String(), nullable=False),
    sa.Column('tipo', sa.Enum('GEOGRAFICO', 'AUTONOMICO', 'LOCAL', 'CENTRAL', 'OTRO', name='tipo_organo_enum', native_enum=False), nullable=False),
    sa.Column('nivel1', sa.String(), nullable=True),
    sa.Column('nivel2', sa.String(), nullable=True),
    sa.Column('nivel3', sa.String(), nullable=True),
    sa.Column('nivel1_norm', sa.String(), nullable=True),
    sa.Column('nivel2_norm', sa.String(), nullable=True),
    sa.Column('nivel3_norm', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['id_padre'], ['organo.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_organo_nivel1_nivel2_nivel3', 'organo', ['nivel1_norm', 'nivel2_norm', 'nivel3_norm'], unique=False)
    op.create_table('region',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=False),
    sa.Column('descripcion_norm', sa.String(), nullable=False),
    sa.Column('id_padre', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['id_padre'], ['region.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('reglamento',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('sector_actividad',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=False),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.Column('id_padre', sa.String(), nullable=True),
    sa.ForeignKeyConstraint(['id_padre'], ['sector_actividad.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_sector_actividad_descripcion_norm'), 'sector_actividad', ['descripcion_norm'], unique=False)
    op.create_table('sector_producto',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('tipo_beneficiario',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('descripcion', sa.String(), nullable=True),
    sa.Column('descripcion_norm', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('convocatoria',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('codigo_bdns', sa.String(), nullable=False),
    sa.Column('descripcion', sa.Text(), nullable=True),
    sa.Column('descripcion_leng', sa.Text(), nullable=True),
    sa.Column('descripcion_finalidad', sa.Text(), nullable=True),
    sa.Column('descripcion_bases', sa.Text(), nullable=True),
    sa.Column('url_bases', sa.String(), nullable=True),
    sa.Column('url_ayuda_estado', sa.String(), nullable=True),
    sa.Column('ayuda_estado', sa.String(), nullable=True),
    sa.Column('tipo_convocatoria', sa.String(), nullable=True),
    sa.Column('sede_electronica', sa.String(), nullable=True),
    sa.Column('abierto', sa.Boolean(), nullable=True),
    sa.Column('se_publica_diario_oficial', sa.Boolean(), nullable=True),
    sa.Column('presupuesto_total', sa.Float(), nullable=True),
    sa.Column('mrr', sa.Boolean(), nullable=True),
    sa.Column('fecha_recepcion', sa.Date(), nullable=True),
    sa.Column('fecha_inicio_solicitud', sa.Date(), nullable=True),
    sa.Column('fecha_fin_solicitud', sa.Date(), nullable=True),
    sa.Column('organo_id', sa.String(), nullable=True),
    sa.Column('reglamento_id', sa.Integer(), nullable=True),
    sa.Column('finalidad_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['finalidad_id'], ['finalidad.id'], ),
    sa.ForeignKeyConstraint(['organo_id'], ['organo.id'], ),
    sa.ForeignKeyConstraint(['reglamento_id'], ['reglamento.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_convocatoria_codigo_bdns'), 'convocatoria', ['codigo_bdns'], unique=False)
    op.create_table('ayuda_estado',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('id_concesion', sa.Integer(), nullable=False),
    sa.Column('numero_convocatoria', sa.String(), nullable=True),
    sa.Column('id_convocatoria', sa.Integer(), nullable=False),
    sa.Column('id_persona', sa.Integer(), nullable=True),
    sa.Column('codigo_concesion', sa.String(), nullable=True),
    sa.Column('beneficiario', sa.String(), nullable=True),
    sa.Column('fecha_concesion', sa.Date(), nullable=True),
    sa.Column('fecha_registro', sa.Date(), nullable=True),
    sa.Column('ayuda_equivalente', sa.Float(), nullable=True),
    sa.Column('ayuda_estado', sa.Integer(), nullable=True),
    sa.Column('url_ayuda_estado', sa.String(), nullable=True),
    sa.Column('entidad', sa.String(), nullable=True),
    sa.Column('intermediario', sa.String(), nullable=True),
    sa.Column('instrumento_id', sa.Integer(), nullable=True),
    sa.Column('reglamento_id', sa.Integer(), nullable=True),
    sa.Column('sector_actividad_id', sa.String(), nullable=True),
    sa.Column('sector_producto_id', sa.Integer(), nullable=True),
    sa.Column('region_id', sa.Integer(), nullable=True),
    sa.Column('objetivo_id', sa.Integer(), nullable=True),
    sa.Column('tipo_beneficiario_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['id_convocatoria'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['instrumento_id'], ['instrumento.id'], ),
    sa.ForeignKeyConstraint(['objetivo_id'], ['objetivo.id'], ),
    sa.ForeignKeyConstraint(['region_id'], ['region.id'], ),
    sa.ForeignKeyConstraint(['reglamento_id'], ['reglamento.id'], ),
    sa.ForeignKeyConstraint(['sector_actividad_id'], ['sector_actividad.id'], ),
    sa.ForeignKeyConstraint(['sector_producto_id'], ['sector_producto.id'], ),
    sa.ForeignKeyConstraint(['tipo_beneficiario_id'], ['tipo_beneficiario.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_ayuda_estado_ayuda_estado'), 'ayuda_estado', ['ayuda_estado'], unique=False)
    op.create_index(op.f('ix_ayuda_estado_codigo_concesion'), 'ayuda_estado', ['codigo_concesion'], unique=True)
    op.create_index(op.f('ix_ayuda_estado_fecha_concesion'), 'ayuda_estado', ['fecha_concesion'], unique=False)
    op.create_index(op.f('ix_ayuda_estado_fecha_registro'), 'ayuda_estado', ['fecha_registro'], unique=False)
    op.create_index(op.f('ix_ayuda_estado_id_concesion'), 'ayuda_estado', ['id_concesion'], unique=True)
    op.create_index(op.f('ix_ayuda_estado_id_convocatoria'), 'ayuda_estado', ['id_convocatoria'], unique=False)
    op.create_index(op.f('ix_ayuda_estado_id_persona'), 'ayuda_estado', ['id_persona'], unique=False)
    op.create_index(op.f('ix_ayuda_estado_numero_convocatoria'), 'ayuda_estado', ['numero_convocatoria'], unique=False)
    op.create_table('concesion',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('id_convocatoria', sa.Integer(), nullable=False),
    sa.Column('numero_convocatoria', sa.String(), nullable=True),
    sa.Column('descripcion_convocatoria', sa.String(), nullable=True),
    sa.Column('descripcion_cooficial', sa.String(), nullable=True),
    sa.Column('nivel1', sa.String(), nullable=True),
    sa.Column('nivel2', sa.String(), nullable=True),
    sa.Column('nivel3', sa.String(), nullable=True),
    sa.Column('codigo_invente', sa.String(), nullable=True),
    sa.Column('fecha_concesion', sa.Date(), nullable=True),
    sa.Column('id_persona', sa.Integer(), nullable=True),
    sa.Column('beneficiario', sa.String(), nullable=True),
    sa.Column('importe', sa.Float(), nullable=True),
    sa.Column('ayuda_equivalente', sa.Float(), nullable=True),
    sa.Column('url_br', sa.String(), nullable=True),
    sa.Column('tiene_proyecto', sa.Boolean(), nullable=True),
    sa.Column('instrumento_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['id_convocatoria'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['instrumento_id'], ['instrumento.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_concesion_id_convocatoria'), 'concesion', ['id_convocatoria'], unique=False)
    op.create_index(op.f('ix_concesion_numero_convocatoria'), 'concesion', ['numero_convocatoria'], unique=False)
    op.create_table('convocatoria_anuncio',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('anuncio_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['anuncio_id'], ['anuncio.num_anuncio'], ),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'anuncio_id')
    )
    op.create_table('convocatoria_documento',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('documento_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['documento_id'], ['documento.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'documento_id')
    )
    op.create_table('convocatoria_finalidad',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('finalidad_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['finalidad_id'], ['finalidad.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'finalidad_id')
    )
    op.create_table('convocatoria_fondo',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('fondo_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['fondo_id'], ['fondo.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'fondo_id')
    )
    op.create_table('convocatoria_instrumento',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('instrumento_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['instrumento_id'], ['instrumento.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'instrumento_id')
    )
    op.create_table('convocatoria_objetivo',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('objetivo_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['objetivo_id'], ['objetivo.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'objetivo_id')
    )
    op.create_table('convocatoria_region',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('region_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['region_id'], ['region.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'region_id')
    )
    op.create_table('convocatoria_sector_actividad',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('sector_actividad_id', sa.String(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['sector_actividad_id'], ['sector_actividad.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'sector_actividad_id')
    )
    op.create_table('convocatoria_sector_producto',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('sector_producto_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['sector_producto_id'], ['sector_producto.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'sector_producto_id')
    )
    op.create_table('convocatoria_tipo_beneficiario',
    sa.Column('convocatoria_id', sa.Integer(), nullable=False),
    sa.Column('tipo_beneficiario_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['convocatoria_id'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['tipo_beneficiario_id'], ['tipo_beneficiario.id'], ),
    sa.PrimaryKeyConstraint('convocatoria_id', 'tipo_beneficiario_id')
    )
    op.create_table('minimi',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('codigo_concesion', sa.String(), nullable=True),
    sa.Column('numero_convocatoria', sa.String(), nullable=True),
    sa.Column('id_convocatoria', sa.Integer(), nullable=False),
    sa.Column('id_persona', sa.Integer(), nullable=True),
    sa.Column('beneficiario', sa.String(), nullable=True),
    sa.Column('fecha_concesion', sa.Date(), nullable=True),
    sa.Column('fecha_registro', sa.Date(), nullable=True),
    sa.Column('ayuda_equivalente', sa.Float(), nullable=True),
    sa.Column('instrumento_id', sa.Integer(), nullable=True),
    sa.Column('reglamento_id', sa.Integer(), nullable=True),
    sa.Column('sector_actividad_id', sa.String(), nullable=True),
    sa.Column('sector_producto_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['id_convocatoria'], ['convocatoria.id'], ),
    sa.ForeignKeyConstraint(['instrumento_id'], ['instrumento.id'], ),
    sa.ForeignKeyConstraint(['reglamento_id'], ['reglamento.id'], ),
    sa.ForeignKeyConstraint(['sector_actividad_id'], ['sector_actividad.id'], ),
    sa.ForeignKeyConstraint(['sector_producto_id'], ['sector_producto.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_minimi_codigo_concesion'), 'minimi', ['codigo_concesion'], unique=True)
    op.create_index(op.f('ix_minimi_fecha_concesion'), 'minimi', ['fecha_concesion'], unique=False)
    op.create_index(op.f('ix_minimi_fecha_registro'), 'minimi', ['fecha_registro'], unique=False)
    op.create_index(op.f('ix_minimi_id_convocatoria'), 'minimi', ['id_convocatoria'], unique=False)
    op.create_index(op.f('ix_minimi_id_persona'), 'minimi', ['id_persona'], unique=False)
    op.create_index(op.f('ix_minimi_numero_convocatoria'), 'minimi', ['numero_convocatoria'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_minimi_numero_convocatoria'), table_name='minimi')
    op.drop_index(op.f('ix_minimi_id_persona'), table_name='minimi')
    op.drop_index(op.f('ix_minimi_id_convocatoria'), table_name='minimi')
    op.drop_index(op.f('ix_minimi_fecha_registro'), table_name='minimi')
    op.drop_index(op.f('ix_minimi_fecha_concesion'), table_name='minimi')
    op.drop_index(op.f('ix_minimi_codigo_concesion'), table_name='minimi')
    op.drop_table('minimi')
    op.drop_table('convocatoria_tipo_beneficiario')
    op.drop_table('convocatoria_sector_producto')
    op.drop_table('convocatoria_sector_actividad')
    op.drop_table('convocatoria_region')
    op.drop_table('convocatoria_objetivo')
    op.drop_table('convocatoria_instrumento')
    op.drop_table('convocatoria_fondo')
    op.drop_table('convocatoria_finalidad')
    op.drop_table('convocatoria_documento')
    op.drop_table('convocatoria_anuncio')
    op.drop_index(op.f('ix_concesion_numero_convocatoria'), table_name='concesion')
    op.drop_index(op.f('ix_concesion_id_convocatoria'), table_name='concesion')
    op.drop_table('concesion')
    op.drop_index(op.f('ix_ayuda_estado_numero_convocatoria'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_id_persona'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_id_convocatoria'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_id_concesion'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_fecha_registro'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_fecha_concesion'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_codigo_concesion'), table_name='ayuda_estado')
    op.drop_index(op.f('ix_ayuda_estado_ayuda_estado'), table_name='ayuda_estado')
    op.drop_table('ayuda_estado')
    op.drop_index(op.f('ix_convocatoria_codigo_bdns'), table_name='convocatoria')
    op.drop_table('convocatoria')
    op.drop_table('tipo_beneficiario')
    op.drop_table('sector_producto')
    op.drop_index(op.f('ix_sector_actividad_descripcion_norm'), table_name='sector_actividad')
    op.drop_table('sector_actividad')
    op.drop_table('reglamento')
    op.drop_table('region')
    op.drop_index('ix_organo_nivel1_nivel2_nivel3', table_name='organo')
    op.drop_table('organo')
    op.drop_table('objetivo')
    op.drop_table('instrumento')
    op.drop_table('fondo')
    op.drop_table('finalidad')
    op.drop_table('documento')
    op.drop_table('anuncio')
    # ### end Alembic commands ###
