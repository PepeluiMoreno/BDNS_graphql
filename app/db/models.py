# models.py
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    Boolean,
    Text,
    Enum as SQLEnum,
    ForeignKey,
    Index,
    Table
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from db.enums import TipoOrgano

Base = declarative_base()

# ================= CATÁLOGOS =================

class Anuncio(Base):
    __tablename__ = "anuncio"
    num_anuncio = Column(Integer, primary_key=True)
    titulo = Column(Text)
    titulo_leng = Column(Text)
    texto = Column(Text)
    texto_leng = Column(Text)
    url = Column(String)
    des_diario_oficial = Column(String)
    fecha_publicacion = Column(Date)
    convocatorias = relationship("Convocatoria", secondary="convocatoria_anuncio", back_populates="anuncios")

class Documento(Base):
    __tablename__ = "documento"
    id = Column(Integer, primary_key=True)
    nombre_fic = Column(String)
    descripcion = Column(String)
    longitud = Column(Integer)
    fecha_modificacion = Column(Date)
    fecha_publicacion = Column(Date)
    convocatorias = relationship("Convocatoria", secondary="convocatoria_documento", back_populates="documentos")

class Finalidad(Base):
    __tablename__ = "finalidad"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Fondo(Base):
    __tablename__ = "fondo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Instrumento(Base):
    __tablename__ = "instrumento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Objetivo(Base):
    __tablename__ = "objetivo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Reglamento(Base):
    __tablename__ = "reglamento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Region(Base):
    __tablename__ = "region"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String, nullable=False)
    descripcion_norm = Column(String, nullable=False)
    id_padre = Column(Integer, ForeignKey("region.id"), nullable=True)
    padre = relationship("Region", remote_side=[id], backref="hijos")

class SectorActividad(Base):
    __tablename__ = "sector_actividad"
    id = Column(String, primary_key=True)  # ← antes era Integer
    descripcion = Column(String, nullable=False)
    descripcion_norm = Column(String, index=True)
    id_padre = Column(String, ForeignKey("sector_actividad.id"), nullable=True)

    padre = relationship("SectorActividad", remote_side=[id], backref="hijos")

class SectorProducto(Base):
    __tablename__ = "sector_producto"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class TipoBeneficiario(Base):
    __tablename__ = "tipo_beneficiario"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    descripcion_norm = Column(String)

class Organo(Base):
    __tablename__ = "organo"
    id = Column(String, primary_key=True)
    id_padre = Column(String, ForeignKey("organo.id"), nullable=True)
    nombre = Column(String, nullable=False)
    tipo = Column(SQLEnum(TipoOrgano, name="tipo_organo_enum", native_enum=False), nullable=False)
    nivel1 = Column(String, nullable=True)
    nivel2 = Column(String, nullable=True)
    nivel3 = Column(String, nullable=True)
    nivel1_norm = Column(String, nullable=True)
    nivel2_norm = Column(String, nullable=True)
    nivel3_norm = Column(String, nullable=True)
    padre = relationship("Organo", remote_side=[id], backref="hijos")
    __table_args__ = (
        Index("ix_organo_nivel1_nivel2_nivel3", "nivel1_norm", "nivel2_norm", "nivel3_norm"),
    )


# ================= RELACIONES N:M =================

convocatoria_anuncio = Table(
    "convocatoria_anuncio", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("anuncio_id", Integer, ForeignKey("anuncio.num_anuncio"), primary_key=True),
)

convocatoria_documento = Table(
    "convocatoria_documento", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("documento_id", Integer, ForeignKey("documento.id"), primary_key=True),
)

convocatoria_finalidad = Table(
    "convocatoria_finalidad", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("finalidad_id", Integer, ForeignKey("finalidad.id"), primary_key=True),
)

convocatoria_fondo = Table(
    "convocatoria_fondo", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("fondo_id", Integer, ForeignKey("fondo.id"), primary_key=True),
)

convocatoria_instrumento = Table(
    "convocatoria_instrumento", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("instrumento_id", Integer, ForeignKey("instrumento.id"), primary_key=True),
)

convocatoria_objetivo = Table(
    "convocatoria_objetivo", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("objetivo_id", Integer, ForeignKey("objetivo.id"), primary_key=True),
)

convocatoria_region = Table(
    "convocatoria_region", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("region_id", Integer, ForeignKey("region.id"), primary_key=True),
)

convocatoria_sector_actividad = Table(
    "convocatoria_sector_actividad", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("sector_actividad_id", String, ForeignKey("sector_actividad.id"), primary_key=True),
)

convocatoria_sector_producto = Table(
    "convocatoria_sector_producto", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("sector_producto_id", Integer, ForeignKey("sector_producto.id"), primary_key=True),
)

convocatoria_tipo_beneficiario = Table(
    "convocatoria_tipo_beneficiario", Base.metadata,
    Column("convocatoria_id", Integer, ForeignKey("convocatoria.id"), primary_key=True),
    Column("tipo_beneficiario_id", Integer, ForeignKey("tipo_beneficiario.id"), primary_key=True),
)


# ================= CONVOCATORIA =================

class Convocatoria(Base):
    __tablename__ = "convocatoria"
    id = Column(Integer, primary_key=True)
    codigo_bdns = Column(String, nullable=False, index=True)
    descripcion = Column(Text)
    descripcion_leng = Column(Text)
    descripcion_finalidad = Column(Text)
    descripcion_bases = Column(Text)
    url_bases = Column(String)
    url_ayuda_estado = Column(String)
    ayuda_estado = Column(String)
    tipo_convocatoria = Column(String)
    sede_electronica = Column(String)
    abierto = Column(Boolean)
    se_publica_diario_oficial = Column(Boolean)
    presupuesto_total = Column(Float)
    mrr = Column(Boolean)
    fecha_recepcion = Column(Date)
    fecha_inicio_solicitud = Column(Date)
    fecha_fin_solicitud = Column(Date)

    organo_id = Column(String, ForeignKey("organo.id"))
    organo = relationship("Organo", backref="convocatorias")

    reglamento_id = Column(Integer, ForeignKey("reglamento.id"))
    reglamento = relationship("Reglamento")

    finalidad_id = Column(Integer, ForeignKey("finalidad.id"))
    finalidad = relationship("Finalidad")

    instrumentos = relationship("Instrumento", secondary="convocatoria_instrumento")
    tipos_beneficiarios = relationship("TipoBeneficiario", secondary="convocatoria_tipo_beneficiario")
    sectores_actividad = relationship("SectorActividad", secondary="convocatoria_sector_actividad")
    sectores_producto = relationship("SectorProducto", secondary="convocatoria_sector_producto")
    regiones = relationship("Region", secondary="convocatoria_region")
    finalidades = relationship("Finalidad", secondary="convocatoria_finalidad")
    objetivos = relationship("Objetivo", secondary="convocatoria_objetivo")
    documentos = relationship("Documento", secondary="convocatoria_documento", back_populates="convocatorias")
    anuncios = relationship("Anuncio", secondary="convocatoria_anuncio", back_populates="convocatorias")
    fondos = relationship("Fondo", secondary="convocatoria_fondo")

# ================= CONCESION =================

class Concesion(Base):
    __tablename__ = "concesion"
    id = Column(Integer, primary_key=True)
    id_convocatoria = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    numero_convocatoria = Column(String, index=True)
    descripcion_convocatoria = Column(String)
    descripcion_cooficial = Column(String)
    nivel1 = Column(String)
    nivel2 = Column(String)
    nivel3 = Column(String)
    codigo_invente = Column(String)
    fecha_concesion = Column(Date)
    id_persona = Column(Integer)
    beneficiario = Column(String)
    importe = Column(Float)
    ayuda_equivalente = Column(Float)
    url_br = Column(String)
    tiene_proyecto = Column(Boolean)

    instrumento_id = Column(Integer, ForeignKey("instrumento.id"))
    instrumento = relationship("Instrumento")

    convocatoria = relationship("Convocatoria", backref="concesiones")

# ================= MINIMIS =================

class Minimi(Base):
    __tablename__ = "minimi"
    id = Column(Integer, primary_key=True)
    codigo_concesion = Column(String, unique=True, index=True)
    numero_convocatoria = Column(String, index=True)
    id_convocatoria = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    id_persona = Column(Integer, index=True)
    beneficiario = Column(String)
    fecha_concesion = Column(Date, index=True)
    fecha_registro = Column(Date, index=True)
    ayuda_equivalente = Column(Float)

    instrumento_id = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")

    reglamento_id = Column(Integer, ForeignKey("reglamento.id"), nullable=True)
    reglamento = relationship("Reglamento")

    sector_actividad_id = Column(String, ForeignKey("sector_actividad.id"), nullable=True)
    sector_actividad = relationship("SectorActividad")

    sector_producto_id = Column(Integer, ForeignKey("sector_producto.id"), nullable=True)
    sector_producto = relationship("SectorProducto")

    convocatoria = relationship("Convocatoria", backref="minimis")

# ================= AYUDA ESTADO =================

class AyudaEstado(Base):
    __tablename__ = "ayuda_estado"
    id = Column(Integer, primary_key=True)
    id_concesion = Column(Integer, index=True, unique=True, nullable=False)
    numero_convocatoria = Column(String, index=True)
    id_convocatoria = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    id_persona = Column(Integer, index=True)
    codigo_concesion = Column(String, unique=True, index=True)
    beneficiario = Column(String)
    fecha_concesion = Column(Date, index=True)
    fecha_registro = Column(Date, index=True)
    ayuda_equivalente = Column(Float)
    ayuda_estado = Column(Integer, index=True)
    url_ayuda_estado = Column(String)
    entidad = Column(String)
    intermediario = Column(String)

    instrumento_id = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")

    reglamento_id = Column(Integer, ForeignKey("reglamento.id"), nullable=True)
    reglamento = relationship("Reglamento")

    sector_actividad_id = Column(String, ForeignKey("sector_actividad.id"), nullable=True)
    sector_actividad = relationship("SectorActividad")

    sector_producto_id = Column(Integer, ForeignKey("sector_producto.id"), nullable=True)
    sector_producto = relationship("SectorProducto")

    region_id = Column(Integer, ForeignKey("region.id"), nullable=True)
    region = relationship("Region")

    objetivo_id = Column(Integer, ForeignKey("objetivo.id"), nullable=True)
    objetivo = relationship("Objetivo")

    tipo_beneficiario_id = Column(Integer, ForeignKey("tipo_beneficiario.id"), nullable=True)
    tipo_beneficiario = relationship("TipoBeneficiario")

    convocatoria = relationship("Convocatoria")

