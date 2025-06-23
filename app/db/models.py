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
)
from sqlalchemy.orm import relationship, foreign, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from db.enums import TipoOrgano

Base = declarative_base()

# ================= CATÁLOGOS =================

class Actividad(Base):
    __tablename__ = 'actividad'
    id = Column(Integer, primary_key=True)
    descripcion = Column(String, unique=True)

class Instrumento(Base):
    __tablename__ = "instrumento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String, unique=True)

class TipoBeneficiario(Base):
    __tablename__ = "tipo_beneficiario"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)

class Sector(Base):
    __tablename__ = "sector"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)

class Region(Base):
    __tablename__ = "region"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)

class Finalidad(Base):
    __tablename__ = "finalidad"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)

class Objetivo(Base):
    __tablename__ = "objetivo"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String)

class Reglamento(Base):
    __tablename__ = "reglamento"
    id = Column(Integer, primary_key=True)
    descripcion = Column(String, unique=True, nullable=False, index=True)
    autorizacion = Column(Integer, nullable=True)
   
class Organo(Base):
    __tablename__ = "organo"

    id       = Column(String, primary_key=True)
    id_padre = Column(String, ForeignKey("organo.id"), nullable=True)
    nombre   = Column(String, nullable=False)
    tipo     = Column(SQLEnum(TipoOrgano, name="tipo_organo_enum", native_enum=False), nullable=False)
    nivel1   = Column(String, nullable=True)
    nivel2   = Column(String, nullable=True)
    nivel3   = Column(String, nullable=True)
    nivel1_norm   = Column(String, nullable=True)
    nivel2_norm   = Column(String, nullable=True)
    nivel3_norm   = Column(String, nullable=True)

    padre = relationship("Organo", remote_side=[id], backref="hijos")

    __table_args__ = (
        Index("ix_organo_nivel1_nivel2_nivel3", "nivel1_norm", "nivel2_norm", "nivel3_norm"),
    )
 

##################################################################
# CONVOCATORIAS
##################################################################

class Convocatoria(Base):
    __tablename__ = "convocatoria"

    id = Column(Integer, primary_key=True)
    codigo_bdns = Column(String, nullable=False, index=True)
    descripcion = Column(Text, nullable=False)
    descripcion_leng = Column(Text)
    descripcion_finalidad = Column(Text)
    descripcion_bases = Column(Text)
    url_bases = Column(String)
    url_ayuda_estado = Column(String)
    ayuda_estado = Column(String)
    tipo_convocatoria = Column(String)
    sede_electronica = Column(String)
    abierto = Column(Boolean, default=False)
    se_publica_diario_oficial = Column(Boolean, default=False)
    presupuesto_total = Column(Integer)
    mrr = Column(Boolean, default=False)
    fecha_recepcion = Column(Date)
    fecha_inicio_solicitud = Column(Date)
    fecha_fin_solicitud = Column(Date)

    organo_id= Column(String, ForeignKey("organo.id"))
    organo = relationship("Organo", backref="convocatorias")

    reglamento_id = Column(Integer, ForeignKey("reglamento.id"))
    reglamento = relationship("Reglamento")

    finalidad_id = Column(Integer, ForeignKey("finalidad.id"))
    finalidad = relationship("Finalidad")

    instrumentos = relationship("Instrumento", secondary="convocatoria_instrumento")
    tipos_beneficiarios = relationship("TipoBeneficiario", secondary="convocatoria_tipo_beneficiario")
    sectores = relationship("Sector", secondary="convocatoria_sector")
    regiones = relationship("Region", secondary="convocatoria_region")
    finalidades = relationship("Finalidad", secondary="convocatoria_finalidad")
    objetivos = relationship("Objetivo", secondary="convocatoria_objetivo")
    documentos = relationship("Documento", backref="convocatoria")
    anuncios = relationship("Anuncio", backref="convocatoria")

# Relaciones N:M para convocatoria
class ConvocatoriaInstrumento(Base):
    __tablename__ = "convocatoria_instrumento"
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"), primary_key=True)
    instrumento_id = Column(Integer, ForeignKey("instrumento.id"), primary_key=True)

class ConvocatoriaTipoBeneficiario(Base):
    __tablename__ = "convocatoria_tipo_beneficiario"
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"), primary_key=True)
    tipo_beneficiario_id = Column(Integer, ForeignKey("tipo_beneficiario.id"), primary_key=True)

class ConvocatoriaSector(Base):
    __tablename__ = "convocatoria_sector"
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"), primary_key=True)
    sector_id = Column(Integer, ForeignKey("sector.id"), primary_key=True)

class ConvocatoriaRegion(Base):
    __tablename__ = "convocatoria_region"
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"), primary_key=True)
    region_id = Column(Integer, ForeignKey("region.id"), primary_key=True)

class ConvocatoriaFinalidad(Base):
    __tablename__ = "convocatoria_finalidad"
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"), primary_key=True)
    finalidad_id = Column(Integer, ForeignKey("finalidad.id"), primary_key=True)

class ConvocatoriaObjetivo(Base):
    __tablename__ = "convocatoria_objetivo"
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"), primary_key=True)
    objetivo_id = Column(Integer, ForeignKey("objetivo.id"), primary_key=True)

class Documento(Base):
    __tablename__ = "documento"
    id = Column(Integer, primary_key=True)
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"))
    nombre_fic = Column(String)
    descripcion = Column(String)
    longitud = Column(Integer)
    fecha_mod = Column(Date)
    fecha_publicacion = Column(Date)

class Anuncio(Base):
    __tablename__ = "anuncio"
    num_anuncio = Column(Integer, primary_key=True)
    convocatoria_id = Column(Integer, ForeignKey("convocatoria.id"))
    titulo = Column(Text)
    texto = Column(Text)
    url = Column(String)
    diario_oficial = Column(String)
    fecha_publicacion = Column(Date)

# ================= CONCESIÓN Y MINIMIS =================

class Concesion(Base):
    __tablename__ = "concesion"
    id = Column(Integer, primary_key=True)
    id_convocatoria = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    convocatoria = relationship("Convocatoria", backref="concesiones")
    numero_convocatoria = Column(String, index=True)
    descripcion_convocatoria = Column(String)
    descripcion_cooficial = Column(String, nullable=True)
    nivel1 = Column(String, index=True)
    nivel2 = Column(String, index=True)
    nivel3 = Column(String, index=True)
    codigo_invente = Column(String, index=True)
    fecha_concesion = Column(Date, index=True)
    id_persona = Column(Integer, nullable=True, index=True)
    beneficiario = Column(String)
    instrumento_id = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")
    importe = Column(Float)
    ayuda_equivalente = Column(Float)
    url_br = Column(String, nullable=True)
    tiene_proyecto = Column(Boolean, default=False)

class Minimi(Base):
    __tablename__ = "minimi"
    id = Column(Integer, primary_key=True)
    codigo_concesion = Column(String, unique=True, index=True)
    id_convocatoria = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    convocatoria = relationship("Convocatoria", backref="minimis")
    numero_convocatoria = Column(String, index=True)
    id_persona = Column(Integer, index=True)
    beneficiario = Column(String)
    fecha_concesion = Column(Date, index=True)
    fecha_registro = Column(Date, index=True)
    ayuda_equivalente = Column(Float)
    instrumento_id = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")
    reglamento_id = Column(Integer, ForeignKey("reglamento.id"), nullable=True)
    reglamento = relationship("Reglamento")
    sector_id = Column(Integer, ForeignKey("sector.id"), nullable=True)
    sector = relationship("Sector")
    actividad_id = Column(Integer, ForeignKey("actividad.id"), nullable=True)
    actividad = relationship("Actividad")

# ================= AYUDA ESTADO =================

class AyudaEstado(Base):
    __tablename__ = "ayuda_estado"
    id = Column(Integer, primary_key=True)
    id_concesion = Column(Integer, index=True, unique=True, nullable=False)
    id_convocatoria = Column(Integer, ForeignKey("convocatoria.id"), nullable=False, index=True)
    convocatoria = relationship("Convocatoria")
    id_persona = Column(Integer, index=True, nullable=True)
    numero_convocatoria = Column(String, index=True)
    reglamento_id = Column(Integer, ForeignKey("reglamento.id"), nullable=True)
    reglamento = relationship("Reglamento")
    objetivo = Column(String, nullable=True)
    instrumento_id = Column(Integer, ForeignKey("instrumento.id"), nullable=True)
    instrumento = relationship("Instrumento")
    tipo_beneficiario_id = Column(Integer, ForeignKey("tipo_beneficiario.id"), nullable=True)
    tipo_beneficiario = relationship("TipoBeneficiario")
    fecha_concesion = Column(Date, nullable=True)
    fecha_alta = Column(Date, nullable=True)
    beneficiario = Column(String, nullable=True)
    importe = Column(Float, nullable=True)
    ayuda_equivalente = Column(Float, nullable=True)
    region = Column(String, nullable=True)
    sectores = Column(String, nullable=True)
    ayuda_estado = Column(String, nullable=True, index=True)
    url_ayuda_estado = Column(String, nullable=True)
    entidad = Column(String, nullable=True)
    intermediario = Column(String, nullable=True)
