import strawberry
from typing import Optional, List
from datetime import date

@strawberry.type
class Organo:
    id: strawberry.ID
    nombre: str
    codigo: str

@strawberry.type
class Beneficiario:
    id: strawberry.ID
    identificador: str
    nombre: str
    tipo: str

@strawberry.type
class Convocatoria:
    id: strawberry.ID
    codigo_bdns: str
    titulo: str
    organo: Organo

@strawberry.type
class Concesion:
    id: strawberry.ID
    codigo_bdns: str
    convocatoria: Convocatoria
    organo: Organo
    beneficiario: Beneficiario
    fecha_concesion: date
    importe: float
    descripcion_proyecto: Optional[str]
    programa_presupuestario: Optional[str]
    tipo_ayuda: str
    año: int

@strawberry.input
class ConcesionInput:
    codigo_bdns: Optional[str] = None
    organo_id: Optional[strawberry.ID] = None
    beneficiario_id: Optional[strawberry.ID] = None
    tipo_beneficiario: Optional[str] = None
    fecha_desde: Optional[date] = None
    fecha_hasta: Optional[date] = None
    importe_minimo: Optional[float] = None
    importe_maximo: Optional[float] = None
    tipo_ayuda: Optional[str] = None
    año: Optional[int] = None
