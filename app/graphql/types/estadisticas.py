import strawberry
from typing import Optional, List
from datetime import date

@strawberry.type
class EstadisticasConcesiones:
    id: Optional[strawberry.ID] = None
    tipo_entidad: Optional[str] = None
    organo_id: Optional[strawberry.ID] = None
    organo_nombre: Optional[str] = None
    beneficiario_id: Optional[strawberry.ID] = None
    beneficiario_nombre: Optional[str] = None
    año: int
    numero_concesiones: int
    importe_total: float

@strawberry.input
class FiltroEstadisticas:
    año: Optional[int] = None
    año_desde: Optional[int] = None
    año_hasta: Optional[int] = None
    tipo_entidad: Optional[str] = None
    organo_id: Optional[strawberry.ID] = None
