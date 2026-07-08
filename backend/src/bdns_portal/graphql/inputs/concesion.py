from typing import Optional, List
from uuid import UUID
from datetime import date
import strawberry


@strawberry.input
class ConcesionFilterInput:
    search: Optional[str] = None
    ids: Optional[List[UUID]] = None
    id_concesion: Optional[str] = None
    
    convocatoria_id: Optional[UUID] = None
    beneficiario_id: Optional[UUID] = None
    regimen_ayuda_ids: Optional[List[UUID]] = None
    
    importe_min: Optional[int] = None
    importe_max: Optional[int] = None
    
    fecha_desde: Optional[date] = None
    fecha_hasta: Optional[date] = None
    anio: Optional[int] = None
    
    solo_ayudas_estado: Optional[bool] = None
    solo_minimis: Optional[bool] = None

    # Para sync: filtrar por tipo de régimen directamente
    regimen_tipo: Optional[str] = None  # "ordinaria", "minimis", "ayuda_estado"


@strawberry.input
class ConcesionSortInput:
    field: str
    direction: str = "desc"