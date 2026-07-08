from typing import Optional, List
from uuid import UUID
from datetime import date, datetime
import strawberry

from .beneficiario import Beneficiario
from .convocatoria import Convocatoria, PageInfo
from .catalogos import RegimenAyuda


@strawberry.type
class Concesion:
    id: UUID
    id_concesion: str
    fecha_concesion: date
    importe_equivalente: Optional[int]
    importe_nominal: Optional[int]
    created_at: datetime
    updated_at: Optional[datetime]
    created_by: Optional[str]
    updated_by: Optional[str]

    beneficiario_id: UUID
    convocatoria_id: UUID
    regimen_ayuda_id: Optional[UUID]

    beneficiario: Optional[Beneficiario]
    convocatoria: Optional[Convocatoria]
    regimen_ayuda: Optional[RegimenAyuda]

    @strawberry.field
    def codigo_bdns(self) -> Optional[str]:
        """Código BDNS de la convocatoria (necesario para hashing)."""
        if self.convocatoria:
            return self.convocatoria.codigo_bdns
        return None

    @strawberry.field
    def regimen_tipo(self) -> Optional[str]:
        """Tipo de régimen normalizado (ordinaria/minimis/ayuda_estado)."""
        if self.regimen_ayuda:
            return self.regimen_ayuda.descripcion_norm
        return None
    
    @strawberry.field
    def importe(self) -> int:
        if self.es_ayuda_estado:
            return self.importe_equivalente or 0
        return self.importe_nominal or 0
    
    @strawberry.field
    def importe_formateado(self) -> str:
        return f"{self.importe:,}€"
    
    @strawberry.field
    def es_ayuda_estado(self) -> bool:
        if not self.regimen_ayuda:
            return False
        return self.regimen_ayuda.descripcion_norm == "ayuda_estado"
    
    @strawberry.field
    def es_minimis(self) -> bool:
        if not self.regimen_ayuda:
            return False
        return self.regimen_ayuda.descripcion_norm == "minimis"
    
    @strawberry.field
    def organo_concedente_id(self) -> Optional[UUID]:
        if self.convocatoria:
            return self.convocatoria.organo_id
        return None


@strawberry.type
class ConcesionEdge:
    cursor: str
    node: Concesion


@strawberry.type
class ConcesionConnection:
    edges: List[ConcesionEdge]
    page_info: 'PageInfo'
    total_count: int


