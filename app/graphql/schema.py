import strawberry
from typing import List, Optional
from datetime import date
from app.graphql.types.concesion import Concesion, ConcesionInput
from app.graphql.types.beneficiario import Beneficiario, BeneficiarioInput
from app.graphql.types.estadisticas import EstadisticasConcesiones, FiltroEstadisticas
from app.graphql.resolvers.concesiones import (
    get_concesiones,
    get_concesion_by_id,
    get_concesiones_por_beneficiario,
)
from app.graphql.resolvers.beneficiarios import (
    get_beneficiarios,
    get_beneficiario_by_id,
)
from app.graphql.resolvers.estadisticas import (
    get_estadisticas_por_tipo_entidad,
    get_estadisticas_por_organo,
    get_concentracion_subvenciones,
)

@strawberry.type
class Query:
    @strawberry.field
    async def concesion(self, id: strawberry.ID) -> Optional[Concesion]:
        return await get_concesion_by_id(id)
    
    @strawberry.field
    async def concesiones(
        self, 
        filtros: Optional[ConcesionInput] = None, 
        limite: int = 100, 
        offset: int = 0
    ) -> List[Concesion]:
        return await get_concesiones(filtros, limite, offset)
    
    @strawberry.field
    async def beneficiario(self, id: strawberry.ID) -> Optional[Beneficiario]:
        return await get_beneficiario_by_id(id)
    
    @strawberry.field
    async def beneficiarios(
        self, 
        filtros: Optional[BeneficiarioInput] = None, 
        limite: int = 100, 
        offset: int = 0
    ) -> List[Beneficiario]:
        return await get_beneficiarios(filtros, limite, offset)
    
    @strawberry.field
    async def concesiones_por_beneficiario(
        self, 
        beneficiario_id: strawberry.ID, 
        año: Optional[int] = None,
        limite: int = 100, 
        offset: int = 0
    ) -> List[Concesion]:
        return await get_concesiones_por_beneficiario(beneficiario_id, año, limite, offset)
    
    @strawberry.field
    async def estadisticas_por_tipo_entidad(
        self, 
        filtros: Optional[FiltroEstadisticas] = None
    ) -> List[EstadisticasConcesiones]:
        return await get_estadisticas_por_tipo_entidad(filtros)
    
    @strawberry.field
    async def estadisticas_por_organo(
        self, 
        filtros: Optional[FiltroEstadisticas] = None
    ) -> List[EstadisticasConcesiones]:
        return await get_estadisticas_por_organo(filtros)
    
    @strawberry.field
    async def concentracion_subvenciones(
        self, 
        año: Optional[int] = None,
        tipo_entidad: Optional[str] = None,
        limite: int = 10
    ) -> List[EstadisticasConcesiones]:
        return await get_concentracion_subvenciones(año, tipo_entidad, limite)

# Crear esquema
schema = strawberry.Schema(query=Query)
