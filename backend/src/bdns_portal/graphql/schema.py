# graphql/schema.py
from typing import Optional, List
from uuid import UUID
import strawberry

# Types existentes
from .types.convocatoria import (
    Convocatoria, ConvocatoriaConnection, 
    DocumentoConvocatoria, AnuncioConvocatoria, PageInfo
)
from .types.beneficiario import (
    Beneficiario, BeneficiarioConnection, Pseudonimo
)
from .types.concesion import (
    Concesion, ConcesionConnection
)
from .types.catalogos import (
    Finalidad, Fondo, FormaJuridica, Instrumento,
    Objetivo, Organo, Reglamento, Region, RegimenAyuda,
    SectorActividad, SectorProducto, TipoBeneficiario
)
from .types.estadisticas import (
    EstadisticasConcesiones,
    EvolucionMensual,
    EstadisticasRegimen,
    EstadisticasRegion,
    TopConvocatoria,
    ComparativaAnual,
    FiltroEstadisticas
)

# Inputs
from .inputs.convocatoria import (
    ConvocatoriaFilterInput, ConvocatoriaSortInput, PaginationInput
)
from .inputs.beneficiario import (
    BeneficiarioFilterInput, BeneficiarioSortInput
)
from .inputs.concesion import (
    ConcesionFilterInput, ConcesionSortInput
)
from .inputs.catalogos import (
    CatalogoFilterInput
)

# Resolvers
from .resolvers import convocatoria as conv_resolvers
from .resolvers import beneficiario as ben_resolvers
from .resolvers import concesion as conc_resolvers
from .resolvers import catalogos as cat_resolvers

# Resolvers de estadísticas
from .resolvers.estadisticas import (
    get_estadisticas_por_tipo_entidad,
    get_estadisticas_por_organo,
    get_concentracion_subvenciones,
    get_estadisticas_evolucion_mensual,
    get_estadisticas_por_regimen,
    get_estadisticas_por_region,
    get_top_convocatorias,
    get_beneficiarios_recurrentes,
    get_comparativa_anual
)


@strawberry.type
class Query:
    # ============ CONVOCATORIAS ============
    @strawberry.field
    async def convocatorias(
        self,
        info: strawberry.Info,
        pagination: Optional[PaginationInput] = None,
        where: Optional[ConvocatoriaFilterInput] = None,
        order_by: Optional[List[ConvocatoriaSortInput]] = None
    ) -> ConvocatoriaConnection:
        return await conv_resolvers.get_convocatorias(info, pagination, where, order_by)
    
    @strawberry.field
    async def convocatoria(
        self,
        info: strawberry.Info,
        id: UUID
    ) -> Optional[Convocatoria]:
        return await conv_resolvers.get_convocatoria_by_id(info, id)
    
    @strawberry.field
    async def buscar_convocatorias(
        self,
        info: strawberry.Info,
        q: str,
        limit: int = 10
    ) -> List[Convocatoria]:
        return await conv_resolvers.buscar_convocatorias(info, q, limit)
    
    # ============ BENEFICIARIOS ============
    @strawberry.field
    async def beneficiarios(
        self,
        info: strawberry.Info,
        pagination: Optional[PaginationInput] = None,
        where: Optional[BeneficiarioFilterInput] = None,
        order_by: Optional[List[BeneficiarioSortInput]] = None
    ) -> BeneficiarioConnection:
        return await ben_resolvers.get_beneficiarios(info, pagination, where, order_by)
    
    @strawberry.field
    async def beneficiario(
        self,
        info: strawberry.Info,
        id: UUID
    ) -> Optional[Beneficiario]:
        return await ben_resolvers.get_beneficiario_by_id(info, id)
    
    @strawberry.field
    async def buscar_beneficiarios(
        self,
        info: strawberry.Info,
        q: str,
        limit: int = 10
    ) -> List[Beneficiario]:
        return await ben_resolvers.buscar_beneficiarios(info, q, limit)
    
    # ============ CONCESIONES ============
    @strawberry.field
    async def concesiones(
        self,
        info: strawberry.Info,
        pagination: Optional[PaginationInput] = None,
        where: Optional[ConcesionFilterInput] = None,
        order_by: Optional[List[ConcesionSortInput]] = None
    ) -> ConcesionConnection:
        return await conc_resolvers.get_concesiones(info, pagination, where, order_by)
    
    @strawberry.field
    async def concesion(
        self,
        info: strawberry.Info,
        id: UUID
    ) -> Optional[Concesion]:
        return await conc_resolvers.get_concesion_by_id(info, id)
    
    @strawberry.field
    async def concesiones_por_beneficiario(
        self,
        info: strawberry.Info,
        beneficiario_id: UUID,
        anio: Optional[int] = None,
        pagination: Optional[PaginationInput] = None
    ) -> ConcesionConnection:
        return await conc_resolvers.get_concesiones_por_beneficiario(
            info, beneficiario_id, anio, pagination
        )
    
    @strawberry.field
    async def concesiones_por_convocatoria(
        self,
        info: strawberry.Info,
        convocatoria_id: UUID,
        pagination: Optional[PaginationInput] = None
    ) -> ConcesionConnection:
        return await conc_resolvers.get_concesiones_por_convocatoria(
            info, convocatoria_id, pagination
        )

    @strawberry.field
    async def concesiones_para_sync(
        self,
        info: strawberry.Info,
        convocatoria_id: UUID,
        regimen_tipo: Optional[str] = None
    ) -> List[Concesion]:
        """
        Query optimizado para sincronización incremental.
        Devuelve todas las concesiones sin paginación, con eager loading.
        """
        return await conc_resolvers.get_concesiones_para_sync(
            info, convocatoria_id, regimen_tipo
        )

    # ============ CATÁLOGOS ============
    @strawberry.field
    async def finalidades(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None
    ) -> List[Finalidad]:
        return await cat_resolvers.get_finalidades(info, where)
    
    @strawberry.field
    async def fondos(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None
    ) -> List[Fondo]:
        return await cat_resolvers.get_fondos(info, where)
    
    @strawberry.field
    async def formas_juridicas(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None
    ) -> List[FormaJuridica]:
        return await cat_resolvers.get_formas_juridicas(info, where)
    
    @strawberry.field
    async def instrumentos(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None
    ) -> List[Instrumento]:
        return await cat_resolvers.get_instrumentos(info, where)
    
    @strawberry.field
    async def objetivos(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None
    ) -> List[Objetivo]:
        return await cat_resolvers.get_objetivos(info, where)
    
    @strawberry.field
    async def organos(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None,
        incluir_hijos: bool = False
    ) -> List[Organo]:
        return await cat_resolvers.get_organos(info, where, incluir_hijos)
    
    @strawberry.field
    async def regiones(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None,
        incluir_hijos: bool = False
    ) -> List[Region]:
        return await cat_resolvers.get_regiones(info, where, incluir_hijos)
    
    @strawberry.field
    async def sectores_actividad(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None,
        incluir_hijos: bool = False
    ) -> List[SectorActividad]:
        return await cat_resolvers.get_sectores_actividad(info, where, incluir_hijos)
    
    @strawberry.field
    async def tipos_beneficiario(
        self,
        info: strawberry.Info,
        where: Optional[CatalogoFilterInput] = None
    ) -> List[TipoBeneficiario]:
        return await cat_resolvers.get_tipos_beneficiario(info, where)
    
    # ============ ESTADÍSTICAS ============
    @strawberry.field
    async def estadisticas_por_tipo_entidad(
        self,
        info: strawberry.Info,
        filtros: Optional[FiltroEstadisticas] = None
    ) -> List[EstadisticasConcesiones]:
        return await get_estadisticas_por_tipo_entidad(info, filtros)
    
    @strawberry.field
    async def estadisticas_por_organo(
        self,
        info: strawberry.Info,
        filtros: Optional[FiltroEstadisticas] = None
    ) -> List[EstadisticasConcesiones]:
        return await get_estadisticas_por_organo(info, filtros)
    
    @strawberry.field
    async def concentracion_subvenciones(
        self,
        info: strawberry.Info,
        anio: Optional[int] = None,
        tipo_entidad: Optional[str] = None,
        limite: int = 10
    ) -> List[EstadisticasConcesiones]:
        return await get_concentracion_subvenciones(info, anio, tipo_entidad, limite)
    
    @strawberry.field
    async def estadisticas_evolucion_mensual(
        self,
        info: strawberry.Info,
        anio: int
    ) -> List[EvolucionMensual]:
        return await get_estadisticas_evolucion_mensual(info, anio)
    
    @strawberry.field
    async def estadisticas_por_regimen(
        self,
        info: strawberry.Info,
        anio: Optional[int] = None
    ) -> List[EstadisticasRegimen]:
        return await get_estadisticas_por_regimen(info, anio)
    
    @strawberry.field
    async def estadisticas_por_region(
        self,
        info: strawberry.Info,
        anio: Optional[int] = None,
        limite: int = 20
    ) -> List[EstadisticasRegion]:
        return await get_estadisticas_por_region(info, anio, limite)
    
    @strawberry.field
    async def top_convocatorias(
        self,
        info: strawberry.Info,
        anio: Optional[int] = None,
        limite: int = 10
    ) -> List[TopConvocatoria]:
        return await get_top_convocatorias(info, anio, limite)
    
    @strawberry.field
    async def beneficiarios_recurrentes(
        self,
        info: strawberry.Info,
        anio: Optional[int] = None,
        minimo_concesiones: int = 3,
        limite: int = 20
    ) -> List[EstadisticasConcesiones]:
        return await get_beneficiarios_recurrentes(info, anio, minimo_concesiones, limite)
    
    @strawberry.field
    async def comparativa_anual(
        self,
        info: strawberry.Info,
        anio_base: int,
        anio_comparar: int
    ) -> ComparativaAnual:
        return await get_comparativa_anual(info, anio_base, anio_comparar)


schema = strawberry.Schema(query=Query)