from typing import Optional, List
from uuid import UUID
import strawberry
from sqlalchemy import select, and_, or_, func, desc, asc
from sqlalchemy.orm import selectinload, joinedload
import base64

from bdns_core.db.models import Concesion as ConcesionModel
from bdns_core.db.models import Beneficiario, Convocatoria, RegimenAyuda
from ..types.concesion import Concesion, ConcesionConnection, ConcesionEdge
from ..types.convocatoria import PageInfo
from ..inputs.concesion import ConcesionFilterInput, ConcesionSortInput
from ..inputs.convocatoria import PaginationInput


def cursor_to_offset(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    try:
        decoded = base64.b64decode(cursor).decode()
        return int(decoded.split(':')[1])
    except:
        return 0


def offset_to_cursor(offset: int, id: UUID) -> str:
    return base64.b64encode(f"concesion:{offset}:{id}".encode()).decode()


def build_filters(filters: Optional[ConcesionFilterInput]):
    conditions = []
    if not filters:
        return conditions
    
    if filters.search:
        term = f"%{filters.search}%"
        conditions.append(
            or_(
                ConcesionModel.id_concesion.ilike(term),
                ConcesionModel.convocatoria.has(Convocatoria.titulo.ilike(term)),
                ConcesionModel.beneficiario.has(Beneficiario.nombre.ilike(term))
            )
        )
    
    if filters.ids:
        conditions.append(ConcesionModel.id.in_(filters.ids))
    
    if filters.id_concesion:
        conditions.append(ConcesionModel.id_concesion == filters.id_concesion)
    
    if filters.convocatoria_id:
        conditions.append(ConcesionModel.convocatoria_id == filters.convocatoria_id)
    
    if filters.beneficiario_id:
        conditions.append(ConcesionModel.beneficiario_id == filters.beneficiario_id)
    
    if filters.regimen_ayuda_ids:
        conditions.append(ConcesionModel.regimen_ayuda_id.in_(filters.regimen_ayuda_ids))
    
    if filters.importe_min is not None or filters.importe_max is not None:
        if filters.solo_ayudas_estado:
            if filters.importe_min:
                conditions.append(ConcesionModel.importe_equivalente >= filters.importe_min)
            if filters.importe_max:
                conditions.append(ConcesionModel.importe_equivalente <= filters.importe_max)
        elif filters.solo_minimis:
            if filters.importe_min:
                conditions.append(ConcesionModel.importe_nominal >= filters.importe_min)
            if filters.importe_max:
                conditions.append(ConcesionModel.importe_nominal <= filters.importe_max)
        else:
            if filters.importe_min:
                conditions.append(
                    or_(
                        ConcesionModel.importe_nominal >= filters.importe_min,
                        ConcesionModel.importe_equivalente >= filters.importe_min
                    )
                )
            if filters.importe_max:
                conditions.append(
                    or_(
                        ConcesionModel.importe_nominal <= filters.importe_max,
                        ConcesionModel.importe_equivalente <= filters.importe_max
                    )
                )
    
    if filters.fecha_desde:
        conditions.append(ConcesionModel.fecha_concesion >= filters.fecha_desde)
    if filters.fecha_hasta:
        conditions.append(ConcesionModel.fecha_concesion <= filters.fecha_hasta)
    if filters.anio:
        conditions.append(
            func.extract('year', ConcesionModel.fecha_concesion) == filters.anio
        )
    
    if filters.solo_ayudas_estado:
        conditions.append(
            ConcesionModel.regimen_ayuda.has(RegimenAyuda.descripcion_norm == "ayuda_estado")
        )
    if filters.solo_minimis:
        conditions.append(
            ConcesionModel.regimen_ayuda.has(RegimenAyuda.descripcion_norm == "minimis")
        )

    # Filtro directo por tipo de régimen (para sync)
    if filters.regimen_tipo:
        conditions.append(
            ConcesionModel.regimen_ayuda.has(RegimenAyuda.descripcion_norm == filters.regimen_tipo)
        )

    return conditions


async def get_concesiones(
    info,
    pagination: Optional[PaginationInput] = None,
    where: Optional[ConcesionFilterInput] = None,
    order_by: Optional[List[ConcesionSortInput]] = None
) -> ConcesionConnection:
    db = info.context["db"]
    
    query = select(ConcesionModel).options(
        joinedload(ConcesionModel.beneficiario),
        joinedload(ConcesionModel.convocatoria),
        joinedload(ConcesionModel.regimen_ayuda)
    )
    
    conditions = build_filters(where)
    if conditions:
        query = query.where(and_(*conditions))
    
    count_query = select(func.count()).select_from(ConcesionModel)
    if conditions:
        count_query = count_query.where(and_(*conditions))
    total_count = await db.scalar(count_query)
    
    limit = None
    offset = 0
    
    if pagination:
        if pagination.first:
            limit = pagination.first
            if pagination.after:
                offset = cursor_to_offset(pagination.after) + 1
        elif pagination.last:
            limit = pagination.last
            if pagination.before:
                offset = max(0, cursor_to_offset(pagination.before) - pagination.last)
        elif pagination.limit:
            limit = pagination.limit
            offset = pagination.offset or 0
    
    if order_by:
        for s in order_by:
            if s.field == "importe":
                if where and where.solo_ayudas_estado:
                    col = ConcesionModel.importe_equivalente
                elif where and where.solo_minimis:
                    col = ConcesionModel.importe_nominal
                else:
                    col = func.coalesce(ConcesionModel.importe_nominal, ConcesionModel.importe_equivalente, 0)
            else:
                col = getattr(ConcesionModel, s.field, None)
            
            if col is not None:
                query = query.order_by(asc(col) if s.direction == "asc" else desc(col))
    else:
        query = query.order_by(desc(ConcesionModel.fecha_concesion))
    
    if limit:
        query = query.limit(limit + 1)
    if offset:
        query = query.offset(offset)
    
    result = await db.execute(query)
    concesiones = result.unique().scalars().all()
    
    has_next = False
    if limit and len(concesiones) > limit:
        has_next = True
        concesiones = concesiones[:limit]
    
    edges = []
    for idx, c in enumerate(concesiones):
        cursor = offset_to_cursor(offset + idx, c.id)
        edges.append(ConcesionEdge(cursor=cursor, node=c))
    
    page_info = PageInfo(
        has_next_page=has_next,
        has_previous_page=offset > 0,
        start_cursor=edges[0].cursor if edges else None,
        end_cursor=edges[-1].cursor if edges else None,
        total_count=total_count
    )
    
    return ConcesionConnection(edges=edges, page_info=page_info, total_count=total_count)


async def get_concesion_by_id(info, id: UUID) -> Optional[Concesion]:
    db = info.context["db"]
    query = select(ConcesionModel).where(ConcesionModel.id == id).options(
        joinedload(ConcesionModel.beneficiario),
        joinedload(ConcesionModel.convocatoria),
        joinedload(ConcesionModel.regimen_ayuda)
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def get_concesiones_por_beneficiario(
    info,
    beneficiario_id: UUID,
    anio: Optional[int] = None,
    pagination: Optional[PaginationInput] = None
) -> ConcesionConnection:
    filters = ConcesionFilterInput(beneficiario_id=beneficiario_id, anio=anio)
    return await get_concesiones(info, pagination, filters)


async def get_concesiones_por_convocatoria(
    info,
    convocatoria_id: UUID,
    pagination: Optional[PaginationInput] = None
) -> ConcesionConnection:
    filters = ConcesionFilterInput(convocatoria_id=convocatoria_id)
    return await get_concesiones(info, pagination, filters)


async def get_concesiones_para_sync(
    info,
    convocatoria_id: UUID,
    regimen_tipo: Optional[str] = None
) -> List[Concesion]:
    """
    Query optimizado para sincronización incremental.

    - Sin paginación (devuelve todas las concesiones)
    - Eager loading de relaciones (convocatoria, beneficiario, regimen)
    - Filtrado por tipo de régimen opcional
    - Ordenado por id_concesion para determinismo

    Args:
        info: Context de Strawberry
        convocatoria_id: UUID de convocatoria
        regimen_tipo: Filtro opcional por tipo (ordinaria, minimis, ayuda_estado)

    Returns:
        Lista de concesiones con todas las relaciones cargadas
    """
    db = info.context["db"]

    # Query con eager loading para evitar N+1
    query = select(ConcesionModel).options(
        joinedload(ConcesionModel.beneficiario),
        joinedload(ConcesionModel.convocatoria),
        joinedload(ConcesionModel.regimen_ayuda)
    )

    # Filtros
    conditions = [ConcesionModel.convocatoria_id == convocatoria_id]

    if regimen_tipo:
        conditions.append(
            ConcesionModel.regimen_ayuda.has(RegimenAyuda.descripcion_norm == regimen_tipo)
        )

    query = query.where(and_(*conditions))

    # Ordenar por id_concesion para determinismo (importante para sync)
    query = query.order_by(ConcesionModel.id_concesion)

    result = await db.execute(query)
    concesiones = result.unique().scalars().all()

    return list(concesiones)