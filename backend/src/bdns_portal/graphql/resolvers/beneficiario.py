from typing import Optional, List
from uuid import UUID
import strawberry
from sqlalchemy import select, or_, and_, func, desc, asc
from sqlalchemy.orm import selectinload, joinedload
import base64

from bdns_core.db.models import Beneficiario as BeneficiarioModel, Pseudonimo as PseudonimoModel
from ..types.beneficiario import Beneficiario, BeneficiarioConnection, BeneficiarioEdge, PageInfo
from ..inputs.beneficiario import BeneficiarioFilterInput, BeneficiarioSortInput
from ..inputs.convocatoria import PaginationInput
from .convocatoria import cursor_to_offset, offset_to_cursor


def build_filters(filters: Optional[BeneficiarioFilterInput]):
    conditions = []
    if not filters:
        return conditions
    
    if filters.search:
        term = f"%{filters.search}%"
        conditions.append(
            or_(
                BeneficiarioModel.nombre.ilike(term),
                BeneficiarioModel.nif.ilike(term)
            )
        )
    
    if filters.ids:
        conditions.append(BeneficiarioModel.id.in_(filters.ids))
    
    if filters.nif:
        conditions.append(BeneficiarioModel.nif == filters.nif)
    
    if filters.nombre_contains:
        conditions.append(BeneficiarioModel.nombre.ilike(f"%{filters.nombre_contains}%"))
    
    if filters.forma_juridica_ids:
        conditions.append(BeneficiarioModel.forma_juridica_id.in_(filters.forma_juridica_ids))
    
    if filters.tipo_beneficiario_ids:
        conditions.append(BeneficiarioModel.tipo_beneficiario_id.in_(filters.tipo_beneficiario_ids))
    
    return conditions


async def get_beneficiarios(
    info,
    pagination: Optional[PaginationInput] = None,
    filters: Optional[BeneficiarioFilterInput] = None,
    sort: Optional[List[BeneficiarioSortInput]] = None
) -> BeneficiarioConnection:
    db = info.context["db"]
    
    query = select(BeneficiarioModel).options(
        joinedload(BeneficiarioModel.forma_juridica),
        joinedload(BeneficiarioModel.tipo_beneficiario),
        selectinload(BeneficiarioModel.pseudonimos)
    )
    
    conditions = build_filters(filters)
    if conditions:
        query = query.where(and_(*conditions))
    
    count_query = select(func.count()).select_from(BeneficiarioModel)
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
    
    if sort:
        for s in sort:
            col = getattr(BeneficiarioModel, s.field, None)
            if col:
                query = query.order_by(asc(col) if s.direction == "asc" else desc(col))
    else:
        query = query.order_by(BeneficiarioModel.nombre)
    
    if limit:
        query = query.limit(limit + 1)
    if offset:
        query = query.offset(offset)
    
    result = await db.execute(query)
    beneficiarios = result.unique().scalars().all()
    
    has_next = False
    if limit and len(beneficiarios) > limit:
        has_next = True
        beneficiarios = beneficiarios[:limit]
    
    edges = []
    for idx, b in enumerate(beneficiarios):
        cursor = offset_to_cursor(offset + idx, b.id)
        edges.append(BeneficiarioEdge(cursor=cursor, node=b))
    
    page_info = PageInfo(
        has_next_page=has_next,
        has_previous_page=offset > 0,
        start_cursor=edges[0].cursor if edges else None,
        end_cursor=edges[-1].cursor if edges else None,
        total_count=total_count
    )
    
    return BeneficiarioConnection(edges=edges, page_info=page_info, total_count=total_count)


async def get_beneficiario_by_id(info, id: UUID) -> Optional[Beneficiario]:
    db = info.context["db"]
    query = select(BeneficiarioModel).where(BeneficiarioModel.id == id).options(
        joinedload(BeneficiarioModel.forma_juridica),
        joinedload(BeneficiarioModel.tipo_beneficiario),
        selectinload(BeneficiarioModel.pseudonimos)
    )
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def buscar_beneficiarios(info, query: str, limit: int = 10) -> List[Beneficiario]:
    db = info.context["db"]
    term = f"%{query}%"
    stmt = select(BeneficiarioModel).where(
        or_(
            BeneficiarioModel.nombre.ilike(term),
            BeneficiarioModel.nif.ilike(term)
        )
    ).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all())