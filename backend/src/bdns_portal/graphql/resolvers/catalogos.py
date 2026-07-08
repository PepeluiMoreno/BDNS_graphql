from typing import Optional, List
from uuid import UUID
import strawberry
from sqlalchemy import select, or_, and_
from sqlalchemy.orm import selectinload

from bdns_core.db.models import (
    Finalidad as FinalidadModel,
    Fondo as FondoModel,
    FormaJuridica as FormaJuridicaModel,
    Instrumento as InstrumentoModel,
    Objetivo as ObjetivoModel,
    Organo as OrganoModel,
    Region as RegionModel,
    SectorActividad as SectorActividadModel,
    TipoBeneficiario as TipoBeneficiarioModel
)
from ..types.catalogos import (
    Finalidad, Fondo, FormaJuridica, Instrumento,
    Objetivo, Organo, Region, SectorActividad, TipoBeneficiario
)
from ..inputs.catalogos import CatalogoFilterInput


async def get_finalidades(info, filters: Optional[CatalogoFilterInput] = None) -> List[Finalidad]:
    db = info.context["db"]
    query = select(FinalidadModel)
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(FinalidadModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(FinalidadModel.id.in_(filters.ids))
        if filters.api_ids:
            query = query.where(FinalidadModel.api_id.in_(filters.api_ids))
    
    result = await db.execute(query.order_by(FinalidadModel.descripcion))
    return list(result.scalars().all())


async def get_fondos(info, filters: Optional[CatalogoFilterInput] = None) -> List[Fondo]:
    db = info.context["db"]
    query = select(FondoModel)
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(FondoModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(FondoModel.id.in_(filters.ids))
    
    result = await db.execute(query.order_by(FondoModel.descripcion))
    return list(result.scalars().all())


async def get_formas_juridicas(info, filters: Optional[CatalogoFilterInput] = None) -> List[FormaJuridica]:
    db = info.context["db"]
    query = select(FormaJuridicaModel)
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(FormaJuridicaModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(FormaJuridicaModel.id.in_(filters.ids))
        if filters.codigo_contains:
            query = query.where(FormaJuridicaModel.codigo.ilike(f"%{filters.codigo_contains}%"))
    
    result = await db.execute(query.order_by(FormaJuridicaModel.descripcion))
    return list(result.scalars().all())


async def get_instrumentos(info, filters: Optional[CatalogoFilterInput] = None) -> List[Instrumento]:
    db = info.context["db"]
    query = select(InstrumentoModel)
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(InstrumentoModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(InstrumentoModel.id.in_(filters.ids))
    
    result = await db.execute(query.order_by(InstrumentoModel.descripcion))
    return list(result.scalars().all())


async def get_objetivos(info, filters: Optional[CatalogoFilterInput] = None) -> List[Objetivo]:
    db = info.context["db"]
    query = select(ObjetivoModel)
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(ObjetivoModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(ObjetivoModel.id.in_(filters.ids))
    
    result = await db.execute(query.order_by(ObjetivoModel.descripcion))
    return list(result.scalars().all())


async def get_organos(
    info, 
    filters: Optional[CatalogoFilterInput] = None,
    incluir_hijos: bool = False
) -> List[Organo]:
    db = info.context["db"]
    query = select(OrganoModel)
    
    if incluir_hijos:
        query = query.options(selectinload(OrganoModel.hijos))
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(
                or_(
                    OrganoModel.nombre.ilike(term),
                    OrganoModel.codigo.ilike(term)
                )
            )
        if filters.ids:
            query = query.where(OrganoModel.id.in_(filters.ids))
    
    result = await db.execute(query.order_by(OrganoModel.nombre))
    return list(result.scalars().all())


async def get_regiones(
    info,
    filters: Optional[CatalogoFilterInput] = None,
    incluir_hijos: bool = False
) -> List[Region]:
    db = info.context["db"]
    query = select(RegionModel)
    
    if incluir_hijos:
        query = query.options(selectinload(RegionModel.hijos))
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(RegionModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(RegionModel.id.in_(filters.ids))
    
    result = await db.execute(query.order_by(RegionModel.descripcion))
    return list(result.scalars().all())


async def get_sectores_actividad(
    info,
    filters: Optional[CatalogoFilterInput] = None,
    incluir_hijos: bool = False
) -> List[SectorActividad]:
    db = info.context["db"]
    query = select(SectorActividadModel)
    
    if incluir_hijos:
        query = query.options(selectinload(SectorActividadModel.hijos))
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(SectorActividadModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(SectorActividadModel.id.in_(filters.ids))
        if filters.codigo_contains:
            query = query.where(SectorActividadModel.codigo.ilike(f"%{filters.codigo_contains}%"))
    
    result = await db.execute(query.order_by(SectorActividadModel.descripcion))
    return list(result.scalars().all())


async def get_tipos_beneficiario(info, filters: Optional[CatalogoFilterInput] = None) -> List[TipoBeneficiario]:
    db = info.context["db"]
    query = select(TipoBeneficiarioModel)
    
    if filters:
        if filters.search:
            term = f"%{filters.search}%"
            query = query.where(TipoBeneficiarioModel.descripcion.ilike(term))
        if filters.ids:
            query = query.where(TipoBeneficiarioModel.id.in_(filters.ids))
        if filters.api_ids:
            query = query.where(TipoBeneficiarioModel.api_id.in_(filters.api_ids))
    
    result = await db.execute(query.order_by(TipoBeneficiarioModel.descripcion))
    return list(result.scalars().all())