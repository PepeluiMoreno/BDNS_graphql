from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, extract, case
from sqlalchemy.sql import text
from bdns_core.db.models import Concesion as ConcesionModel
from bdns_core.db.models import Beneficiario as BeneficiarioModel
from bdns_core.db.models import Convocatoria as ConvocatoriaModel
from bdns_core.db.models import Organo as OrganoModel
from bdns_core.db.models import FormaJuridica as FormaJuridicaModel
from bdns_core.db.models import Region as RegionModel
from bdns_core.db.models import RegimenAyuda as RegimenAyudaModel
from ..types.estadisticas import (
    EstadisticasConcesiones,
    FiltroEstadisticas,
    EvolucionMensual,
    EstadisticasRegimen,
    EstadisticasRegion,
    TopConvocatoria,
    ComparativaAnual
)
from ...cache.redis_cache import redis_cache


# ============================================================================
# EXISTENTES (CORREGIDAS)
# ============================================================================

async def get_estadisticas_por_tipo_entidad(
    info,
    filtros: Optional[FiltroEstadisticas] = None
) -> List[EstadisticasConcesiones]:
    db = info.context["db"]
    
    cache_key = f"estadisticas:tipo_entidad:{_build_cache_key_from_filtros(filtros)}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            FormaJuridicaModel.tipo.label("tipo_entidad"),
            anio_col.label("anio"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_total")
        )
        .join(BeneficiarioModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
        .join(FormaJuridicaModel, BeneficiarioModel.forma_juridica_id == FormaJuridicaModel.id)
        .group_by(FormaJuridicaModel.tipo, anio_col)
    )
    
    if filtros:
        if filtros.anio:
            stmt = stmt.where(anio_col == filtros.anio)
        elif filtros.anio_desde and filtros.anio_hasta:
            stmt = stmt.where(anio_col >= filtros.anio_desde)
            stmt = stmt.where(anio_col <= filtros.anio_hasta)
        if filtros.tipo_entidad:
            stmt = stmt.where(FormaJuridicaModel.tipo == filtros.tipo_entidad)
    
    stmt = stmt.order_by(func.sum(
        func.coalesce(ConcesionModel.importe_equivalente, 0) +
        func.coalesce(ConcesionModel.importe_nominal, 0)
    ).desc())
    
    result = await db.execute(stmt)
    rows = result.all()
    
    estadisticas = [
        EstadisticasConcesiones(
            tipo_entidad=row.tipo_entidad,
            anio=int(row.anio) if row.anio else None,
            numero_concesiones=row.numero_concesiones,
            importe_total=float(row.importe_total) if row.importe_total else 0
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, estadisticas, expire=3600)
    return estadisticas


async def get_estadisticas_por_organo(
    info,
    filtros: Optional[FiltroEstadisticas] = None
) -> List[EstadisticasConcesiones]:
    db = info.context["db"]
    
    cache_key = f"estadisticas:organo:{_build_cache_key_from_filtros(filtros)}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            OrganoModel.id.label("organo_id"),
            OrganoModel.nombre.label("organo_nombre"),
            anio_col.label("anio"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_total")
        )
        .join(ConvocatoriaModel, ConcesionModel.convocatoria_id == ConvocatoriaModel.id)
        .join(OrganoModel, ConvocatoriaModel.organo_id == OrganoModel.id)
        .group_by(OrganoModel.id, OrganoModel.nombre, anio_col)
    )
    
    if filtros:
        if filtros.anio:
            stmt = stmt.where(anio_col == filtros.anio)
        elif filtros.anio_desde and filtros.anio_hasta:
            stmt = stmt.where(anio_col >= filtros.anio_desde)
            stmt = stmt.where(anio_col <= filtros.anio_hasta)
        if filtros.organo_id:
            stmt = stmt.where(OrganoModel.id == filtros.organo_id)
    
    stmt = stmt.order_by(func.sum(
        func.coalesce(ConcesionModel.importe_equivalente, 0) +
        func.coalesce(ConcesionModel.importe_nominal, 0)
    ).desc())
    
    result = await db.execute(stmt)
    rows = result.all()
    
    estadisticas = [
        EstadisticasConcesiones(
            organo_id=str(row.organo_id),
            organo_nombre=row.organo_nombre,
            anio=int(row.anio) if row.anio else None,
            numero_concesiones=row.numero_concesiones,
            importe_total=float(row.importe_total) if row.importe_total else 0
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, estadisticas, expire=3600)
    return estadisticas


async def get_concentracion_subvenciones(
    info,
    anio: Optional[int] = None,
    tipo_entidad: Optional[str] = None,
    limite: int = 10
) -> List[EstadisticasConcesiones]:
    db = info.context["db"]
    
    cache_key = f"estadisticas:concentracion:anio:{anio or 'todos'}:tipo:{tipo_entidad or 'todos'}:limite:{limite}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            BeneficiarioModel.id.label("beneficiario_id"),
            BeneficiarioModel.nombre.label("beneficiario_nombre"),
            FormaJuridicaModel.tipo.label("tipo_entidad"),
            anio_col.label("anio"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_total")
        )
        .join(BeneficiarioModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
        .join(FormaJuridicaModel, BeneficiarioModel.forma_juridica_id == FormaJuridicaModel.id)
        .group_by(
            BeneficiarioModel.id,
            BeneficiarioModel.nombre,
            FormaJuridicaModel.tipo,
            anio_col
        )
    )
    
    if anio:
        stmt = stmt.where(anio_col == anio)
    if tipo_entidad:
        stmt = stmt.where(FormaJuridicaModel.tipo == tipo_entidad)
    
    stmt = stmt.order_by(func.sum(
        func.coalesce(ConcesionModel.importe_equivalente, 0) +
        func.coalesce(ConcesionModel.importe_nominal, 0)
    ).desc()).limit(limite)
    
    result = await db.execute(stmt)
    rows = result.all()
    
    estadisticas = [
        EstadisticasConcesiones(
            beneficiario_id=str(row.beneficiario_id),
            beneficiario_nombre=row.beneficiario_nombre,
            tipo_entidad=row.tipo_entidad,
            anio=int(row.anio) if row.anio else None,
            numero_concesiones=row.numero_concesiones,
            importe_total=float(row.importe_total) if row.importe_total else 0
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, estadisticas, expire=3600)
    return estadisticas


# ============================================================================
# NUEVAS ESTADÍSTICAS
# ============================================================================

async def get_estadisticas_evolucion_mensual(
    info,
    anio: int
) -> List[EvolucionMensual]:
    """Evolución mensual de concesiones para un año"""
    db = info.context["db"]
    
    cache_key = f"estadisticas:evolucion_mensual:{anio}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    mes_col = extract('month', ConcesionModel.fecha_concesion)
    
    # Total anual para calcular acumulado
    total_anual = await db.scalar(
        select(func.sum(
            func.coalesce(ConcesionModel.importe_equivalente, 0) +
            func.coalesce(ConcesionModel.importe_nominal, 0)
        )).where(extract('year', ConcesionModel.fecha_concesion) == anio)
    ) or 0
    
    stmt = (
        select(
            mes_col.label("mes"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_mensual")
        )
        .where(extract('year', ConcesionModel.fecha_concesion) == anio)
        .group_by(mes_col)
        .order_by(mes_col)
    )
    
    result = await db.execute(stmt)
    rows = result.all()
    
    acumulado = 0
    evolucion = []
    
    for row in rows:
        acumulado += float(row.importe_mensual or 0)
        evolucion.append(
            EvolucionMensual(
                mes=int(row.mes),
                numero_concesiones=row.numero_concesiones,
                importe_mensual=float(row.importe_mensual or 0),
                acumulado_anual=acumulado,
                porcentaje_total=(acumulado / total_anual * 100) if total_anual > 0 else 0
            )
        )
    
    await redis_cache.set(cache_key, evolucion, expire=3600)
    return evolucion


async def get_estadisticas_por_regimen(
    info,
    anio: Optional[int] = None
) -> List[EstadisticasRegimen]:
    """Estadísticas por tipo de régimen (minimis, ayuda_estado, ordinaria)"""
    db = info.context["db"]
    
    cache_key = f"estadisticas:regimen:{anio or 'todos'}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            RegimenAyudaModel.descripcion_norm.label("regimen"),
            anio_col.label("anio"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_total")
        )
        .join(RegimenAyudaModel, ConcesionModel.regimen_ayuda_id == RegimenAyudaModel.id)
        .group_by(RegimenAyudaModel.descripcion_norm, anio_col)
    )
    
    if anio:
        stmt = stmt.where(anio_col == anio)
    
    result = await db.execute(stmt)
    rows = result.all()
    
    # Calcular total para porcentajes
    total_importe = sum(row.importe_total or 0 for row in rows)
    total_concesiones = sum(row.numero_concesiones for row in rows)
    
    estadisticas = [
        EstadisticasRegimen(
            regimen=row.regimen or "desconocido",
            anio=int(row.anio) if row.anio else None,
            numero_concesiones=row.numero_concesiones,
            importe_total=float(row.importe_total or 0),
            porcentaje_importe=(float(row.importe_total or 0) / total_importe * 100) if total_importe > 0 else 0,
            porcentaje_concesiones=(row.numero_concesiones / total_concesiones * 100) if total_concesiones > 0 else 0
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, estadisticas, expire=3600)
    return estadisticas


async def get_estadisticas_por_region(
    info,
    anio: Optional[int] = None,
    limite: int = 20
) -> List[EstadisticasRegion]:
    """Estadísticas por comunidad autónoma / provincia"""
    db = info.context["db"]
    
    cache_key = f"estadisticas:region:{anio or 'todos'}:limite:{limite}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            RegionModel.id.label("region_id"),
            RegionModel.descripcion.label("region_nombre"),
            anio_col.label("anio"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_total"),
            func.count(BeneficiarioModel.id.distinct()).label("numero_beneficiarios")
        )
        .join(BeneficiarioModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
        .join(RegionModel, ConcesionModel.region_id == RegionModel.id)
        .group_by(RegionModel.id, RegionModel.descripcion, anio_col)
    )
    
    if anio:
        stmt = stmt.where(anio_col == anio)
    
    stmt = stmt.order_by(func.sum(
        func.coalesce(ConcesionModel.importe_equivalente, 0) +
        func.coalesce(ConcesionModel.importe_nominal, 0)
    ).desc()).limit(limite)
    
    result = await db.execute(stmt)
    rows = result.all()
    
    estadisticas = [
        EstadisticasRegion(
            region_id=str(row.region_id),
            region_nombre=row.region_nombre,
            anio=int(row.anio) if row.anio else None,
            numero_concesiones=row.numero_concesiones,
            importe_total=float(row.importe_total or 0),
            numero_beneficiarios=row.numero_beneficiarios,
            importe_medio=float(row.importe_total or 0) / row.numero_concesiones if row.numero_concesiones > 0 else 0
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, estadisticas, expire=3600)
    return estadisticas


async def get_top_convocatorias(
    info,
    anio: Optional[int] = None,
    limite: int = 10
) -> List[TopConvocatoria]:
    """Convocatorias con más presupuesto concedido"""
    db = info.context["db"]
    
    cache_key = f"estadisticas:top_convocatorias:{anio or 'todos'}:limite:{limite}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            ConvocatoriaModel.id.label("convocatoria_id"),
            ConvocatoriaModel.codigo_bdns,
            ConvocatoriaModel.titulo,
            ConvocatoriaModel.presupuesto_total,
            anio_col.label("anio"),
            func.count(ConcesionModel.id).label("numero_beneficiarios"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_concedido")
        )
        .join(ConvocatoriaModel, ConcesionModel.convocatoria_id == ConvocatoriaModel.id)
        .group_by(
            ConvocatoriaModel.id,
            ConvocatoriaModel.codigo_bdns,
            ConvocatoriaModel.titulo,
            ConvocatoriaModel.presupuesto_total,
            anio_col
        )
    )
    
    if anio:
        stmt = stmt.where(anio_col == anio)
    
    stmt = stmt.order_by(func.sum(
        func.coalesce(ConcesionModel.importe_equivalente, 0) +
        func.coalesce(ConcesionModel.importe_nominal, 0)
    ).desc()).limit(limite)
    
    result = await db.execute(stmt)
    rows = result.all()
    
    top = [
        TopConvocatoria(
            convocatoria_id=str(row.convocatoria_id),
            codigo_bdns=row.codigo_bdns,
            titulo=row.titulo,
            anio=int(row.anio) if row.anio else None,
            importe_concedido=float(row.importe_concedido or 0),
            presupuesto_total=float(row.presupuesto_total or 0),
            porcentaje_ejecutado=(float(row.importe_concedido or 0) / float(row.presupuesto_total or 1) * 100) if row.presupuesto_total else 0,
            numero_beneficiarios=row.numero_beneficiarios
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, top, expire=3600)
    return top


async def get_beneficiarios_recurrentes(
    info,
    anio: Optional[int] = None,
    minimo_concesiones: int = 3,
    limite: int = 20
) -> List[EstadisticasConcesiones]:
    """Beneficiarios con más de N concesiones en un año"""
    db = info.context["db"]
    
    cache_key = f"estadisticas:recurrentes:{anio or 'todos'}:min:{minimo_concesiones}:limite:{limite}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    stmt = (
        select(
            BeneficiarioModel.id.label("beneficiario_id"),
            BeneficiarioModel.nombre.label("beneficiario_nombre"),
            FormaJuridicaModel.tipo.label("tipo_entidad"),
            anio_col.label("anio"),
            func.count().label("numero_concesiones"),
            func.sum(
                func.coalesce(ConcesionModel.importe_equivalente, 0) +
                func.coalesce(ConcesionModel.importe_nominal, 0)
            ).label("importe_total"),
            func.min(ConcesionModel.fecha_concesion).label("primera_concesion"),
            func.max(ConcesionModel.fecha_concesion).label("ultima_concesion")
        )
        .join(BeneficiarioModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
        .join(FormaJuridicaModel, BeneficiarioModel.forma_juridica_id == FormaJuridicaModel.id)
        .group_by(
            BeneficiarioModel.id,
            BeneficiarioModel.nombre,
            FormaJuridicaModel.tipo,
            anio_col
        )
        .having(func.count() >= minimo_concesiones)
    )
    
    if anio:
        stmt = stmt.where(anio_col == anio)
    
    stmt = stmt.order_by(func.count().desc()).limit(limite)
    
    result = await db.execute(stmt)
    rows = result.all()
    
    estadisticas = [
        EstadisticasConcesiones(
            beneficiario_id=str(row.beneficiario_id),
            beneficiario_nombre=row.beneficiario_nombre,
            tipo_entidad=row.tipo_entidad,
            anio=int(row.anio) if row.anio else None,
            numero_concesiones=row.numero_concesiones,
            importe_total=float(row.importe_total or 0),
            primera_concesion=row.primera_concesion,
            ultima_concesion=row.ultima_concesion
        )
        for row in rows
    ]
    
    await redis_cache.set(cache_key, estadisticas, expire=3600)
    return estadisticas


async def get_comparativa_anual(
    info,
    anio_base: int,
    anio_comparar: int
) -> ComparativaAnual:
    """Comparativa interanual de métricas clave"""
    db = info.context["db"]
    
    cache_key = f"estadisticas:comparativa:{anio_base}:{anio_comparar}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    anio_col = extract('year', ConcesionModel.fecha_concesion)
    
    # Métricas para año base
    base_importe = await db.scalar(
        select(func.sum(
            func.coalesce(ConcesionModel.importe_equivalente, 0) +
            func.coalesce(ConcesionModel.importe_nominal, 0)
        )).where(anio_col == anio_base)
    ) or 0
    
    base_concesiones = await db.scalar(
        select(func.count()).where(anio_col == anio_base)
    ) or 0
    
    base_beneficiarios = await db.scalar(
        select(func.count(BeneficiarioModel.id.distinct()))
        .join(ConcesionModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
        .where(anio_col == anio_base)
    ) or 0
    
    base_importe_medio = base_importe / base_concesiones if base_concesiones > 0 else 0
    
    # Métricas para año comparar
    comp_importe = await db.scalar(
        select(func.sum(
            func.coalesce(ConcesionModel.importe_equivalente, 0) +
            func.coalesce(ConcesionModel.importe_nominal, 0)
        )).where(anio_col == anio_comparar)
    ) or 0
    
    comp_concesiones = await db.scalar(
        select(func.count()).where(anio_col == anio_comparar)
    ) or 0
    
    comp_beneficiarios = await db.scalar(
        select(func.count(BeneficiarioModel.id.distinct()))
        .join(ConcesionModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
        .where(anio_col == anio_comparar)
    ) or 0
    
    comp_importe_medio = comp_importe / comp_concesiones if comp_concesiones > 0 else 0
    
    comparativa = ComparativaAnual(
        anio_base=anio_base,
        anio_comparar=anio_comparar,
        total_concedido_base=float(base_importe),
        total_concedido_comparar=float(comp_importe),
        variacion_importe=float(comp_importe - base_importe),
        variacion_importe_porcentual=((comp_importe - base_importe) / base_importe * 100) if base_importe > 0 else 0,
        numero_concesiones_base=base_concesiones,
        numero_concesiones_comparar=comp_concesiones,
        variacion_concesiones=comp_concesiones - base_concesiones,
        variacion_concesiones_porcentual=((comp_concesiones - base_concesiones) / base_concesiones * 100) if base_concesiones > 0 else 0,
        importe_medio_base=float(base_importe_medio),
        importe_medio_comparar=float(comp_importe_medio),
        variacion_importe_medio=float(comp_importe_medio - base_importe_medio),
        variacion_importe_medio_porcentual=((comp_importe_medio - base_importe_medio) / base_importe_medio * 100) if base_importe_medio > 0 else 0,
        numero_beneficiarios_base=base_beneficiarios,
        numero_beneficiarios_comparar=comp_beneficiarios,
        variacion_beneficiarios=comp_beneficiarios - base_beneficiarios,
        variacion_beneficiarios_porcentual=((comp_beneficiarios - base_beneficiarios) / base_beneficiarios * 100) if base_beneficiarios > 0 else 0
    )
    
    await redis_cache.set(cache_key, comparativa, expire=3600)
    return comparativa


def _build_cache_key_from_filtros(filtros: Optional[FiltroEstadisticas]) -> str:
    if not filtros:
        return "sin_filtros"
    
    parts = []
    if filtros.anio:
        parts.append(f"anio:{filtros.anio}")
    if filtros.anio_desde:
        parts.append(f"anio_desde:{filtros.anio_desde}")
    if filtros.anio_hasta:
        parts.append(f"anio_hasta:{filtros.anio_hasta}")
    if filtros.tipo_entidad:
        parts.append(f"tipo_entidad:{filtros.tipo_entidad}")
    if filtros.organo_id:
        parts.append(f"organo_id:{filtros.organo_id}")
    
    return "_".join(parts) if parts else "sin_filtros"
