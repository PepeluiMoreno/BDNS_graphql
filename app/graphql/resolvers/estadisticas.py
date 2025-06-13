from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from app.db.session import get_db
from app.db.models import Concesion as ConcesionModel
from app.db.models import Beneficiario as BeneficiarioModel
from app.db.models import Organo as OrganoModel
from app.graphql.types.estadisticas import EstadisticasConcesiones, FiltroEstadisticas
from app.cache.redis_cache import redis_cache
from fastapi import Depends

async def get_estadisticas_por_tipo_entidad(
    filtros: Optional[FiltroEstadisticas] = None,
    db: Session = Depends(get_db)
) -> List[EstadisticasConcesiones]:
    """Obtener estadísticas de concesiones agrupadas por tipo de entidad"""
    # Construir clave de caché
    cache_key = f"estadisticas:tipo_entidad:{_build_cache_key_from_filtros(filtros)}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    # Intentar usar vista materializada primero
    try:
        stmt = select(
            "tipo_entidad",
            "año",
            "numero_concesiones",
            "importe_total"
        ).select_from("MV_CONCESIONES_POR_TIPO")
        
        # Aplicar filtros
        if filtros:
            if filtros.año:
                stmt = stmt.where(text("año = :año")).params(año=filtros.año)
            elif filtros.año_desde and filtros.año_hasta:
                stmt = stmt.where(text("año >= :año_desde AND año <= :año_hasta")).params(
                    año_desde=filtros.año_desde, año_hasta=filtros.año_hasta
                )
            if filtros.tipo_entidad:
                stmt = stmt.where(text("tipo_entidad = :tipo_entidad")).params(tipo_entidad=filtros.tipo_entidad)
        
        # Ordenar por importe total descendente
        stmt = stmt.order_by(text("importe_total DESC"))
        
        # Ejecutar consulta
        results = db.execute(stmt).fetchall()
        
        # Convertir a tipos GraphQL
        estadisticas = [
            EstadisticasConcesiones(
                tipo_entidad=row[0],
                año=row[1],
                numero_concesiones=row[2],
                importe_total=row[3]
            )
            for row in results
        ]
        
        # Guardar en caché
        await redis_cache.set(cache_key, estadisticas, expire=3600)
        
        return estadisticas
    
    except Exception as e:
        # Si falla la vista materializada, usar consulta directa
        stmt = (
            select(
                BeneficiarioModel.tipo,
                ConcesionModel.año,
                func.count().label("numero_concesiones"),
                func.sum(ConcesionModel.importe).label("importe_total")
            )
            .join(BeneficiarioModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
            .group_by(BeneficiarioModel.tipo, ConcesionModel.año)
        )
        
        # Aplicar filtros
        if filtros:
            if filtros.año:
                stmt = stmt.where(ConcesionModel.año == filtros.año)
            elif filtros.año_desde and filtros.año_hasta:
                stmt = stmt.where(ConcesionModel.año >= filtros.año_desde)
                stmt = stmt.where(ConcesionModel.año <= filtros.año_hasta)
            if filtros.tipo_entidad:
                stmt = stmt.where(BeneficiarioModel.tipo == filtros.tipo_entidad)
        
        # Ordenar por importe total descendente
        stmt = stmt.order_by(func.sum(ConcesionModel.importe).desc())
        
        # Ejecutar consulta
        results = db.execute(stmt).all()
        
        # Convertir a tipos GraphQL
        estadisticas = [
            EstadisticasConcesiones(
                tipo_entidad=row.tipo,
                año=row.año,
                numero_concesiones=row.numero_concesiones,
                importe_total=row.importe_total
            )
            for row in results
        ]
        
        # Guardar en caché
        await redis_cache.set(cache_key, estadisticas, expire=3600)
        
        return estadisticas

async def get_estadisticas_por_organo(
    filtros: Optional[FiltroEstadisticas] = None,
    db: Session = Depends(get_db)
) -> List[EstadisticasConcesiones]:
    """Obtener estadísticas de concesiones agrupadas por órgano concedente"""
    # Construir clave de caché
    cache_key = f"estadisticas:organo:{_build_cache_key_from_filtros(filtros)}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    # Intentar usar vista materializada primero
    try:
        stmt = select(
            "organo_id",
            "organo_nombre",
            "año",
            "numero_concesiones",
            "importe_total"
        ).select_from("MV_CONCESIONES_POR_ORGANO")
        
        # Aplicar filtros
        if filtros:
            if filtros.año:
                stmt = stmt.where(text("año = :año")).params(año=filtros.año)
            elif filtros.año_desde and filtros.año_hasta:
                stmt = stmt.where(text("año >= :año_desde AND año <= :año_hasta")).params(
                    año_desde=filtros.año_desde, año_hasta=filtros.año_hasta
                )
            if filtros.organo_id:
                stmt = stmt.where(text("organo_id = :organo_id")).params(organo_id=filtros.organo_id)
        
        # Ordenar por importe total descendente
        stmt = stmt.order_by(text("importe_total DESC"))
        
        # Ejecutar consulta
        results = db.execute(stmt).fetchall()
        
        # Convertir a tipos GraphQL
        estadisticas = [
            EstadisticasConcesiones(
                organo_id=str(row[0]),
                organo_nombre=row[1],
                año=row[2],
                numero_concesiones=row[3],
                importe_total=row[4]
            )
            for row in results
        ]
        
        # Guardar en caché
        await redis_cache.set(cache_key, estadisticas, expire=3600)
        
        return estadisticas
    
    except Exception as e:
        # Si falla la vista materializada, usar consulta directa
        stmt = (
            select(
                OrganoModel.id,
                OrganoModel.nombre,
                ConcesionModel.año,
                func.count().label("numero_concesiones"),
                func.sum(ConcesionModel.importe).label("importe_total")
            )
            .join(OrganoModel, ConcesionModel.organo_id == OrganoModel.id)
            .group_by(OrganoModel.id, OrganoModel.nombre, ConcesionModel.año)
        )
        
        # Aplicar filtros
        if filtros:
            if filtros.año:
                stmt = stmt.where(ConcesionModel.año == filtros.año)
            elif filtros.año_desde and filtros.año_hasta:
                stmt = stmt.where(ConcesionModel.año >= filtros.año_desde)
                stmt = stmt.where(ConcesionModel.año <= filtros.año_hasta)
            if filtros.organo_id:
                stmt = stmt.where(OrganoModel.id == filtros.organo_id)
        
        # Ordenar por importe total descendente
        stmt = stmt.order_by(func.sum(ConcesionModel.importe).desc())
        
        # Ejecutar consulta
        results = db.execute(stmt).all()
        
        # Convertir a tipos GraphQL
        estadisticas = [
            EstadisticasConcesiones(
                organo_id=str(row.id),
                organo_nombre=row.nombre,
                año=row.año,
                numero_concesiones=row.numero_concesiones,
                importe_total=row.importe_total
            )
            for row in results
        ]
        
        # Guardar en caché
        await redis_cache.set(cache_key, estadisticas, expire=3600)
        
        return estadisticas

async def get_concentracion_subvenciones(
    año: Optional[int] = None,
    tipo_entidad: Optional[str] = None,
    limite: int = 10,
    db: Session = Depends(get_db)
) -> List[EstadisticasConcesiones]:
    """Obtener estadísticas de concentración de subvenciones por beneficiario"""
    # Construir clave de caché
    cache_key = f"estadisticas:concentracion:año:{año or 'todos'}:tipo:{tipo_entidad or 'todos'}:limite:{limite}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    # Intentar usar vista materializada primero
    try:
        stmt = select(
            "beneficiario_id",
            "beneficiario_nombre",
            "tipo_entidad",
            "año",
            "numero_concesiones",
            "importe_total"
        ).select_from("MV_CONCENTRACION_SUBVENCIONES")
        
        # Aplicar filtros
        if año:
            stmt = stmt.where(text("año = :año")).params(año=año)
        if tipo_entidad:
            stmt = stmt.where(text("tipo_entidad = :tipo_entidad")).params(tipo_entidad=tipo_entidad)
        
        # Ordenar por importe total descendente y limitar resultados
        stmt = stmt.order_by(text("importe_total DESC")).limit(limite)
        
        # Ejecutar consulta
        results = db.execute(stmt).fetchall()
        
        # Convertir a tipos GraphQL
        estadisticas = [
            EstadisticasConcesiones(
                beneficiario_id=str(row[0]),
                beneficiario_nombre=row[1],
                tipo_entidad=row[2],
                año=row[3],
                numero_concesiones=row[4],
                importe_total=row[5]
            )
            for row in results
        ]
        
        # Guardar en caché
        await redis_cache.set(cache_key, estadisticas, expire=3600)
        
        return estadisticas
    
    except Exception as e:
        # Si falla la vista materializada, usar consulta directa
        stmt = (
            select(
                BeneficiarioModel.id,
                BeneficiarioModel.nombre,
                BeneficiarioModel.tipo,
                ConcesionModel.año,
                func.count().label("numero_concesiones"),
                func.sum(ConcesionModel.importe).label("importe_total")
            )
            .join(BeneficiarioModel, ConcesionModel.beneficiario_id == BeneficiarioModel.id)
            .group_by(
                BeneficiarioModel.id,
                BeneficiarioModel.nombre,
                BeneficiarioModel.tipo,
                ConcesionModel.año
            )
        )
        
        # Aplicar filtros
        if año:
            stmt = stmt.where(ConcesionModel.año == año)
        if tipo_entidad:
            stmt = stmt.where(BeneficiarioModel.tipo == tipo_entidad)
        
        # Ordenar por importe total descendente y limitar resultados
        stmt = stmt.order_by(func.sum(ConcesionModel.importe).desc()).limit(limite)
        
        # Ejecutar consulta
        results = db.execute(stmt).all()
        
        # Convertir a tipos GraphQL
        estadisticas = [
            EstadisticasConcesiones(
                beneficiario_id=str(row.id),
                beneficiario_nombre=row.nombre,
                tipo_entidad=row.tipo,
                año=row.año,
                numero_concesiones=row.numero_concesiones,
                importe_total=row.importe_total
            )
            for row in results
        ]
        
        # Guardar en caché
        await redis_cache.set(cache_key, estadisticas, expire=3600)
        
        return estadisticas

def _build_cache_key_from_filtros(filtros: Optional[FiltroEstadisticas]) -> str:
    """Construir clave de caché a partir de filtros"""
    if not filtros:
        return "sin_filtros"
    
    parts = []
    if filtros.año:
        parts.append(f"año:{filtros.año}")
    if filtros.año_desde:
        parts.append(f"año_desde:{filtros.año_desde}")
    if filtros.año_hasta:
        parts.append(f"año_hasta:{filtros.año_hasta}")
    if filtros.tipo_entidad:
        parts.append(f"tipo_entidad:{filtros.tipo_entidad}")
    if filtros.organo_id:
        parts.append(f"organo_id:{filtros.organo_id}")
    
    return "_".join(parts) if parts else "sin_filtros"
