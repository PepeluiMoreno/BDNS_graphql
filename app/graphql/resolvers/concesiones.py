from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from app.db.session import get_db
from app.db.models import Concesion as ConcesionModel
from app.db.models import Beneficiario as BeneficiarioModel
from app.db.models import Organo as OrganoModel
from app.db.models import Convocatoria as ConvocatoriaModel
from app.graphql.types.concesion import Concesion, ConcesionInput
from app.cache.redis_cache import redis_cache
from fastapi import Depends

async def get_concesion_by_id(id: str, db: Session = Depends(get_db)) -> Optional[Concesion]:
    """Obtener una concesión por su ID"""
    # Intentar obtener de caché primero
    cache_key = f"concesion:{id}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    # Si no está en caché, consultar base de datos
    stmt = (
        select(ConcesionModel)
        .where(ConcesionModel.id == id)
        .options(
            joinedload(ConcesionModel.convocatoria).joinedload(ConvocatoriaModel.organo),
            joinedload(ConcesionModel.organo),
            joinedload(ConcesionModel.beneficiario)
        )
    )
    result = db.execute(stmt).scalar_one_or_none()
    
    if result:
        # Convertir a tipo GraphQL
        concesion = _map_concesion_model_to_type(result)
        # Guardar en caché
        await redis_cache.set(cache_key, concesion, expire=3600)
        return concesion
    
    return None

async def get_concesiones(
    filtros: Optional[ConcesionInput] = None, 
    limite: int = 100, 
    offset: int = 0,
    db: Session = Depends(get_db)
) -> List[Concesion]:
    """Obtener concesiones con filtros opcionales"""
    # Construir consulta base
    stmt = (
        select(ConcesionModel)
        .options(
            joinedload(ConcesionModel.convocatoria).joinedload(ConvocatoriaModel.organo),
            joinedload(ConcesionModel.organo),
            joinedload(ConcesionModel.beneficiario)
        )
    )
    
    # Aplicar filtros si existen
    if filtros:
        if filtros.codigo_bdns:
            stmt = stmt.where(ConcesionModel.codigo_bdns == filtros.codigo_bdns)
        if filtros.organo_id:
            stmt = stmt.where(ConcesionModel.organo_id == filtros.organo_id)
        if filtros.beneficiario_id:
            stmt = stmt.where(ConcesionModel.beneficiario_id == filtros.beneficiario_id)
        if filtros.tipo_beneficiario:
            stmt = stmt.join(ConcesionModel.beneficiario).where(
                BeneficiarioModel.tipo == filtros.tipo_beneficiario
            )
        if filtros.fecha_desde:
            stmt = stmt.where(ConcesionModel.fecha_concesion >= filtros.fecha_desde)
        if filtros.fecha_hasta:
            stmt = stmt.where(ConcesionModel.fecha_concesion <= filtros.fecha_hasta)
        if filtros.importe_minimo:
            stmt = stmt.where(ConcesionModel.importe >= filtros.importe_minimo)
        if filtros.importe_maximo:
            stmt = stmt.where(ConcesionModel.importe <= filtros.importe_maximo)
        if filtros.tipo_ayuda:
            stmt = stmt.where(ConcesionModel.tipo_ayuda == filtros.tipo_ayuda)
        if filtros.año:
            stmt = stmt.where(ConcesionModel.año == filtros.año)
    
    # Ordenar por fecha de concesión descendente
    stmt = stmt.order_by(ConcesionModel.fecha_concesion.desc())
    
    # Aplicar paginación
    stmt = stmt.limit(limite).offset(offset)
    
    # Ejecutar consulta
    results = db.execute(stmt).scalars().all()
    
    # Convertir a tipos GraphQL
    return [_map_concesion_model_to_type(result) for result in results]

async def get_concesiones_por_beneficiario(
    beneficiario_id: str,
    año: Optional[int] = None,
    limite: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
) -> List[Concesion]:
    """Obtener concesiones de un beneficiario específico"""
    # Construir clave de caché
    cache_key = f"concesiones:beneficiario:{beneficiario_id}:año:{año or 'todos'}:limite:{limite}:offset:{offset}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    # Construir consulta
    stmt = (
        select(ConcesionModel)
        .where(ConcesionModel.beneficiario_id == beneficiario_id)
        .options(
            joinedload(ConcesionModel.convocatoria).joinedload(ConvocatoriaModel.organo),
            joinedload(ConcesionModel.organo),
            joinedload(ConcesionModel.beneficiario)
        )
    )
    
    # Filtrar por año si se especifica
    if año:
        stmt = stmt.where(ConcesionModel.año == año)
    
    # Ordenar y paginar
    stmt = stmt.order_by(ConcesionModel.fecha_concesion.desc())
    stmt = stmt.limit(limite).offset(offset)
    
    # Ejecutar consulta
    results = db.execute(stmt).scalars().all()
    
    # Convertir a tipos GraphQL
    concesiones = [_map_concesion_model_to_type(result) for result in results]
    
    # Guardar en caché
    await redis_cache.set(cache_key, concesiones, expire=3600)
    
    return concesiones

def _map_concesion_model_to_type(model: ConcesionModel) -> Concesion:
    """Convertir modelo de base de datos a tipo GraphQL"""
    return Concesion(
        id=str(model.id),
        codigo_bdns=model.codigo_bdns,
        convocatoria=Convocatoria(
            id=str(model.convocatoria.id),
            codigo_bdns=model.convocatoria.codigo_bdns,
            titulo=model.convocatoria.titulo,
            organo=Organo(
                id=str(model.convocatoria.organo.id),
                nombre=model.convocatoria.organo.nombre,
                codigo=model.convocatoria.organo.codigo
            )
        ),
        organo=Organo(
            id=str(model.organo.id),
            nombre=model.organo.nombre,
            codigo=model.organo.codigo
        ),
        beneficiario=Beneficiario(
            id=str(model.beneficiario.id),
            identificador=model.beneficiario.identificador,
            nombre=model.beneficiario.nombre,
            tipo=model.beneficiario.tipo
        ),
        fecha_concesion=model.fecha_concesion,
        importe=model.importe,
        descripcion_proyecto=model.descripcion_proyecto,
        programa_presupuestario=model.programa_presupuestario,
        tipo_ayuda=model.tipo_ayuda,
        año=model.año
    )
