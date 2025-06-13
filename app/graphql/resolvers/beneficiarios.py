from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.db.session import get_db
from app.db.models import Beneficiario as BeneficiarioModel
from app.graphql.types.beneficiario import Beneficiario, BeneficiarioInput
from app.cache.redis_cache import redis_cache
from fastapi import Depends

async def get_beneficiario_by_id(id: str, db: Session = Depends(get_db)) -> Optional[Beneficiario]:
    """Obtener un beneficiario por su ID"""
    # Intentar obtener de caché primero
    cache_key = f"beneficiario:{id}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return cached
    
    # Si no está en caché, consultar base de datos
    stmt = select(BeneficiarioModel).where(BeneficiarioModel.id == id)
    result = db.execute(stmt).scalar_one_or_none()
    
    if result:
        # Convertir a tipo GraphQL
        beneficiario = _map_beneficiario_model_to_type(result)
        # Guardar en caché
        await redis_cache.set(cache_key, beneficiario, expire=3600)
        return beneficiario
    
    return None

async def get_beneficiarios(
    filtros: Optional[BeneficiarioInput] = None, 
    limite: int = 100, 
    offset: int = 0,
    db: Session = Depends(get_db)
) -> List[Beneficiario]:
    """Obtener beneficiarios con filtros opcionales"""
    # Construir consulta base
    stmt = select(BeneficiarioModel)
    
    # Aplicar filtros si existen
    if filtros:
        if filtros.identificador:
            stmt = stmt.where(BeneficiarioModel.identificador == filtros.identificador)
        if filtros.tipo:
            stmt = stmt.where(BeneficiarioModel.tipo == filtros.tipo)
    
    # Ordenar por nombre
    stmt = stmt.order_by(BeneficiarioModel.nombre)
    
    # Aplicar paginación
    stmt = stmt.limit(limite).offset(offset)
    
    # Ejecutar consulta
    results = db.execute(stmt).scalars().all()
    
    # Convertir a tipos GraphQL
    return [_map_beneficiario_model_to_type(result) for result in results]

def _map_beneficiario_model_to_type(model: BeneficiarioModel) -> Beneficiario:
    """Convertir modelo de base de datos a tipo GraphQL"""
    return Beneficiario(
        id=str(model.id),
        identificador=model.identificador,
        nombre=model.nombre,
        tipo=model.tipo
    )
