import logging
from datetime import datetime, timedelta
from etl.extractor import BDNSExtractor
from etl.transformer import BDNSTransformer
from etl.loader import BDNSLoader
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.db.models import Concesion
from app.cache.redis_cache import redis_cache

logger = logging.getLogger(__name__)

async def run_incremental_load():
    """Ejecutar carga incremental de datos"""
    logger.info("Iniciando carga incremental de datos")
    
    # Obtener fecha de última actualización
    db = SessionLocal()
    try:
        ultima_fecha = db.execute(
            select(func.max(Concesion.fecha_concesion))
        ).scalar()
        
        # Si no hay datos, usar fecha hace 30 días
        if not ultima_fecha:
            ultima_fecha = datetime.now().date() - timedelta(days=30)
            
        logger.info(f"Última fecha de concesión: {ultima_fecha}")
    finally:
        db.close()
    
    # Inicializar componentes ETL
    extractor = BDNSExtractor()
    transformer = BDNSTransformer()
    loader = BDNSLoader()
    
    try:
        # 1. Extraer nuevas concesiones
        logger.info(f"Extrayendo concesiones desde {ultima_fecha}")
        concesiones_raw = list(extractor.extract_concesiones(fecha_desde=ultima_fecha))
        logger.info(f"Extraídas {len(concesiones_raw)} concesiones")
        
        if not concesiones_raw:
            logger.info("No hay nuevas concesiones para cargar")
            return
        
        # 2. Transformar concesiones
        concesiones_df = transformer.transform_concesiones(concesiones_raw)
        logger.info(f"Transformadas {len(concesiones_df)} concesiones")
        
        # 3. Extraer y transformar beneficiarios
        beneficiarios_df = transformer.transform_beneficiarios(concesiones_df)
        logger.info(f"Extraídos {len(beneficiarios_df)} beneficiarios únicos")
        
        # 4. Extraer convocatorias relacionadas
        convocatorias_ids = concesiones_df['convocatoria_id'].unique().tolist()
        convocatorias_raw = []
        
        for conv_id in convocatorias_ids:
            if conv_id:
                convocatoria = extractor._request(f"convocatorias/{conv_id}", {})
                if convocatoria:
                    convocatorias_raw.append(convocatoria)
        
        logger.info(f"Extraídas {len(convocatorias_raw)} convocatorias relacionadas")
        
        # 5. Transformar convocatorias
        convocatorias_df = transformer.transform_convocatorias(convocatorias_raw)
        
        # 6. Cargar datos en la base de datos
        logger.info("Cargando datos en la base de datos")
        
        # Cargar en orden para mantener integridad referencial
        beneficiarios_count = loader.load_beneficiarios(beneficiarios_df)
        logger.info(f"Cargados {beneficiarios_count} beneficiarios")
        
        convocatorias_count = loader.load_convocatorias(convocatorias_df)
        logger.info(f"Cargadas {convocatorias_count} convocatorias")
        
        concesiones_count = loader.load_concesiones(concesiones_df)
        logger.info(f"Cargadas {concesiones_count} concesiones")
        
        # 7. Actualizar vistas materializadas
        logger.info("Actualizando vistas materializadas")
        loader.refresh_materialized_views()
        
        # 8. Limpiar caché
        logger.info("Limpiando caché")
        await redis_cache.clear_pattern("concesiones:*")
        await redis_cache.clear_pattern("estadisticas:*")
        
        logger.info("Carga incremental completada con éxito")
        return {
            "beneficiarios": beneficiarios_count,
            "convocatorias": convocatorias_count,
            "concesiones": concesiones_count
        }
    
    except Exception as e:
        logger.error(f"Error en carga incremental: {e}", exc_info=True)
        raise
