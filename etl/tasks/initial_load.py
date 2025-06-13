import logging
import asyncio
from datetime import datetime, timedelta
from etl.extractor import BDNSExtractor
from etl.transformer import BDNSTransformer
from etl.loader import BDNSLoader
from app.cache.redis_cache import redis_cache
from etl.config import get_etl_settings

settings = get_etl_settings()
logger = logging.getLogger(__name__)

async def run_initial_load(años_atras: int = 3, max_concesiones: int = None):
    """
    Ejecutar carga inicial de datos históricos
    
    Args:
        años_atras: Número de años hacia atrás para cargar datos
        max_concesiones: Límite máximo de concesiones a cargar (None para sin límite)
    """
    logger.info(f"Iniciando carga inicial de datos de los últimos {años_atras} años")
    
    # Calcular fecha de inicio
    fecha_desde = datetime.now().date() - timedelta(days=365 * años_atras)
    fecha_hasta = datetime.now().date()
    
    logger.info(f"Periodo de carga: {fecha_desde} hasta {fecha_hasta}")
    
    # Inicializar componentes ETL
    extractor = BDNSExtractor()
    transformer = BDNSTransformer()
    loader = BDNSLoader()
    
    try:
        # 1. Extraer concesiones
        logger.info("Extrayendo concesiones...")
        concesiones_raw = []
        count = 0
        
        for concesion in extractor.extract_concesiones(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta):
            concesiones_raw.append(concesion)
            count += 1
            
            # Mostrar progreso
            if count % 1000 == 0:
                logger.info(f"Extraídas {count} concesiones")
            
            # Verificar límite máximo
            if max_concesiones and count >= max_concesiones:
                logger.info(f"Alcanzado límite máximo de {max_concesiones} concesiones")
                break
        
        logger.info(f"Extracción completada: {len(concesiones_raw)} concesiones")
        
        # 2. Transformar concesiones
        logger.info("Transformando concesiones...")
        concesiones_df = transformer.transform_concesiones(concesiones_raw)
        logger.info(f"Transformación completada: {len(concesiones_df)} concesiones")
        
        # 3. Extraer y transformar beneficiarios
        logger.info("Extrayendo beneficiarios únicos...")
        beneficiarios_df = transformer.transform_beneficiarios(concesiones_df)
        logger.info(f"Extracción completada: {len(beneficiarios_df)} beneficiarios únicos")
        
        # 4. Extraer convocatorias relacionadas
        logger.info("Extrayendo convocatorias relacionadas...")
        convocatorias_ids = concesiones_df['convocatoria_id'].unique().tolist()
        convocatorias_raw = []
        
        for i, conv_id in enumerate(convocatorias_ids):
            if conv_id:
                try:
                    convocatoria = extractor._request(f"convocatorias/{conv_id}", {})
                    if convocatoria:
                        convocatorias_raw.append(convocatoria)
                    
                    # Mostrar progreso
                    if (i + 1) % 100 == 0:
                        logger.info(f"Extraídas {i + 1}/{len(convocatorias_ids)} convocatorias")
                except Exception as e:
                    logger.error(f"Error al extraer convocatoria {conv_id}: {e}")
        
        logger.info(f"Extracción completada: {len(convocatorias_raw)} convocatorias")
        
        # 5. Transformar convocatorias
        logger.info("Transformando convocatorias...")
        convocatorias_df = transformer.transform_convocatorias(convocatorias_raw)
        logger.info(f"Transformación completada: {len(convocatorias_df)} convocatorias")
        
        # 6. Cargar datos en la base de datos
        logger.info("Cargando datos en la base de datos...")
        
        # Cargar en orden para mantener integridad referencial
        beneficiarios_count = loader.load_beneficiarios(beneficiarios_df)
        logger.info(f"Cargados {beneficiarios_count} beneficiarios")
        
        convocatorias_count = loader.load_convocatorias(convocatorias_df)
        logger.info(f"Cargadas {convocatorias_count} convocatorias")
        
        concesiones_count = loader.load_concesiones(concesiones_df)
        logger.info(f"Cargadas {concesiones_count} concesiones")
        
        # 7. Actualizar vistas materializadas
        logger.info("Actualizando vistas materializadas...")
        loader.refresh_materialized_views()
        
        # 8. Limpiar caché
        logger.info("Limpiando caché...")
        await redis_cache.clear_pattern("*")
        
        logger.info("Carga inicial completada con éxito")
        return {
            "beneficiarios": beneficiarios_count,
            "convocatorias": convocatorias_count,
            "concesiones": concesiones_count
        }
    
    except Exception as e:
        logger.error(f"Error en carga inicial: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Ejecutar carga inicial
    asyncio.run(run_initial_load(
        años_atras=3,  # Cargar datos de los últimos 3 años
        max_concesiones=None  # Sin límite de concesiones
    ))
