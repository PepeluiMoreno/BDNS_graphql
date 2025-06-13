import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, List, Optional
import logging
from etl.config import get_etl_settings
from app.db.models import (
    Administracion, Departamento, Organo, Region, Finalidad,
    Convocatoria, Beneficiario, Concesion, Pago, Reintegro
)

settings = get_etl_settings()
logger = logging.getLogger(__name__)

class BDNSLoader:
    """Cargador de datos a la base de datos Oracle"""
    
    def __init__(self):
        # Crear conexión a la base de datos
        self.engine = create_engine(
            f"oracle+oracledb://{settings.ORACLE_USER}:{settings.ORACLE_PASSWORD}@"
            f"{settings.ORACLE_HOST}:{settings.ORACLE_PORT}/?service_name={settings.ORACLE_SERVICE}",
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
        )
        self.Session = sessionmaker(bind=self.engine)
    
    def load_convocatorias(self, convocatorias_df: pd.DataFrame) -> int:
        """Cargar convocatorias en la base de datos"""
        if convocatorias_df.empty:
            return 0
        
        session = self.Session()
        try:
            count = 0
            batch_size = settings.ETL_BATCH_SIZE
            
            # Procesar en lotes para evitar problemas de memoria
            for i in range(0, len(convocatorias_df), batch_size):
                batch = convocatorias_df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Verificar si ya existe
                    existing = session.query(Convocatoria).filter_by(
                        codigo_bdns=row['codigo_bdns']
                    ).first()
                    
                    if existing:
                        # Actualizar si es necesario
                        # Aquí se podrían implementar reglas de actualización
                        continue
                    
                    # Crear nueva convocatoria
                    convocatoria = Convocatoria(
                        codigo_bdns=row['codigo_bdns'],
                        titulo=row['titulo'],
                        titulo_cooficial=row.get('titulo_cooficial'),
                        organo_id=row['organo_id'],
                        fecha_registro=row['fecha_registro'],
                        fecha_publicacion=row.get('fecha_publicacion'),
                        fecha_inicio_solicitud=row.get('fecha_inicio_solicitud'),
                        fecha_fin_solicitud=row.get('fecha_fin_solicitud'),
                        tipo_beneficiario=row.get('tipo_beneficiario', 'desconocido'),
                        instrumento_ayuda=row.get('instrumento_ayuda', 'desconocido'),
                        finalidad_id=row.get('finalidad_id'),
                        region_impacto_id=row.get('region_impacto_id'),
                        importe_total=row.get('importe_total'),
                        mecanismo_recuperacion=row.get('mecanismo_recuperacion'),
                        numero_ayuda_estado=row.get('numero_ayuda_estado'),
                        descripcion=row.get('descripcion'),
                        requisitos=row.get('requisitos'),
                        documentacion=row.get('documentacion'),
                        criterios_evaluacion=row.get('criterios_evaluacion'),
                        url_bases_reguladoras=row.get('url_bases_reguladoras'),
                        url_publicacion_oficial=row.get('url_publicacion_oficial')
                    )
                    
                    session.add(convocatoria)
                    count += 1
                
                # Commit por lotes
                session.commit()
                logger.info(f"Cargado lote de {len(batch)} convocatorias")
            
            return count
        except Exception as e:
            session.rollback()
            logger.error(f"Error al cargar convocatorias: {e}")
            raise
        finally:
            session.close()
    
    def load_beneficiarios(self, beneficiarios_df: pd.DataFrame) -> int:
        """Cargar beneficiarios en la base de datos"""
        if beneficiarios_df.empty:
            return 0
        
        session = self.Session()
        try:
            count = 0
            batch_size = settings.ETL_BATCH_SIZE
            
            # Procesar en lotes
            for i in range(0, len(beneficiarios_df), batch_size):
                batch = beneficiarios_df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Verificar si ya existe
                    existing = session.query(Beneficiario).filter_by(
                        identificador=row['identificador']
                    ).first()
                    
                    if existing:
                        # Actualizar si es necesario
                        if existing.nombre != row['nombre'] or existing.tipo != row['tipo']:
                            existing.nombre = row['nombre']
                            existing.tipo = row['tipo']
                            count += 1
                        continue
                    
                    # Crear nuevo beneficiario
                    beneficiario = Beneficiario(
                        identificador=row['identificador'],
                        nombre=row['nombre'],
                        tipo=row['tipo']
                    )
                    
                    session.add(beneficiario)
                    count += 1
                
                # Commit por lotes
                session.commit()
                logger.info(f"Cargado lote de {len(batch)} beneficiarios")
            
            return count
        except Exception as e:
            session.rollback()
            logger.error(f"Error al cargar beneficiarios: {e}")
            raise
        finally:
            session.close()
    
    def load_concesiones(self, concesiones_df: pd.DataFrame) -> int:
        """Cargar concesiones en la base de datos"""
        if concesiones_df.empty:
            return 0
        
        session = self.Session()
        try:
            count = 0
            batch_size = settings.ETL_BATCH_SIZE
            
            # Procesar en lotes
            for i in range(0, len(concesiones_df), batch_size):
                batch = concesiones_df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Verificar si ya existe
                    existing = session.query(Concesion).filter_by(
                        codigo_bdns=row['codigo_bdns']
                    ).first()
                    
                    if existing:
                        # Actualizar si es necesario
                        continue
                    
                    # Obtener ID del beneficiario
                    beneficiario = session.query(Beneficiario).filter_by(
                        identificador=row['beneficiario_id']
                    ).first()
                    
                    if not beneficiario:
                        logger.warning(f"Beneficiario no encontrado: {row['beneficiario_id']}")
                        continue
                    
                    # Crear nueva concesión
                    concesion = Concesion(
                        codigo_bdns=row['codigo_bdns'],
                        convocatoria_id=row['convocatoria_id'],
                        organo_id=row['organo_id'],
                        beneficiario_id=beneficiario.id,
                        fecha_concesion=row['fecha_concesion'],
                        importe=row['importe'],
                        descripcion_proyecto=row.get('descripcion_proyecto'),
                        programa_presupuestario=row.get('programa_presupuestario'),
                        tipo_ayuda=row.get('tipo_ayuda', 'desconocido'),
                        año=row['año']
                    )
                    
                    session.add(concesion)
                    count += 1
                
                # Commit por lotes
                session.commit()
                logger.info(f"Cargado lote de {len(batch)} concesiones")
            
            return count
        except Exception as e:
            session.rollback()
            logger.error(f"Error al cargar concesiones: {e}")
            raise
        finally:
            session.close()
    
    def refresh_materialized_views(self):
        """Actualizar vistas materializadas"""
        session = self.Session()
        try:
            # Lista de vistas materializadas a actualizar
            views = [
                "MV_CONCESIONES_POR_TIPO",
                "MV_CONCESIONES_POR_ORGANO",
                "MV_CONCENTRACION_SUBVENCIONES"
            ]
            
            for view in views:
                logger.info(f"Actualizando vista materializada {view}")
                session.execute(text(f"BEGIN DBMS_MVIEW.REFRESH('{view}'); END;"))
            
            session.commit()
            logger.info("Vistas materializadas actualizadas correctamente")
        except Exception as e:
            session.rollback()
            logger.error(f"Error al actualizar vistas materializadas: {e}")
            raise
        finally:
            session.close()
