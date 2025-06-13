import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, AmbitoGeografico   
import urllib.request
import tempfile
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
import sys

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Añadir el directorio raíz del proyecto al path para imports correctos
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

# Cargar variables de entorno desde el .env en la raíz del proyecto
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

# Configuración de PostgreSQL
POSTGRES_CONFIG = {
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'db': os.getenv('POSTGRES_DB', 'administracion_publica')
}

# Validar configuración
if not all(POSTGRES_CONFIG.values()):
    missing = [k for k, v in POSTGRES_CONFIG.items() if not v]
    logger.error(f"Faltan variables de entorno: {missing}")
    raise ValueError("Configuración de PostgreSQL incompleta")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['db']}"

def setup_database():
    """Configura la conexión a PostgreSQL con manejo de errores mejorado"""
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute("SELECT 1")
            
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine)
    except Exception as e:
        logger.error("Error de conexión a PostgreSQL", exc_info=True)
        raise

def descargar_excel(url):
    """Descarga segura con timeout y manejo de errores"""
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            if response.status != 200:
                raise ValueError(f"HTTP {response.status}")
                
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                tmp_file.write(response.read())
                return tmp_file.name
    except Exception as e:
        logger.error(f"Error descargando {url}", exc_info=True)
        raise

def poblar_estructura_geografica(session):
    """Pobla la jerarquía geográfica con transacciones atómicas"""
    try:
        # Verificar si ya existen datos
        if session.query(AmbitoGeografico).count() > 0:
            logger.warning("La base ya contiene datos geográficos. Usar --force para repoblar")
            return False

        logger.info("Iniciando carga de estructura territorial...")

        # 1. España (nodo raíz)
        with session.begin_nested():
            espana = AmbitoGeografico(
                nombre="España",
                tipo="pais",
                codigo="00"
            )
            session.add(espana)
            logger.info("Creado nodo España")

        # 2. Comunidades Autónomas
        url_ccaa = "https://administracionelectronica.gob.es/ctt/resources/Soluciones/238/Descargas/Catalogo-de-Comunidades-Autonomas.xlsx"
        file_ccaa = descargar_excel(url_ccaa)
        
        try:
            df_ccaa = pd.read_excel(file_ccaa)
            comunidades = {}
            
            with session.begin_nested():
                for _, row in df_ccaa.iterrows():
                    comunidad = AmbitoGeografico(
                        nombre=row['Nombre'],
                        tipo="comunidad",
                        codigo=str(row['Codigo']),
                        parent_id=espana.id
                    )
                    session.add(comunidad)
                    comunidades[str(row['Codigo'])] = comunidad
                    
                logger.info(f"Insertadas {len(comunidades)} CCAA")
        finally:
            os.unlink(file_ccaa)

        # 3. Provincias
        url_prov = "https://administracionelectronica.gob.es/ctt/resources/Soluciones/238/Descargas/Catalogo%20de%20Provincias.xlsx"
        file_prov = descargar_excel(url_prov)
        
        try:
            df_prov = pd.read_excel(file_prov)
            provincias = {}
            
            with session.begin_nested():
                for _, row in df_prov.iterrows():
                    codigo = str(row['Codigo'])
                    provincia = AmbitoGeografico(
                        nombre=row['Nombre'],
                        tipo="provincia",
                        codigo=codigo,
                        parent_id=comunidades[codigo[:2]].id
                    )
                    session.add(provincia)
                    provincias[codigo] = provincia
                    
                logger.info(f"Insertadas {len(provincias)} provincias")
        finally:
            os.unlink(file_prov)

        # 4. Localidades (con inserción por lotes)
        url_loc = "https://administracionelectronica.gob.es/ctt/resources/Soluciones/238/Descargas/Catalogo%20de%20Localidades.xlsx"
        file_loc = descargar_excel(url_loc)
        
        try:
            df_loc = pd.read_excel(file_loc)
            batch_size = 5000
            total = 0
            
            with session.begin_nested():
                for i in range(0, len(df_loc), batch_size):
                    batch = df_loc.iloc[i:i+batch_size]
                    objects = [
                        AmbitoGeografico(
                            nombre=row['Nombre'],
                            tipo="localidad",
                            codigo=str(row['Codigo']),
                            parent_id=provincias[str(row['Codigo'])[:2]].id
                        ) for _, row in batch.iterrows()
                    ]
                    session.bulk_save_objects(objects)
                    total += len(objects)
                    logger.info(f"Localidades insertadas: {total}/{len(df_loc)}")
                    
            logger.info(f"Total localidades insertadas: {total}")
        finally:
            os.unlink(file_loc)

        return True

    except Exception as e:
        session.rollback()
        logger.error("Error en la carga de datos", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        logger.info("Iniciando script de población de datos...")
        
        Session = setup_database()
        session = Session()
        
        if poblar_estructura_geografica(session):
            logger.info("Proceso completado con éxito")
        else:
            logger.warning("Proceso completado sin cambios")
            
    except Exception as e:
        logger.error("Error fatal en el script", exc_info=True)
        sys.exit(1)
    finally:
        if 'session' in locals():
            session.close()
        logger.info("Script finalizado")