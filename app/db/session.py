from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de conexión
def get_db_url():
    if os.getenv('DEBUG', 'True') == 'True':
        # PostgreSQL local en modo DEBUG
        return (
            f"postgresql://{os.getenv('DB_USER_LOCAL')}:{os.getenv('DB_PASSWORD_LOCAL')}@"
            f"{os.getenv('DB_HOST_LOCAL')}:{os.getenv('DB_PORT_LOCAL')}/{os.getenv('DB_NAME_LOCAL')}"
        )
    else:
        # Oracle en producción
        return (
            f"oracle+oracledb://{os.getenv('ORACLE_USER')}:{os.getenv('ORACLE_PASSWORD')}@"
            f"{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT')}/?service_name={os.getenv('ORACLE_SERVICE')}"
        )
        
print("Conectando a la base de datos con URL:", get_db_url())

# Crear motor de base de datos
engine = create_engine(
    get_db_url(),
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
)

# Crear sesión
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
 
# Base para modelos declarativos
Base = declarative_base()

# Función para obtener sesión
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()