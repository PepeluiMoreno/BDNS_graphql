
import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class ETLSettings(BaseSettings):
    # Configuración de la aplicación
    APP_NAME: str = "BDNS GraphQL API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # Configuración de Oracle
    ORACLE_USER: str
    ORACLE_PASSWORD: str
    ORACLE_HOST: str
    ORACLE_PORT: str = "1521"
    ORACLE_SERVICE: str
    
    # Configuración de Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    
    # Configuración de la API BDNS
    BDNS_API_URL: str = "https://www.pap.hacienda.gob.es/bdnstrans/GE/es/api"
    BDNS_REQUEST_DELAY: float = 2.0  # segundos entre peticiones
    BDNS_PAGE_SIZE: int = 1000
    
    # Configuración ETL
    ETL_BATCH_SIZE: int = 5000
    ETL_MAX_WORKERS: int = 1  # Evitar saturar la API
    
    # Límites de consulta GraphQL
    GRAPHQL_MAX_COMPLEXITY: int = 100
    GRAPHQL_MAX_DEPTH: int = 10
    
    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache( )
def get_etl_settings():
    return ETLSettings()
