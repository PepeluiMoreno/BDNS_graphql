#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para aplicar las migraciones de Alembic y crear las tablas en la base de datos
"""

import os
import sys
import logging
import subprocess
from pathlib import Path
from sqlalchemy import create_engine, inspect

# Agrega el directorio raíz al PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("alembic_migrations.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("alembic_migrations")

# Directorios
DIR_BASE = Path(__file__).resolve().parent.parent  # Apunta al directorio raíz del proyecto
DB_URL = os.environ.get('DB_URL', 'postgresql://postgres:postgres@localhost/bdnsdb')

def aplicar_migraciones():
    """
    Aplica todas las migraciones pendientes
    """
    logger.info("Aplicando migraciones...")
    
    try:
        # Especifica explícitamente el archivo alembic.ini
        subprocess.run(
            ["alembic", "-c", str(DIR_BASE / "alembic.ini"), "upgrade", "head"],
            check=True,
            cwd=DIR_BASE
        )
        logger.info("Migraciones aplicadas correctamente")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al aplicar migraciones: {e}")
        return False
    except FileNotFoundError as e:
        logger.error(f"Error: No se encontró el archivo alembic.ini o el comando alembic: {e}")
        return False

def verificar_tablas():
    """
    Verifica que las tablas se hayan creado correctamente
    """
    logger.info("Verificando tablas creadas...")
    
    try:
        # Conectar a la base de datos
        engine = create_engine(DB_URL)
        inspector = inspect(engine)
        
        # Obtener lista de tablas
        tablas = inspector.get_table_names()
        
        # Lista de tablas esperadas según models.py
        tablas_esperadas = [
            'ambito_geografico',
            'tipo_organo_administrativo',
            'organo_administrativo',
            'relacion_jerarquica',
            'convocatoria',
            'actividad',
            'actividad_convocatoria',
            'concesion',
            'ayuda_estado',
            'minimis',
            'alembic_version'
        ]
        
        # Verificar que todas las tablas esperadas existan
        tablas_faltantes = [tabla for tabla in tablas_esperadas if tabla not in tablas]
        
        if tablas_faltantes:
            logger.warning(f"Faltan las siguientes tablas: {', '.join(tablas_faltantes)}")
            return False
        
        logger.info(f"Se han creado correctamente todas las tablas esperadas ({len(tablas_esperadas)})")
        
        # Mostrar detalles de cada tabla
        for tabla in tablas:
            columnas = inspector.get_columns(tabla)
            logger.info(f"Tabla '{tabla}': {len(columnas)} columnas")
        
        return True
    except Exception as e:
        logger.error(f"Error al verificar tablas: {e}")
        return False

def main():
    """
    Función principal
    """
    logger.info("Iniciando aplicación de migraciones...")
    
    # Verificar que alembic.ini existe
    alembic_ini_path = DIR_BASE / "alembic.ini"
    if not alembic_ini_path.exists():
        logger.error(f"No se encontró el archivo alembic.ini en {alembic_ini_path}")
        return False
    
    # Aplicar migraciones y verificar tablas
    if aplicar_migraciones():
        verificar_tablas()
    else:
        logger.error("No se pudieron aplicar las migraciones, omitiendo verificación de tablas")
    
    logger.info("Proceso de aplicación de migraciones completado")
    return True

if __name__ == "__main__":
    main()
