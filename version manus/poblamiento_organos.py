#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script   para poblar ámbitos geográficos y órganos administrativos
a partir de archivos Excel, utilizando los códigos de los Excel como claves primarias
y creando un índice compuesto por los tres niveles jerárquicos.

Versión ajustada para usar las cabeceras correctas de los archivos Excel.
"""

import os
import sys
import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, Index, text, Table, Column, String, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError


# Ajustar el path para incluir la raíz del proyecto
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Importar modelos
from app.db.models import Base, AmbitoGeografico, OrganoAdministrativo, TipoOrganoAdministrativo
 
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("poblamiento_unificado")

# Configuración de la base de datos
DB_URL = os.environ.get('DB_URL', 'postgresql://postgres:postgres@localhost/bdnsdb')

# Rutas de archivos Excel
EXCEL_DIR = '..\data'
PROVINCIAS_EXCEL = os.path.join(EXCEL_DIR, 'Catalogo de Provincias.xlsx')
TIPOS_ENTIDAD_EXCEL = os.path.join(EXCEL_DIR, 'Catalogo de Tipos de Entidad Publica.xlsx')
UNIDADES_EELL_EXCEL = os.path.join(EXCEL_DIR, 'Listado Unidades EELL.xlsx')

# Definir la tabla de mapeo para términos en diferentes lenguas
metadata = MetaData()
tabla_mapeo_terminos = Table(
    'mapeo_terminos_lenguas', 
    metadata,
    Column('termino', String(255), primary_key=True),
    Column('lengua', String(10), nullable=False),
    Column('termino_normalizado', String(255), nullable=False),
    Column('tipo_entidad', String(50), nullable=True)
)

# Definir la tabla de mapeo nivel1-comunidad autónoma
tabla_mapeo_nivel1 = Table(
    'mapeo_nivel1_comunidad_autonoma', 
    metadata,
    Column('nivel1', String(255), primary_key=True),
    Column('codigo_comunidad_autonoma', String(10), nullable=False),
    Column('descripcion', String(255), nullable=True)
)

# Territorios con idiomas cooficiales
TERRITORIOS_CATALAN = ['CA09', 'CA10', 'CA04']  # Cataluña, C. Valenciana, Baleares
TERRITORIOS_EUSKERA = ['CA16', 'CA15']          # País Vasco, Navarra
TERRITORIOS_GALLEGO = ['CA12']                  # Galicia

def crear_conexion():
    """
    Crea la conexión a la base de datos
    """
    try:
        engine = create_engine(DB_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        return engine, session
    except Exception as e:
        logger.error(f"Error al conectar con la base de datos: {e}")
        raise

def inicializar_bd(engine):
    """
    Inicializa la base de datos creando todas las tablas
    """
    logger.info("Inicializando base de datos...")
    
    try:
        # Crear tablas del modelo
        Base.metadata.create_all(engine)
        
        # Crear tablas de mapeo
        metadata.create_all(engine)
        
        logger.info("Base de datos inicializada correctamente")
        return True
    except Exception as e:
        logger.error(f"Error al inicializar la base de datos: {e}")
        return False

def crear_indices(engine):
    """
    Crea índices para optimizar búsquedas
    """
    logger.info("Creando índices...")
    
    try:
        # Verificar si los índices ya existen
        with engine.connect() as conn:
            if DB_URL.startswith('sqlite'):
                # Para SQLite
                result = conn.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name='ix_organo_administrativo_niveles'"
                ))
                index_niveles_exists = result.fetchone() is not None
                
                result = conn.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name='ix_ambito_geografico_nombre'"
                ))
                index_nombre_exists = result.fetchone() is not None
                
                result = conn.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name='ix_ambito_geografico_tipo_nombre'"
                ))
                index_tipo_nombre_exists = result.fetchone() is not None
            else:
                # Para PostgreSQL u otros
                result = conn.execute(text(
                    "SELECT indexname FROM pg_indexes WHERE indexname = 'ix_organo_administrativo_niveles'"
                ))
                index_niveles_exists = result.fetchone() is not None
                
                result = conn.execute(text(
                    "SELECT indexname FROM pg_indexes WHERE indexname = 'ix_ambito_geografico_nombre'"
                ))
                index_nombre_exists = result.fetchone() is not None
                
                result = conn.execute(text(
                    "SELECT indexname FROM pg_indexes WHERE indexname = 'ix_ambito_geografico_tipo_nombre'"
                ))
                index_tipo_nombre_exists = result.fetchone() is not None
        
        # Crear índice compuesto por niveles en órganos administrativos
        if not index_niveles_exists:
            Index('ix_organo_administrativo_niveles', 
                  OrganoAdministrativo.nivel1, 
                  OrganoAdministrativo.nivel2, 
                  OrganoAdministrativo.nivel3).create(engine)
            logger.info("Índice compuesto por niveles creado correctamente")
        else:
            logger.info("El índice compuesto por niveles ya existe")
        
        # Crear índice por nombre en ámbito geográfico
        if not index_nombre_exists:
            Index('ix_ambito_geografico_nombre', 
                  AmbitoGeografico.nombre).create(engine)
            logger.info("Índice por nombre creado correctamente")
        else:
            logger.info("El índice por nombre ya existe")
        
        # Crear índice compuesto por tipo y nombre en ámbito geográfico
        if not index_tipo_nombre_exists:
            Index('ix_ambito_geografico_tipo_nombre', 
                  AmbitoGeografico.tipo, 
                  AmbitoGeografico.nombre).create(engine)
            logger.info("Índice compuesto por tipo y nombre creado correctamente")
        else:
            logger.info("El índice compuesto por tipo y nombre ya existe")
        
        return True
    except Exception as e:
        logger.error(f"Error al crear índices: {e}")
        return False

def poblar_terminos_lenguas(engine):
    """
    Pobla la tabla de mapeo de términos en diferentes lenguas
    """
    logger.info("Poblando tabla de mapeo de términos en diferentes lenguas...")
    
    # Definir mapeos de términos en diferentes lenguas
    mapeos = [
        # Español
        {"termino": "AYUNTAMIENTO DE", "lengua": "es", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "AYTO. DE", "lengua": "es", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "AYTO DE", "lengua": "es", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "AYTO.", "lengua": "es", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "AYTO", "lengua": "es", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "MANCOMUNIDAD DE", "lengua": "es", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "MANCOMUNIDAD", "lengua": "es", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "DIPUTACIÓN DE", "lengua": "es", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DIPUTACION DE", "lengua": "es", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DIPUTACIÓN", "lengua": "es", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DIPUTACION", "lengua": "es", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "CABILDO DE", "lengua": "es", "termino_normalizado": "cabildo", "tipo_entidad": "cabildo"},
        {"termino": "CABILDO", "lengua": "es", "termino_normalizado": "cabildo", "tipo_entidad": "cabildo"},
        {"termino": "CONSEJO INSULAR DE", "lengua": "es", "termino_normalizado": "consejo_insular", "tipo_entidad": "consejo_insular"},
        {"termino": "CONSEJO INSULAR", "lengua": "es", "termino_normalizado": "consejo_insular", "tipo_entidad": "consejo_insular"},
        
        # Catalán/Valenciano
        {"termino": "AJUNTAMENT DE", "lengua": "ca", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "AJUNTAMENT D'", "lengua": "ca", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "AJUNTAMENT", "lengua": "ca", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "MANCOMUNITAT DE", "lengua": "ca", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "MANCOMUNITAT", "lengua": "ca", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "DIPUTACIÓ DE", "lengua": "ca", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DIPUTACIO DE", "lengua": "ca", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DIPUTACIÓ", "lengua": "ca", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DIPUTACIO", "lengua": "ca", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "CONSELL INSULAR DE", "lengua": "ca", "termino_normalizado": "consejo_insular", "tipo_entidad": "consejo_insular"},
        {"termino": "CONSELL INSULAR", "lengua": "ca", "termino_normalizado": "consejo_insular", "tipo_entidad": "consejo_insular"},
        
        # Euskera
        {"termino": "UDALA", "lengua": "eu", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "UDALETXEA", "lengua": "eu", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "MANKOMUNITATEA", "lengua": "eu", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "FORU ALDUNDIA", "lengua": "eu", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        
        # Gallego
        {"termino": "CONCELLO DE", "lengua": "gl", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "CONCELLO", "lengua": "gl", "termino_normalizado": "ayuntamiento", "tipo_entidad": "ayuntamiento"},
        {"termino": "MANCOMUNIDADE DE", "lengua": "gl", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "MANCOMUNIDADE", "lengua": "gl", "termino_normalizado": "mancomunidad", "tipo_entidad": "mancomunidad"},
        {"termino": "DEPUTACIÓN DE", "lengua": "gl", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DEPUTACION DE", "lengua": "gl", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DEPUTACIÓN", "lengua": "gl", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"},
        {"termino": "DEPUTACION", "lengua": "gl", "termino_normalizado": "diputacion", "tipo_entidad": "diputacion"}
    ]
    
    try:
        # Insertar los mapeos en la tabla
        with engine.connect() as conn:
            # Primero eliminar registros existentes
            conn.execute(text("DELETE FROM mapeo_terminos_lenguas"))
            
            # Insertar nuevos registros
            for mapeo in mapeos:
                conn.execute(
                    text(
                        "INSERT INTO mapeo_terminos_lenguas (termino, lengua, termino_normalizado, tipo_entidad) "
                        "VALUES (:termino, :lengua, :termino_normalizado, :tipo_entidad)"
                    ),
                    mapeo
                )
            
            conn.commit()
        
        logger.info(f"Se han insertado {len(mapeos)} términos en la tabla")
        return True
    except Exception as e:
        logger.error(f"Error al poblar la tabla de mapeo de términos: {e}")
        return False

def leer_excel_con_cabeceras_correctas(ruta_excel, fila_cabecera=0, fila_datos=1):
    """
    Lee un archivo Excel asignando nombres de columnas desde la fila correcta.
    """
    try:
        # Leer el archivo Excel sin cabeceras
        df_raw = pd.read_excel(ruta_excel, header=None)
        
        # Obtener la fila de cabeceras
        cabeceras = df_raw.iloc[fila_cabecera].tolist()
        
        # Limpiar nombres de cabeceras (eliminar espacios, saltos de línea, etc.)
        cabeceras_limpias = [str(col).strip().replace('\n', ' ') if pd.notna(col) else f"Col_{i}" 
                            for i, col in enumerate(cabeceras)]
        
        # Crear un nuevo DataFrame con las cabeceras correctas
        df = pd.read_excel(ruta_excel, skiprows=fila_datos)
        df.columns = cabeceras_limpias
        
        return df
    except Exception as e:
        logger.error(f"Error al leer Excel con cabeceras correctas: {e}")
        return None

def poblar_ambito_geografico_espana(session):
    """
    Pobla el ámbito geográfico de España como nodo raíz
    """
    logger.info("Poblando ámbito geográfico de España...")
    
    try:
        # Verificar si ya existe
        espana = session.query(AmbitoGeografico).filter_by(codigo='ES').first()
        
        if not espana:
            # Crear España como nodo raíz
            espana = AmbitoGeografico(
                codigo='ES',
                tipo='NA',
                nombre='España',
                codigo_ambito_superior=None,
                nivel_jerarquico=1,
                codigo_iso='ES',
                es_capital=False,
                estado='activo'
            )
            
            session.add(espana)
            session.commit()
            logger.info("Ámbito geográfico de España creado correctamente")
        else:
            logger.info("El ámbito geográfico de España ya existe")
        
        return espana
    except Exception as e:
        session.rollback()
        logger.error(f"Error al poblar el ámbito geográfico de España: {e}")
        return None

def poblar_ambitos_geograficos_ccaa_provincias(session):
    """
    Pobla los ámbitos geográficos de comunidades autónomas y provincias desde Excel
    """
    logger.info("Poblando ámbitos geográficos de comunidades autónomas y provincias...")
    
    try:
        # Cargar el Excel de provincias con cabeceras correctas
        df_provincias = leer_excel_con_cabeceras_correctas(PROVINCIAS_EXCEL, fila_cabecera=0, fila_datos=2)
        
        if df_provincias is None:
            logger.error("No se pudo cargar el Excel de provincias")
            return False
        
        # Renombrar columnas para facilitar el acceso
        df_provincias.columns = ['indice', 'codigo_ine', 'nombre_provincia', 'codigo_ca']
        
        # Obtener España como nodo raíz
        espana = session.query(AmbitoGeografico).filter_by(codigo='ES').first()
        
        if not espana:
            logger.error("No se encontró el ámbito geográfico de España")
            return False
        
        # Procesar comunidades autónomas y provincias
        comunidades_autonomas = {}
        provincias_creadas = 0
        
        # Crear diccionario de comunidades autónomas
        ca_dict = {
            1: "Andalucía",
            2: "Aragón",
            3: "Asturias, Principado de",
            4: "Balears, Illes",
            5: "Canarias",
            6: "Cantabria",
            7: "Castilla y León",
            8: "Castilla - La Mancha",
             9: "Cataluña",
            10: "Comunitat Valenciana",
            11: "Extremadura",
            12: "Galicia",
            13: "Madrid, Comunidad de",
            14: "Murcia, Región de",
            15: "Navarra, Comunidad Foral de",
            16: "País Vasco",
            17: "Rioja, La",
            18: "Ceuta",
            19: "Melilla"
        }
        
        # Iterar sobre las filas del Excel
        for _, row in df_provincias.iterrows():
            if pd.isna(row['codigo_ca']) or pd.isna(row['codigo_ine']):
                continue
                
            codigo_ca_num = int(row['codigo_ca'])
            codigo_ca = f"CA{codigo_ca_num:02d}"
            nombre_ca = ca_dict.get(codigo_ca_num, f"Comunidad Autónoma {codigo_ca_num}")
            
            codigo_provincia = f"PR{int(row['codigo_ine']):02d}"
            nombre_provincia = row['nombre_provincia'].strip()
            
            # Crear comunidad autónoma si no existe
            if codigo_ca not in comunidades_autonomas:
                # Verificar si ya existe en la base de datos
                ca = session.query(AmbitoGeografico).filter_by(codigo=codigo_ca).first()
                
                if not ca:
                    ca = AmbitoGeografico(
                        codigo=codigo_ca,
                        tipo='CA',
                        nombre=nombre_ca,
                        codigo_ambito_superior='ES',
                        nivel_jerarquico=2,
                        es_capital=False,
                        estado='activo'
                    )
                    
                    session.add(ca)
                    comunidades_autonomas[codigo_ca] = ca
                    logger.debug(f"Comunidad autónoma creada: {nombre_ca} ({codigo_ca})")
                else:
                    comunidades_autonomas[codigo_ca] = ca
            
            # Crear provincia
            provincia = session.query(AmbitoGeografico).filter_by(codigo=codigo_provincia).first()
            
            if not provincia:
                provincia = AmbitoGeografico(
                    codigo=codigo_provincia,
                    tipo='PR',
                    nombre=nombre_provincia,
                    codigo_ambito_superior=codigo_ca,
                    nivel_jerarquico=3,
                    es_capital=False,
                    estado='activo'
                )
                
                session.add(provincia)
                provincias_creadas += 1
                logger.debug(f"Provincia creada: {nombre_provincia} ({codigo_provincia})")
        
        session.commit()
        logger.info(f"Se han creado/verificado {len(comunidades_autonomas)} comunidades autónomas y {provincias_creadas} provincias")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error al poblar ámbitos geográficos de comunidades autónomas y provincias: {e}")
        return False

def poblar_ambitos_geograficos_municipios(session):
    """
    Pobla los ámbitos geográficos de municipios desde Excel
    """
    logger.info("Poblando ámbitos geográficos de municipios...")
    
    try:
        # Cargar el Excel de unidades EELL con cabeceras correctas
        df_unidades = leer_excel_con_cabeceras_correctas(UNIDADES_EELL_EXCEL, fila_cabecera=0, fila_datos=1)
        
        if df_unidades is None:
            logger.error("No se pudo cargar el Excel de unidades EELL")
            return False
        
        # Renombrar columnas para facilitar el acceso
        df_unidades.columns = [
            'indice', 'codigo_dir3', 'denominacion', 'nivel_administracion', 'codigo_tipo_ente',
            'nivel_jerarquico', 'codigo_provincia', 'provincia', 'codigo_unidad_superior',
            'denominacion_unidad_superior', 'codigo_unidad_principal', 'denominacion_unidad_principal',
            'dep_edp_principal', 'codigo_edp_principal', 'denominacion_edp_principal',
            'estado', 'fecha_alta', 'nif_cif', 'contactos'
        ]
        
        # Filtrar solo ayuntamientos (código 01)
        df_ayuntamientos = df_unidades[df_unidades['codigo_tipo_ente'] == 'AY']
        
        # Obtener todas las provincias
        provincias = {p.codigo: p for p in session.query(AmbitoGeografico).filter_by(tipo='PR').all()}
        
        if not provincias:
            logger.error("No se encontraron provincias en la base de datos")
            return False
        
        # Crear diccionario de mapeo de nombres de provincia a códigos
        provincia_a_codigo = {}
        for codigo, provincia in provincias.items():
            nombre_limpio = provincia.nombre.strip().lower()
            provincia_a_codigo[nombre_limpio] = codigo
        
        # Procesar municipios
        municipios_creados = 0
        municipios_existentes = 0
        municipios_sin_provincia = 0
        
        # Iterar sobre las filas de ayuntamientos
        for _, row in df_ayuntamientos.iterrows():
            codigo_municipio = f"LO{row['codigo_dir3']}"
            nombre_municipio = f"Ayuntamiento de {row['denominacion']}"
            
            # Obtener código de provincia
            codigo_provincia = None
            if pd.notna(row['provincia']):
                nombre_provincia = row['provincia'].strip().lower()
                codigo_provincia = provincia_a_codigo.get(nombre_provincia)
            
            if not codigo_provincia and pd.notna(row['codigo_provincia']):
                # Intentar por código numérico
                try:
                    codigo_num = int(row['codigo_provincia'])
                    codigo_provincia = f"PR{codigo_num:02d}"
                    if codigo_provincia not in provincias:
                        codigo_provincia = None
                except (ValueError, TypeError):
                    codigo_provincia = None
            
            if not codigo_provincia:
                municipios_sin_provincia += 1
                logger.warning(f"No se encontró la provincia para el municipio: {nombre_municipio}")
                continue
            
            # Verificar si ya existe
            municipio = session.query(AmbitoGeografico).filter_by(codigo=codigo_municipio).first()
            
            if not municipio:
                municipio = AmbitoGeografico(
                    codigo=codigo_municipio,
                    tipo='LO',
                    nombre=nombre_municipio,
                    codigo_ambito_superior=codigo_provincia,
                    nivel_jerarquico=4,
                    es_capital=False,
                    estado='activo'
                )
                
                session.add(municipio)
                municipios_creados += 1
                
                # Commit cada 100 municipios para evitar transacciones muy largas
                if municipios_creados % 100 == 0:
                    session.commit()
                    logger.debug(f"Progreso: {municipios_creados} municipios creados")
            else:
                municipios_existentes += 1
        
        session.commit()
        logger.info(f"Se han creado {municipios_creados} municipios, {municipios_existentes} ya existían y {municipios_sin_provincia} no tienen provincia asignada")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error al poblar ámbitos geográficos de municipios: {e}")
        return False

def poblar_tipos_organo_administrativo(session):
    """
    Pobla los tipos de órgano administrativo desde Excel
    """
    logger.info("Poblando tipos de órgano administrativo...")
    
    try:
        # Cargar el Excel de tipos de entidad pública con cabeceras correctas
        df_tipos = leer_excel_con_cabeceras_correctas(TIPOS_ENTIDAD_EXCEL, fila_cabecera=0, fila_datos=2)
        
        if df_tipos is None:
            logger.error("No se pudo cargar el Excel de tipos de entidad pública")
            return False
        
        # Renombrar columnas para facilitar el acceso
        df_tipos.columns = ['indice', 'codigo', 'descripcion']
        
        # Procesar tipos de órgano
        tipos_creados = 0
        
        # Iterar sobre las filas del Excel
        for _, row in df_tipos.iterrows():
            if pd.isna(row['codigo']) or pd.isna(row['descripcion']):
                continue
                
            codigo_tipo = f"T{row['codigo']}"
            descripcion = row['descripcion'].strip()
            
            # Determinar nivel de administración
            if 'ESTADO' in descripcion.upper() or 'CENTRAL' in descripcion.upper():
                nivel_administracion = 'C'
            elif 'AUTONÓM' in descripcion.upper() or 'AUTONOM' in descripcion.upper():
                nivel_administracion = 'A'
            elif 'LOCAL' in descripcion.upper() or 'MUNICIPAL' in descripcion.upper():
                nivel_administracion = 'L'
            else:
                nivel_administracion = 'O'
            
            # Verificar si ya existe
            tipo_organo = session.query(TipoOrganoAdministrativo).filter_by(codigo=codigo_tipo).first()
            
            if not tipo_organo:
                tipo_organo = TipoOrganoAdministrativo(
                    codigo=codigo_tipo,
                    descripcion=descripcion,
                    nivel_administracion=nivel_administracion
                )
                
                session.add(tipo_organo)
                tipos_creados += 1
        
        session.commit()
        logger.info(f"Se han creado {tipos_creados} tipos de órgano administrativo")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error al poblar tipos de órgano administrativo: {e}")
        return False

def poblar_organos_administrativos(session):
    """
    Pobla los órganos administrativos desde Excel
    """
    logger.info("Poblando órganos administrativos...")
    
    try:
        # Cargar el Excel de unidades EELL con cabeceras correctas
        df_unidades = leer_excel_con_cabeceras_correctas(UNIDADES_EELL_EXCEL, fila_cabecera=0, fila_datos=1)
        
        if df_unidades is None:
            logger.error("No se pudo cargar el Excel de unidades EELL")
            return False
        
        # Renombrar columnas para facilitar el acceso
        df_unidades.columns = [
            'indice', 'codigo_dir3', 'denominacion', 'nivel_administracion', 'codigo_tipo_ente',
            'nivel_jerarquico', 'codigo_provincia', 'provincia', 'codigo_unidad_superior',
            'denominacion_unidad_superior', 'codigo_unidad_principal', 'denominacion_unidad_principal',
            'dep_edp_principal', 'codigo_edp_principal', 'denominacion_edp_principal',
            'estado', 'fecha_alta', 'nif_cif', 'contactos'
        ]
        
        # Obtener todos los ámbitos geográficos
        ambitos_por_codigo = {a.codigo: a for a in session.query(AmbitoGeografico).all()}
        ambitos_por_nombre = {a.nombre: a for a in session.query(AmbitoGeografico).all()}
        
        # Obtener todos los tipos de órgano
        tipos_organo = {t.codigo: t for t in session.query(TipoOrganoAdministrativo).all()}
        
        # Crear diccionario de mapeo de códigos de tipo de ente a códigos de tipo de órgano
        tipo_ente_a_tipo_organo = {
            'AY': 'TAY',  # Ayuntamiento
            'DI': 'TDI',  # Diputación
            'CA': 'TCA',  # Cabildo/Consejo Insular
            'MA': 'TMA',  # Mancomunidad
            'CM': 'TCM',  # Comarca
            'AG': 'TAG',  # Agrupación
            'CS': 'TCS'   # Consorcio
        }
        
        # Procesar órganos administrativos
        organos_creados = 0
        organos_existentes = 0
        organos_sin_ambito = 0
        
        # Iterar sobre las filas del Excel
        for _, row in df_unidades.iterrows():
            if pd.isna(row['codigo_dir3']) or pd.isna(row['denominacion']):
                continue
                
            codigo_organo = row['codigo_dir3']
            denominacion = row['denominacion'].strip()
            
            # Determinar nivel1, nivel2, nivel3
            nivel1 = denominacion
            nivel2 = None
            nivel3 = None
            
            # Si es un ayuntamiento, extraer el nombre del municipio
            if row['codigo_tipo_ente'] == 'AY':
                nivel1 = denominacion
                nivel2 = f"Ayuntamiento de {denominacion}"
            
            # Determinar tipo de administración
            tipo_administracion = str(row['nivel_administracion']) if pd.notna(row['nivel_administracion']) else '3'
            
            # Determinar código de tipo de órgano
            codigo_tipo_ente = row['codigo_tipo_ente'] if pd.notna(row['codigo_tipo_ente']) else 'AY'
            codigo_tipo_organo = tipo_ente_a_tipo_organo.get(codigo_tipo_ente, 'TAY')
            
            # Determinar ámbito geográfico
            ambito_geografico_codigo = None
            
            # Buscar por nombre de municipio
            nombre_municipio = f"Ayuntamiento de {denominacion}"
            if nombre_municipio in ambitos_por_nombre:
                ambito_geografico_codigo = ambitos_por_nombre[nombre_municipio].codigo
            
            # Si no se encuentra, buscar por provincia
            if not ambito_geografico_codigo and pd.notna(row['codigo_provincia']):
                try:
                    codigo_provincia = f"PR{int(row['codigo_provincia']):02d}"
                    if codigo_provincia in ambitos_por_codigo:
                        ambito_geografico_codigo = codigo_provincia
                except (ValueError, TypeError):
                    pass
            
            # Si no se encuentra, usar España como fallback
            if not ambito_geografico_codigo:
                ambito_geografico_codigo = 'ES'
                organos_sin_ambito += 1
            
            # Verificar si ya existe
            organo = session.query(OrganoAdministrativo).filter_by(codigo=codigo_organo).first()
            
            if not organo:
                # Verificar si el tipo de órgano existe
                if codigo_tipo_organo not in tipos_organo:
                    # Crear tipo de órgano si no existe
                    tipo_organo = TipoOrganoAdministrativo(
                        codigo=codigo_tipo_organo,
                        descripcion=f"Tipo de órgano {codigo_tipo_ente}",
                        nivel_administracion=tipo_administracion[0] if len(tipo_administracion) > 0 else 'L'
                    )
                    session.add(tipo_organo)
                    tipos_organo[codigo_tipo_organo] = tipo_organo
                
                organo = OrganoAdministrativo(
                    codigo=codigo_organo,
                    codigo_organo_superior=None,  # Se establecerá después
                    denominacion=denominacion,
                    nivel1=nivel1,
                    nivel2=nivel2,
                    nivel3=nivel3,
                    tipo_administracion=tipo_administracion,
                    codigo_tipo_organo=codigo_tipo_organo,
                    estado='activo',
                    fecha_creacion=datetime.now().date(),
                    ambito_geografico_codigo=ambito_geografico_codigo
                )
                
                session.add(organo)
                organos_creados += 1
                
                # Commit cada 100 órganos para evitar transacciones muy largas
                if organos_creados % 100 == 0:
                    session.commit()
                    logger.debug(f"Progreso: {organos_creados} órganos creados")
            else:
                organos_existentes += 1
        
        session.commit()
        logger.info(f"Se han creado {organos_creados} órganos administrativos, {organos_existentes} ya existían y {organos_sin_ambito} no tienen ámbito geográfico específico")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error al poblar órganos administrativos: {e}")
        return False

def establecer_relaciones_jerarquicas_organos(session):
    """
    Establece las relaciones jerárquicas entre órganos administrativos
    """
    logger.info("Estableciendo relaciones jerárquicas entre órganos administrativos...")
    
    try:
        # Cargar el Excel de unidades EELL con cabeceras correctas
        df_unidades = leer_excel_con_cabeceras_correctas(UNIDADES_EELL_EXCEL, fila_cabecera=0, fila_datos=1)
        
        if df_unidades is None:
            logger.error("No se pudo cargar el Excel de unidades EELL")
            return False
        
        # Renombrar columnas para facilitar el acceso
        df_unidades.columns = [
            'indice', 'codigo_dir3', 'denominacion', 'nivel_administracion', 'codigo_tipo_ente',
            'nivel_jerarquico', 'codigo_provincia', 'provincia', 'codigo_unidad_superior',
            'denominacion_unidad_superior', 'codigo_unidad_principal', 'denominacion_unidad_principal',
            'dep_edp_principal', 'codigo_edp_principal', 'denominacion_edp_principal',
            'estado', 'fecha_alta', 'nif_cif', 'contactos'
        ]
        
        # Crear un diccionario de órganos por código
        organos = {o.codigo: o for o in session.query(OrganoAdministrativo).all()}
        
        # Procesar relaciones jerárquicas
        relaciones_establecidas = 0
        
        # Iterar sobre las filas del Excel
        for _, row in df_unidades.iterrows():
            if pd.isna(row['codigo_dir3']) or pd.isna(row['codigo_unidad_superior']):
                continue
                
            codigo_organo = row['codigo_dir3']
            codigo_organo_superior = row['codigo_unidad_superior']
            
            # Verificar si ambos órganos existen
            if codigo_organo in organos and codigo_organo_superior in organos:
                organo = organos[codigo_organo]
                
                # Establecer relación jerárquica
                if organo.codigo_organo_superior != codigo_organo_superior:
                    organo.codigo_organo_superior = codigo_organo_superior
                    relaciones_establecidas += 1
        
        session.commit()
        logger.info(f"Se han establecido {relaciones_establecidas} relaciones jerárquicas entre órganos")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error al establecer relaciones jerárquicas entre órganos: {e}")
        return False

def generar_mapeos_nivel1_ca(session, engine):
    """
    Genera mapeos entre nivel1 y códigos de comunidades autónomas
    """
    logger.info("Generando mapeos entre nivel1 y códigos de comunidades autónomas...")
    
    try:
        # Obtener todas las comunidades autónomas
        comunidades_autonomas = session.query(AmbitoGeografico).filter_by(tipo='CA').all()
        
        # Mapeos básicos
        mapeos = []
        
        # Mapeos para comunidades autónomas
        for ca in comunidades_autonomas:
            # Nombre oficial
            mapeos.append({
                "nivel1": ca.nombre.upper(),
                "codigo_comunidad_autonoma": ca.codigo,
                "descripcion": f"Comunidad Autónoma: {ca.nombre}"
            })
            
            # Variantes comunes
            if "COMUNIDAD" in ca.nombre.upper():
                nombre_corto = ca.nombre.upper().replace("COMUNIDAD DE ", "").replace("COMUNIDAD ", "")
                mapeos.append({
                    "nivel1": nombre_corto,
                    "codigo_comunidad_autonoma": ca.codigo,
                    "descripcion": f"Comunidad Autónoma: {ca.nombre}"
                })
            
            if "PRINCIPADO" in ca.nombre.upper():
                nombre_corto = ca.nombre.upper().replace("PRINCIPADO DE ", "")
                mapeos.append({
                    "nivel1": nombre_corto,
                    "codigo_comunidad_autonoma": ca.codigo,
                    "descripcion": f"Comunidad Autónoma: {ca.nombre}"
                })
        
        # Mapeos específicos
        mapeos_especificos = [
            # Andalucía
            {"nivel1": "JUNTA DE ANDALUCIA", "codigo_comunidad_autonoma": "CA01", "descripcion": "Comunidad Autónoma: Andalucía"},
            
            # Aragón
            {"nivel1": "GOBIERNO DE ARAGON", "codigo_comunidad_autonoma": "CA02", "descripcion": "Comunidad Autónoma: Aragón"},
            
            # Cataluña
            {"nivel1": "CATALUNYA", "codigo_comunidad_autonoma": "CA09", "descripcion": "Comunidad Autónoma: Cataluña"},
            {"nivel1": "GENERALITAT DE CATALUNYA", "codigo_comunidad_autonoma": "CA09", "descripcion": "Comunidad Autónoma: Cataluña"},
            
            # Comunitat Valenciana
            {"nivel1": "COMUNIDAD VALENCIANA", "codigo_comunidad_autonoma": "CA10", "descripcion": "Comunidad Autónoma: Comunitat Valenciana"},
            {"nivel1": "VALENCIA", "codigo_comunidad_autonoma": "CA10", "descripcion": "Comunidad Autónoma: Comunitat Valenciana"},
            {"nivel1": "C. VALENCIANA", "codigo_comunidad_autonoma": "CA10", "descripcion": "Comunidad Autónoma: Comunitat Valenciana"},
            {"nivel1": "GENERALITAT VALENCIANA", "codigo_comunidad_autonoma": "CA10", "descripcion": "Comunidad Autónoma: Comunitat Valenciana"},
            
            # País Vasco
            {"nivel1": "PAIS VASCO", "codigo_comunidad_autonoma": "CA16", "descripcion": "Comunidad Autónoma: País Vasco"},
            {"nivel1": "EUSKADI", "codigo_comunidad_autonoma": "CA16", "descripcion": "Comunidad Autónoma: País Vasco"},
            {"nivel1": "GOBIERNO VASCO", "codigo_comunidad_autonoma": "CA16", "descripcion": "Comunidad Autónoma: País Vasco"},
            
            # Galicia
            {"nivel1": "XUNTA DE GALICIA", "codigo_comunidad_autonoma": "CA12", "descripcion": "Comunidad Autónoma: Galicia"},
            
            # Estado
            {"nivel1": "ESTADO", "codigo_comunidad_autonoma": "ES", "descripcion": "España"},
            {"nivel1": "ADMINISTRACION GENERAL DEL ESTADO", "codigo_comunidad_autonoma": "ES", "descripcion": "España"},
            {"nivel1": "ADMINISTRACIÓN GENERAL DEL ESTADO", "codigo_comunidad_autonoma": "ES", "descripcion": "España"},
            {"nivel1": "ESPAÑA", "codigo_comunidad_autonoma": "ES", "descripcion": "España"},
            {"nivel1": "SPAIN", "codigo_comunidad_autonoma": "ES", "descripcion": "España"},
            {"nivel1": "GOBIERNO DE ESPAÑA", "codigo_comunidad_autonoma": "ES", "descripcion": "España"}
        ]
        
        mapeos.extend(mapeos_especificos)
        
        # Insertar los mapeos en la tabla
        with engine.connect() as conn:
            # Primero eliminar registros existentes
            conn.execute(text("DELETE FROM mapeo_nivel1_comunidad_autonoma"))
            
            # Insertar nuevos registros
            for mapeo in mapeos:
                conn.execute(
                    text(
                        "INSERT INTO mapeo_nivel1_comunidad_autonoma (nivel1, codigo_comunidad_autonoma, descripcion) "
                        "VALUES (:nivel1, :codigo_comunidad_autonoma, :descripcion)"
                    ),
                    mapeo
                )
            
            conn.commit()
        
        logger.info(f"Se han generado {len(mapeos)} mapeos entre nivel1 y códigos de comunidades autónomas")
        return True
    except Exception as e:
        logger.error(f"Error al generar mapeos entre nivel1 y códigos de comunidades autónomas: {e}")
        return False

def generar_mapeos_municipios(session, engine):
    """
    Genera mapeos para municipios respetando idiomas cooficiales por territorio
    """
    logger.info("Generando mapeos para municipios respetando idiomas cooficiales...")
    
    try:
        # Obtener municipios con su territorio
        municipios_con_territorio = []
        
        # Obtener todos los municipios
        municipios = session.query(AmbitoGeografico).filter_by(tipo='LO').all()
        logger.info(f"Se han encontrado {len(municipios)} municipios")
        
        for municipio in municipios:
            # Obtener provincia y comunidad autónoma
            provincia = None
            comunidad_autonoma = None
            
            if municipio.codigo_ambito_superior:
                provincia = session.query(AmbitoGeografico).filter_by(codigo=municipio.codigo_ambito_superior).first()
                
                if provincia and provincia.codigo_ambito_superior:
                    comunidad_autonoma = session.query(AmbitoGeografico).filter_by(codigo=provincia.codigo_ambito_superior).first()
            
            if comunidad_autonoma:
                municipios_con_territorio.append({
                    'municipio': municipio,
                    'provincia': provincia,
                    'comunidad_autonoma': comunidad_autonoma
                })
        
        logger.info(f"Se han procesado {len(municipios_con_territorio)} municipios con territorio")
        
        # Preparar mapeos para entidades locales
        mapeos = []
        
        for item in municipios_con_territorio:
            municipio = item['municipio']
            provincia = item['provincia']
            comunidad_autonoma = item['comunidad_autonoma']
            
            # Extraer nombre puro del municipio (sin "Ayuntamiento de ")
            nombre_municipio = municipio.nombre
            if nombre_municipio.startswith("Ayuntamiento de "):
                nombre_municipio = nombre_municipio[15:]
            
            # Código de comunidad autónoma
            codigo_ca = comunidad_autonoma.codigo
            
            # Mapeo básico en castellano (para todos los municipios)
            mapeos.append({
                "nivel1": nombre_municipio.upper(),
                "codigo_comunidad_autonoma": codigo_ca,
                "descripcion": f"Municipio en {comunidad_autonoma.nombre}"
            })
            
            mapeos.append({
                "nivel1": f"AYUNTAMIENTO DE {nombre_municipio.upper()}",
                "codigo_comunidad_autonoma": codigo_ca,
                "descripcion": f"Municipio en {comunidad_autonoma.nombre}"
            })
            
            # Mapeos en catalán (solo para territorios catalanoparlantes)
            if codigo_ca in TERRITORIOS_CATALAN:
                mapeos.append({
                    "nivel1": f"AJUNTAMENT DE {nombre_municipio.upper()}",
                    "codigo_comunidad_autonoma": codigo_ca,
                    "descripcion": f"Municipio en {comunidad_autonoma.nombre} (catalán)"
                })
                
                # Caso especial para nombres que empiezan por vocal
                if nombre_municipio and len(nombre_municipio) > 0 and nombre_municipio[0].lower() in 'aeiouàèéíòóú':
                    mapeos.append({
                        "nivel1": f"AJUNTAMENT D'{nombre_municipio.upper()}",
                        "codigo_comunidad_autonoma": codigo_ca,
                        "descripcion": f"Municipio en {comunidad_autonoma.nombre} (catalán)"
                    })
            
            # Mapeos en euskera (solo para territorios vascoparlantes)
            if codigo_ca in TERRITORIOS_EUSKERA:
                mapeos.append({
                    "nivel1": f"{nombre_municipio.upper()} UDALA",
                    "codigo_comunidad_autonoma": codigo_ca,
                    "descripcion": f"Municipio en {comunidad_autonoma.nombre} (euskera)"
                })
                
                mapeos.append({
                    "nivel1": f"{nombre_municipio.upper()}KO UDALA",
                    "codigo_comunidad_autonoma": codigo_ca,
                    "descripcion": f"Municipio en {comunidad_autonoma.nombre} (euskera)"
                })
            
            # Mapeos en gallego (solo para territorios gallegoparlantes)
            if codigo_ca in TERRITORIOS_GALLEGO:
                mapeos.append({
                    "nivel1": f"CONCELLO DE {nombre_municipio.upper()}",
                    "codigo_comunidad_autonoma": codigo_ca,
                    "descripcion": f"Municipio en {comunidad_autonoma.nombre} (gallego)"
                })
        
        # Insertar los mapeos en la tabla
        with engine.connect() as conn:
            # Insertar nuevos registros
            for mapeo in mapeos:
                # Verificar si ya existe
                result = conn.execute(
                    text("SELECT nivel1 FROM mapeo_nivel1_comunidad_autonoma WHERE nivel1 = :nivel1"),
                    {"nivel1": mapeo["nivel1"]}
                )
                
                if result.fetchone() is None:
                    conn.execute(
                        text(
                            "INSERT INTO mapeo_nivel1_comunidad_autonoma (nivel1, codigo_comunidad_autonoma, descripcion) "
                            "VALUES (:nivel1, :codigo_comunidad_autonoma, :descripcion)"
                        ),
                        mapeo
                    )
            
            conn.commit()
        
        logger.info(f"Se han generado {len(mapeos)} mapeos para municipios")
        return True
    except Exception as e:
        logger.error(f"Error al generar mapeos para municipios: {e}")
        return False

def generar_estadisticas(session, engine):
    """
    Genera estadísticas de poblamiento
    """
    logger.info("Generando estadísticas de poblamiento...")
    
    try:
        # Estadísticas de ámbitos geográficos
        ambitos_por_tipo = {}
        for tipo in ['NA', 'CA', 'PR', 'LO']:
            count = session.query(AmbitoGeografico).filter_by(tipo=tipo).count()
            ambitos_por_tipo[tipo] = count
        
        # Estadísticas de órganos administrativos
        organos_por_tipo_admin = {}
        for tipo in ['1', '2', '3']:
            count = session.query(OrganoAdministrativo).filter_by(tipo_administracion=tipo).count()
            organos_por_tipo_admin[tipo] = count
        
        # Estadísticas de mapeos
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma"))
            total_mapeos = result.fetchone()[0]
            
            result = conn.execute(text(
                "SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma WHERE descripcion LIKE 'Comunidad%'"
            ))
            mapeos_ca = result.fetchone()[0]
            
            result = conn.execute(text(
                "SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma WHERE descripcion LIKE 'Municipio%'"
            ))
            mapeos_municipios = result.fetchone()[0]
            
            result = conn.execute(text(
                "SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma WHERE descripcion LIKE '%catalán%'"
            ))
            mapeos_catalan = result.fetchone()[0]
            
            result = conn.execute(text(
                "SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma WHERE descripcion LIKE '%euskera%'"
            ))
            mapeos_euskera = result.fetchone()[0]
            
            result = conn.execute(text(
                "SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma WHERE descripcion LIKE '%gallego%'"
            ))
            mapeos_gallego = result.fetchone()[0]
        
        # Crear diccionario de estadísticas
        estadisticas = {
            "ambitos_geograficos": {
                "total": sum(ambitos_por_tipo.values()),
                "por_tipo": {
                    "NA": ambitos_por_tipo.get('NA', 0),
                    "CA": ambitos_por_tipo.get('CA', 0),
                    "PR": ambitos_por_tipo.get('PR', 0),
                    "LO": ambitos_por_tipo.get('LO', 0)
                }
            },
            "organos_administrativos": {
                "total": sum(organos_por_tipo_admin.values()),
                "por_tipo_administracion": {
                    "1_Estado": organos_por_tipo_admin.get('1', 0),
                    "2_Autonomica": organos_por_tipo_admin.get('2', 0),
                    "3_Local": organos_por_tipo_admin.get('3', 0)
                }
            },
            "mapeos": {
                "total": total_mapeos,
                "comunidades_autonomas": mapeos_ca,
                "municipios": mapeos_municipios,
                "por_idioma": {
                    "castellano": mapeos_municipios - mapeos_catalan - mapeos_euskera - mapeos_gallego,
                    "catalan": mapeos_catalan,
                    "euskera": mapeos_euskera,
                    "gallego": mapeos_gallego
                }
            }
        }
        
        # Guardar estadísticas en un archivo JSON
        with open('estadisticas_poblamiento_ajustado.json', 'w', encoding='utf-8') as f:
            json.dump(estadisticas, f, ensure_ascii=False, indent=2)
        
        # Mostrar estadísticas
        print("\nEstadísticas de poblamiento:")
        print(f"Ámbitos geográficos: {estadisticas['ambitos_geograficos']['total']}")
        print(f"- Países: {estadisticas['ambitos_geograficos']['por_tipo']['NA']}")
        print(f"- Comunidades Autónomas: {estadisticas['ambitos_geograficos']['por_tipo']['CA']}")
        print(f"- Provincias: {estadisticas['ambitos_geograficos']['por_tipo']['PR']}")
        print(f"- Municipios: {estadisticas['ambitos_geograficos']['por_tipo']['LO']}")
        print(f"\nÓrganos administrativos: {estadisticas['organos_administrativos']['total']}")
        print(f"- Estado: {estadisticas['organos_administrativos']['por_tipo_administracion']['1_Estado']}")
        print(f"- Autonómica: {estadisticas['organos_administrativos']['por_tipo_administracion']['2_Autonomica']}")
        print(f"- Local: {estadisticas['organos_administrativos']['por_tipo_administracion']['3_Local']}")
        print(f"\nMapeos: {estadisticas['mapeos']['total']}")
        print(f"- Comunidades Autónomas: {estadisticas['mapeos']['comunidades_autonomas']}")
        print(f"- Municipios: {estadisticas['mapeos']['municipios']}")
        print(f"  - Castellano: {estadisticas['mapeos']['por_idioma']['castellano']}")
        print(f"  - Catalán: {estadisticas['mapeos']['por_idioma']['catalan']}")
        print(f"  - Euskera: {estadisticas['mapeos']['por_idioma']['euskera']}")
        print(f"  - Gallego: {estadisticas['mapeos']['por_idioma']['gallego']}")
        
        logger.info("Estadísticas de poblamiento generadas correctamente")
        return estadisticas
    except Exception as e:
        logger.error(f"Error al generar estadísticas de poblamiento: {e}")
        return None

def probar_busqueda_por_niveles(session):
    """
    Prueba la búsqueda de órganos por niveles jerárquicos
    """
    logger.info("Probando búsqueda de órganos por niveles jerárquicos...")
    
    try:
        # Ejemplos de búsqueda
        ejemplos = [
            {"nivel1": "MADRID", "nivel2": "AYUNTAMIENTO DE MADRID", "nivel3": None},
            {"nivel1": "BARCELONA", "nivel2": "AJUNTAMENT DE BARCELONA", "nivel3": None},
            {"nivel1": "BILBAO", "nivel2": "BILBOKO UDALA", "nivel3": None},
            {"nivel1": "VIGO", "nivel2": "CONCELLO DE VIGO", "nivel3": None}
        ]
        
        resultados = {}
        
        for ejemplo in ejemplos:
            nivel1 = ejemplo.get("nivel1")
            nivel2 = ejemplo.get("nivel2")
            nivel3 = ejemplo.get("nivel3")
            
            # Construir consulta
            query = session.query(OrganoAdministrativo)
            
            if nivel1:
                query = query.filter(OrganoAdministrativo.nivel1 == nivel1)
            
            if nivel2:
                query = query.filter(OrganoAdministrativo.nivel2 == nivel2)
            
            if nivel3:
                query = query.filter(OrganoAdministrativo.nivel3 == nivel3)
            
            # Ejecutar consulta
            organos = query.all()
            
            # Guardar resultados
            clave = f"{nivel1} / {nivel2}"
            if nivel3:
                clave += f" / {nivel3}"
            
            resultados[clave] = [
                {
                    "codigo": o.codigo,
                    "denominacion": o.denominacion,
                    "ambito_geografico": o.ambito_geografico_codigo
                }
                for o in organos
            ]
        
        # Guardar resultados en un archivo JSON
        with open('resultados_busqueda_niveles_ajustado.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        
        # Mostrar resultados
        print("\nResultados de búsqueda por niveles jerárquicos:")
        for clave, organos in resultados.items():
            print(f"'{clave}': {len(organos)} órganos encontrados")
        
        logger.info("Prueba de búsqueda por niveles jerárquicos completada")
        return resultados
    except Exception as e:
        logger.error(f"Error al probar búsqueda por niveles jerárquicos: {e}")
        return None

def main():
    """
    Función principal
    """
    logger.info("Iniciando poblamiento unificado de ámbitos geográficos y órganos administrativos...")
    
    try:
        # Crear conexión a la base de datos
        engine, session = crear_conexion()
        
        # Inicializar base de datos
        inicializar_bd(engine)
        
        # Crear índices
        crear_indices(engine)
        
        # Poblar tabla de mapeo de términos
        poblar_terminos_lenguas(engine)
        
        # Poblar ámbitos geográficos
        poblar_ambito_geografico_espana(session)
        poblar_ambitos_geograficos_ccaa_provincias(session)
        poblar_ambitos_geograficos_municipios(session)
        
        # Poblar órganos administrativos
        poblar_tipos_organo_administrativo(session)
        poblar_organos_administrativos(session)
        establecer_relaciones_jerarquicas_organos(session)
        
        # Generar mapeos
        generar_mapeos_nivel1_ca(session, engine)
        generar_mapeos_municipios(session, engine)
        
        # Generar estadísticas
        generar_estadisticas(session, engine)
        
        # Probar búsqueda por niveles
        probar_busqueda_por_niveles(session)
        
        logger.info("Poblamiento unificado completado con éxito")
        return True
    except Exception as e:
        logger.error(f"Error en el proceso de poblamiento unificado: {e}")
        return False

if __name__ == "__main__":
    main()
 