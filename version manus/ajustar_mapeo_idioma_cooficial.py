#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para ajustar la lógica de mapeo de idiomas cooficiales por territorio
"""

import os
import sys
import logging
import json
import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Importar modelos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.db.models import Base, AmbitoGeografico

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ajustar_mapeo_idioma")

# Configuración de la base de datos
DB_URL = os.environ.get('DB_URL', 'postgresql://postgres:postgres@localhost/bdnsdb')

# Definir territorios con idiomas cooficiales
TERRITORIOS_CATALAN = ['CA09', 'CA10', 'CA04']  # Cataluña, C. Valenciana, Baleares
TERRITORIOS_EUSKERA = ['CA16', 'CA15']          # País Vasco, Navarra
TERRITORIOS_GALLEGO = ['CA12']                  # Galicia
TERRITORIOS_ARANES = ['CA09']                   # Valle de Arán (Cataluña)

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

def limpiar_tabla_mapeo_nivel1(engine):
    """
    Limpia la tabla de mapeo nivel1-comunidad autónoma para reconstruirla
    """
    logger.info("Limpiando tabla de mapeo nivel1-comunidad autónoma...")
    
    try:
        with engine.connect() as conn:
            # Preservar solo los mapeos de comunidades autónomas y estado
            conn.execute(text(
                "DELETE FROM mapeo_nivel1_comunidad_autonoma WHERE descripcion NOT LIKE 'Comunidad%' AND descripcion NOT LIKE 'España%'"
            ))
            
            # Verificar cuántos registros quedan
            result = conn.execute(text("SELECT COUNT(*) FROM mapeo_nivel1_comunidad_autonoma"))
            count = result.fetchone()[0]
            
            logger.info(f"Se han preservado {count} mapeos básicos")
            return True
    except Exception as e:
        logger.error(f"Error al limpiar la tabla de mapeo: {e}")
        return False

def obtener_municipios_con_territorio(session):
    """
    Obtiene todos los municipios con su territorio (comunidad autónoma)
    """
    logger.info("Obteniendo municipios con su territorio...")
    
    try:
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
        return municipios_con_territorio
    except Exception as e:
        logger.error(f"Error al obtener municipios con territorio: {e}")
        return []

def generar_mapeos_territoriales(engine, municipios_con_territorio):
    """
    Genera mapeos territoriales respetando los idiomas cooficiales
    """
    logger.info("Generando mapeos territoriales respetando idiomas cooficiales...")
    
    try:
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
                if re.match(r'^[aeiouàèéíòóúAEIOUÀÈÉÍÒÓÚ]', nombre_municipio):
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
        
        logger.info(f"Se han generado {len(mapeos)} mapeos territoriales")
        return mapeos
    except Exception as e:
        logger.error(f"Error al generar mapeos territoriales: {e}")
        return []

def consultar_mapeos_por_territorio(engine):
    """
    Consulta los mapeos por territorio para verificar la distribución
    """
    logger.info("Consultando mapeos por territorio...")
    
    try:
        # Consultar todos los mapeos
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT nivel1, codigo_comunidad_autonoma, descripcion FROM mapeo_nivel1_comunidad_autonoma"
            ))
            mapeos = [
                {
                    "nivel1": row[0], 
                    "codigo_ca": row[1], 
                    "descripcion": row[2]
                } 
                for row in result
            ]
        
        # Clasificar por tipo de descripción
        mapeos_ca = [m for m in mapeos if m["descripcion"].startswith("Comunidad")]
        mapeos_es = [m for m in mapeos if m["descripcion"].startswith("España")]
        mapeos_municipios = [m for m in mapeos if m["descripcion"].startswith("Municipio")]
        
        # Clasificar municipios por idioma
        mapeos_castellano = [m for m in mapeos_municipios if "catalán" not in m["descripcion"] and 
                                                           "euskera" not in m["descripcion"] and 
                                                           "gallego" not in m["descripcion"]]
        mapeos_catalan = [m for m in mapeos_municipios if "catalán" in m["descripcion"]]
        mapeos_euskera = [m for m in mapeos_municipios if "euskera" in m["descripcion"]]
        mapeos_gallego = [m for m in mapeos_municipios if "gallego" in m["descripcion"]]
        
        # Clasificar por territorio
        territorios = {}
        for m in mapeos_municipios:
            codigo_ca = m["codigo_ca"]
            if codigo_ca not in territorios:
                territorios[codigo_ca] = []
            territorios[codigo_ca].append(m)
        
        # Guardar resultados en un archivo JSON
        with open('mapeos_por_territorio.json', 'w', encoding='utf-8') as f:
            json.dump({
                "total_mapeos": len(mapeos),
                "mapeos_comunidades_autonomas": len(mapeos_ca),
                "mapeos_estado": len(mapeos_es),
                "mapeos_municipios": {
                    "total": len(mapeos_municipios),
                    "castellano": len(mapeos_castellano),
                    "catalan": len(mapeos_catalan),
                    "euskera": len(mapeos_euskera),
                    "gallego": len(mapeos_gallego)
                },
                "territorios": {
                    codigo: len(mapeos) for codigo, mapeos in territorios.items()
                }
            }, f, ensure_ascii=False, indent=2)
        
        # Mostrar estadísticas
        print("\nEstadísticas de mapeos por territorio:")
        print(f"Total de mapeos: {len(mapeos)}")
        print(f"- Mapeos de comunidades autónomas: {len(mapeos_ca)}")
        print(f"- Mapeos de estado: {len(mapeos_es)}")
        print(f"- Mapeos de municipios: {len(mapeos_municipios)}")
        print(f"  - En castellano: {len(mapeos_castellano)}")
        print(f"  - En catalán: {len(mapeos_catalan)}")
        print(f"  - En euskera: {len(mapeos_euskera)}")
        print(f"  - En gallego: {len(mapeos_gallego)}")
        
        # Mostrar distribución por territorio
        print("\nDistribución por territorio:")
        for codigo, mapeos in territorios.items():
            print(f"- {codigo}: {len(mapeos)} mapeos")
        
        return mapeos
    except Exception as e:
        logger.error(f"Error al consultar mapeos por territorio: {e}")
        return []

def main():
    """
    Función principal
    """
    logger.info("Iniciando ajuste de mapeo de idiomas cooficiales por territorio...")
    
    try:
        # Crear conexión a la base de datos
        engine, session = crear_conexion()
        
        # Limpiar tabla de mapeo nivel1
        limpiar_tabla_mapeo_nivel1(engine)
        
        # Obtener municipios con territorio
        municipios_con_territorio = obtener_municipios_con_territorio(session)
        
        # Generar mapeos territoriales
        generar_mapeos_territoriales(engine, municipios_con_territorio)
        
        # Consultar mapeos por territorio
        consultar_mapeos_por_territorio(engine)
        
        logger.info("Ajuste de mapeo de idiomas cooficiales completado")
        return True
    except Exception as e:
        logger.error(f"Error en el proceso: {e}")
        return False

if __name__ == "__main__":
    main()
