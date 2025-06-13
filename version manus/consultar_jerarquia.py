#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para consultar la jerarquía geográfica autoreferenciada
"""

import os
import sys
import json
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Importar modelos
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from app.db.models import AmbitoGeografico

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("consulta_jerarquia")

# Configuración de la base de datos
DB_URL = os.environ.get('DB_URL', 'postgresql://postgres:postgres@localhost/bdnsdb')

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

def obtener_estructura_jerarquica(session):
    """
    Obtiene la estructura jerárquica completa
    """
    logger.info("Obteniendo estructura jerárquica completa...")
    
    # Obtener todos los países
    paises = session.query(AmbitoGeografico).filter_by(tipo='NA').all()
    
    # Crear estructura jerárquica
    jerarquia = []
    
    # Para cada país, obtener su jerarquía completa
    for pais in paises:
        # Solo procesar España para el ejemplo
        if pais.codigo == 'ES':
            estructura_pais = {
                'codigo': pais.codigo,
                'tipo': pais.tipo,
                'nombre': pais.nombre,
                'nivel': pais.nivel_jerarquico,
                'comunidades_autonomas': []
            }
            
            # Obtener comunidades autónomas del país
            comunidades = session.query(AmbitoGeografico).filter_by(
                tipo='CA', 
                codigo_ambito_superior=pais.codigo
            ).all()
            
            for comunidad in comunidades:
                estructura_comunidad = {
                    'codigo': comunidad.codigo,
                    'tipo': comunidad.tipo,
                    'nombre': comunidad.nombre,
                    'nivel': comunidad.nivel_jerarquico,
                    'provincias': []
                }
                
                # Obtener provincias de la comunidad ordenadas alfabéticamente por nombre
                provincias = session.query(AmbitoGeografico).filter_by(
                    tipo='PR', 
                    codigo_ambito_superior=comunidad.codigo
                ).order_by(AmbitoGeografico.nombre).all()
                
                for provincia in provincias:
                    estructura_provincia = {
                        'codigo': provincia.codigo,
                        'tipo': provincia.tipo,
                        'nombre': provincia.nombre,
                        'nivel': provincia.nivel_jerarquico,
                        'municipios': []
                    }
                    
                    # Obtener algunos municipios de la provincia (limitar a 5 para el ejemplo)
                    municipios = session.query(AmbitoGeografico).filter_by(
                        tipo='LO', 
                        codigo_ambito_superior=provincia.codigo
                    ).limit(5).all()
                    
                    for municipio in municipios:
                        estructura_municipio = {
                            'codigo': municipio.codigo,
                            'tipo': municipio.tipo,
                            'nombre': municipio.nombre,
                            'nivel': municipio.nivel_jerarquico,
                            'es_capital': municipio.es_capital
                        }
                        estructura_provincia['municipios'].append(estructura_municipio)
                    
                    estructura_comunidad['provincias'].append(estructura_provincia)
                
                estructura_pais['comunidades_autonomas'].append(estructura_comunidad)
            
            jerarquia.append(estructura_pais)
    
    return jerarquia

def obtener_estadisticas(session):
    """
    Obtiene estadísticas de la tabla de ámbito geográfico
    """
    logger.info("Obteniendo estadísticas...")
    
    estadisticas = {
        'total_registros': session.query(AmbitoGeografico).count(),
        'por_tipo': {
            'paises': session.query(AmbitoGeografico).filter_by(tipo='NA').count(),
            'comunidades_autonomas': session.query(AmbitoGeografico).filter_by(tipo='CA').count(),
            'provincias': session.query(AmbitoGeografico).filter_by(tipo='PR').count(),
            'municipios': session.query(AmbitoGeografico).filter_by(tipo='LO').count()
        },
        'capitales': {
            'total': session.query(AmbitoGeografico).filter_by(es_capital=True).count(),
            'de_provincia': session.query(AmbitoGeografico).filter(
                AmbitoGeografico.es_capital == True,
                AmbitoGeografico.capital_de.like('PR%')
            ).count(),
            'de_comunidad': session.query(AmbitoGeografico).filter(
                AmbitoGeografico.es_capital == True,
                AmbitoGeografico.capital_de.like('CA%')
            ).count()
        }
    }
    
    return estadisticas

def main():
    """
    Función principal
    """
    logger.info("Iniciando consulta de jerarquía geográfica...")
    
    try:
        # Crear conexión a la base de datos
        engine, session = crear_conexion()
        
        # Obtener estructura jerárquica
        jerarquia = obtener_estructura_jerarquica(session)
        
        # Obtener estadísticas
        estadisticas = obtener_estadisticas(session)
        
        # Guardar resultados
        with open('jerarquia_geografica_ejemplo.json', 'w', encoding='utf-8') as f:
            json.dump(jerarquia, f, ensure_ascii=False, indent=2)
        
        with open('estadisticas_ambito_geografico.json', 'w', encoding='utf-8') as f:
            json.dump(estadisticas, f, ensure_ascii=False, indent=2)
        
        logger.info("Consulta de jerarquía geográfica completada")
        
        # Mostrar estadísticas
        print("\nEstadísticas de la tabla de ámbito geográfico:")
        print(f"Total de registros: {estadisticas['total_registros']}")
        print(f"Países: {estadisticas['por_tipo']['paises']}")
        print(f"Comunidades Autónomas: {estadisticas['por_tipo']['comunidades_autonomas']}")
        print(f"Provincias: {estadisticas['por_tipo']['provincias']}")
        print(f"Municipios: {estadisticas['por_tipo']['municipios']}")
        print(f"Capitales: {estadisticas['capitales']['total']}")
        
        return True
    except Exception as e:
        logger.error(f"Error en la consulta: {e}")
        return False

if __name__ == "__main__":
    main()
