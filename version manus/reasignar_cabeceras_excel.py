#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para reasignar nombres de columnas desde la fila correcta en los archivos Excel
y ajustar el script de poblamiento unificado.
"""

import os
import pandas as pd
import json

# Rutas de archivos Excel
EXCEL_DIR = '/home/ubuntu/upload'
PROVINCIAS_EXCEL = os.path.join(EXCEL_DIR, 'Catalogo de Provincias.xlsx')
TIPOS_ENTIDAD_EXCEL = os.path.join(EXCEL_DIR, 'Catalogo de Tipos de Entidad Publica.xlsx')
UNIDADES_EELL_EXCEL = os.path.join(EXCEL_DIR, 'Listado Unidades EELL.xlsx')

def leer_excel_con_cabeceras_correctas(ruta_excel, nombre_archivo, fila_cabecera=0, fila_datos=1):
    """
    Lee un archivo Excel asignando nombres de columnas desde la fila correcta.
    """
    print(f"\nProcesando archivo: {nombre_archivo}")
    
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
        
        print(f"Número de columnas: {len(cabeceras_limpias)}")
        print("Nombres de columnas asignados:")
        for i, col in enumerate(cabeceras_limpias):
            print(f"  {i+1}. '{col}'")
        
        # Mostrar primeras filas para verificar
        print("\nPrimeras 3 filas con cabeceras correctas:")
        print(df.head(3))
        
        return {
            "archivo": nombre_archivo,
            "cabeceras_originales": cabeceras,
            "cabeceras_limpias": cabeceras_limpias,
            "num_filas": len(df),
            "dataframe": df
        }
    except Exception as e:
        print(f"Error al procesar {nombre_archivo}: {e}")
        return {
            "archivo": nombre_archivo,
            "error": str(e)
        }

def generar_mapeo_columnas():
    """
    Genera un mapeo entre los nombres de columnas esperados y los reales.
    """
    # Mapeo para Catálogo de Provincias
    mapeo_provincias = {
        "codigo_ine": "Código INE",
        "nombre_provincia": "Denominación Provincia",
        "codigo_ca": "Cod.CA"
    }
    
    # Mapeo para Catálogo de Tipos de Entidad Pública
    mapeo_tipos_entidad = {
        "codigo": "Cód.",
        "descripcion": "Entidad Pública"
    }
    
    # Mapeo para Listado Unidades EELL
    mapeo_unidades_eell = {
        "codigo_dir3": "C_ID_UD_ORGANICA",
        "denominacion": "C_DNM_UD_ORGANICA",
        "nivel_administracion": "C_ID_NIVEL_ADMON",
        "codigo_tipo_ente": "C_ID_TIPO_ENT_PUBLICA",
        "nivel_jerarquico": "N_NIVEL_JERARQUICO",
        "codigo_provincia": "C_ID_AMB_PROVINCIA",
        "provincia": "C_DESC_PROV",
        "codigo_unidad_superior": "C_ID_DEP_UD_SUPERIOR",
        "denominacion_unidad_superior": "C_DNM_UD_ORGANICA.1",
        "codigo_unidad_principal": "C_ID_DEP_UD_PRINCIPAL",
        "denominacion_unidad_principal": "C_DNM_UD_ORGANICA.2",
        "estado": "C_ID_ESTADO",
        "nif_cif": "NIF_CIF"
    }
    
    return {
        "provincias": mapeo_provincias,
        "tipos_entidad": mapeo_tipos_entidad,
        "unidades_eell": mapeo_unidades_eell
    }

def main():
    """
    Función principal
    """
    print("Iniciando reasignación de nombres de columnas en archivos Excel...")
    
    resultados = {}
    
    # Procesar Catálogo de Provincias (cabeceras en fila 0, datos desde fila 2)
    resultados["provincias"] = leer_excel_con_cabeceras_correctas(
        PROVINCIAS_EXCEL, 
        "Catalogo de Provincias.xlsx",
        fila_cabecera=0,
        fila_datos=2
    )
    
    # Procesar Catálogo de Tipos de Entidad Pública (cabeceras en fila 0, datos desde fila 2)
    resultados["tipos_entidad"] = leer_excel_con_cabeceras_correctas(
        TIPOS_ENTIDAD_EXCEL, 
        "Catalogo de Tipos de Entidad Publica.xlsx",
        fila_cabecera=0,
        fila_datos=2
    )
    
    # Procesar Listado Unidades EELL (cabeceras en fila 0, datos desde fila 1)
    resultados["unidades_eell"] = leer_excel_con_cabeceras_correctas(
        UNIDADES_EELL_EXCEL, 
        "Listado Unidades EELL.xlsx",
        fila_cabecera=0,
        fila_datos=1
    )
    
    # Generar mapeo de columnas
    mapeo_columnas = generar_mapeo_columnas()
    
    # Guardar resultados en un archivo JSON
    with open('cabeceras_corregidas.json', 'w', encoding='utf-8') as f:
        json.dump({
            "resultados": {
                "provincias": {
                    "archivo": resultados["provincias"]["archivo"],
                    "cabeceras_limpias": resultados["provincias"]["cabeceras_limpias"],
                    "num_filas": resultados["provincias"]["num_filas"]
                },
                "tipos_entidad": {
                    "archivo": resultados["tipos_entidad"]["archivo"],
                    "cabeceras_limpias": resultados["tipos_entidad"]["cabeceras_limpias"],
                    "num_filas": resultados["tipos_entidad"]["num_filas"]
                },
                "unidades_eell": {
                    "archivo": resultados["unidades_eell"]["archivo"],
                    "cabeceras_limpias": resultados["unidades_eell"]["cabeceras_limpias"],
                    "num_filas": resultados["unidades_eell"]["num_filas"]
                }
            },
            "mapeo_columnas": mapeo_columnas
        }, f, ensure_ascii=False, indent=2)
    
    print("\nReasignación completada. Resultados guardados en 'cabeceras_corregidas.json'")
    
    # Guardar DataFrames para uso posterior
    if "dataframe" in resultados["provincias"]:
        resultados["provincias"]["dataframe"].to_csv('provincias_procesado.csv', index=False)
        print("DataFrame de provincias guardado en 'provincias_procesado.csv'")
    
    if "dataframe" in resultados["tipos_entidad"]:
        resultados["tipos_entidad"]["dataframe"].to_csv('tipos_entidad_procesado.csv', index=False)
        print("DataFrame de tipos de entidad guardado en 'tipos_entidad_procesado.csv'")
    
    if "dataframe" in resultados["unidades_eell"]:
        # Guardar solo una muestra para evitar archivos muy grandes
        resultados["unidades_eell"]["dataframe"].head(100).to_csv('unidades_eell_muestra.csv', index=False)
        print("Muestra de DataFrame de unidades EELL guardada en 'unidades_eell_muestra.csv'")

if __name__ == "__main__":
    main()
