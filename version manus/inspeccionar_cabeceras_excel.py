#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para inspeccionar las cabeceras de los archivos Excel
y detectar los nombres reales de las columnas.
"""

import os
import pandas as pd
import json

# Rutas de archivos Excel
EXCEL_DIR = '/home/ubuntu/upload'
PROVINCIAS_EXCEL = os.path.join(EXCEL_DIR, 'Catalogo de Provincias.xlsx')
TIPOS_ENTIDAD_EXCEL = os.path.join(EXCEL_DIR, 'Catalogo de Tipos de Entidad Publica.xlsx')
UNIDADES_EELL_EXCEL = os.path.join(EXCEL_DIR, 'Listado Unidades EELL.xlsx')

def inspeccionar_excel(ruta_excel, nombre_archivo):
    """
    Inspecciona las cabeceras de un archivo Excel y devuelve los nombres de las columnas.
    """
    print(f"\nInspeccionando archivo: {nombre_archivo}")
    
    try:
        # Leer el archivo Excel
        df = pd.read_excel(ruta_excel)
        
        # Obtener nombres de columnas
        columnas = df.columns.tolist()
        
        print(f"Número de columnas: {len(columnas)}")
        print("Nombres de columnas:")
        for i, col in enumerate(columnas):
            print(f"  {i+1}. '{col}'")
        
        # Mostrar primeras filas para entender la estructura
        print("\nPrimeras 3 filas:")
        print(df.head(3))
        
        return {
            "archivo": nombre_archivo,
            "columnas": columnas,
            "num_filas": len(df),
            "muestra": df.head(3).to_dict(orient='records')
        }
    except Exception as e:
        print(f"Error al inspeccionar {nombre_archivo}: {e}")
        return {
            "archivo": nombre_archivo,
            "error": str(e)
        }

def main():
    """
    Función principal
    """
    print("Iniciando inspección de cabeceras de archivos Excel...")
    
    resultados = {}
    
    # Inspeccionar Catálogo de Provincias
    resultados["provincias"] = inspeccionar_excel(
        PROVINCIAS_EXCEL, 
        "Catalogo de Provincias.xlsx"
    )
    
    # Inspeccionar Catálogo de Tipos de Entidad Pública
    resultados["tipos_entidad"] = inspeccionar_excel(
        TIPOS_ENTIDAD_EXCEL, 
        "Catalogo de Tipos de Entidad Publica.xlsx"
    )
    
    # Inspeccionar Listado Unidades EELL
    resultados["unidades_eell"] = inspeccionar_excel(
        UNIDADES_EELL_EXCEL, 
        "Listado Unidades EELL.xlsx"
    )
    
    # Guardar resultados en un archivo JSON
    with open('cabeceras_excel.json', 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    
    print("\nInspección completada. Resultados guardados en 'cabeceras_excel.json'")

if __name__ == "__main__":
    main()
