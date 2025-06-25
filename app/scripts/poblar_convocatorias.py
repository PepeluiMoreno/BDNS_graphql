# poblar_convocatorias.py

import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
import requests

# --- Configuración de rutas y logging ---
URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
RUTA_JSONS = Path("json/convocatorias")
RUTA_LOGS = Path("logs")
RUTA_JSONS.mkdir(parents=True, exist_ok=True)
RUTA_LOGS.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_poblar_convocatorias.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def descargar_convocatorias(tipo, anio):
    resultados = []
    page = 0
    page_size = 10000
    total_esperado = None

    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "order": "numeroConvocatoria",
            "direccion": "asc",
            "fechaDesde": f"01/01/{anio}",
            "fechaHasta": f"31/12/{anio}",
            "tipoAdministracion": tipo
        }
        url = f"{URL_BASE}/convocatorias/busqueda"
        try:
            response = requests.get(url, params=params, timeout=180)
            response.raise_for_status()
            data = response.json()
            contenido = data.get("content", [])

            resultados.extend(contenido)

            if total_esperado is None:
                total_esperado = data.get("totalElements", 0)

            logging.info(f"Página {page} descargada ({len(contenido)} registros) para {tipo}-{anio}")
            print(f"  Página {page}: {len(contenido)} registros")

            if len(resultados) >= total_esperado:
                break

            page += 1

        except Exception as e:
            logging.error(f"Error al descargar {tipo}-{anio} página {page}: {e}")
            print(f"[ERROR] {tipo}-{anio} página {page}: {e}")
            break

    archivo = RUTA_JSONS / f"convocatorias_{tipo}_{anio}.json"
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    logging.info(f"{len(resultados)} convocatorias descargadas para {tipo}-{anio}")
    print(f"→ Subtotal {tipo}-{anio}: {len(resultados)} convocatorias")
    return len(resultados)

def main():
    parser = argparse.ArgumentParser(description="Descargar JSONs de convocatorias por año")
    parser.add_argument("anio", type=int, help="Año del archivo JSON")
    args = parser.parse_args()

    total_general = 0
    resumen = {}

    print(f"\n=== Descargando convocatorias del año {args.anio} ===\n")

    for tipo in ["C", "A", "L", "O"]:
        print(f"Descargando tipo {tipo}...")
        subtotal = descargar_convocatorias(tipo, args.anio)
        resumen[tipo] = subtotal
        total_general += subtotal

    print("\n=== RESUMEN ===")
    for tipo, subtotal in resumen.items():
        print(f"  {tipo}: {subtotal} convocatorias")
        logging.info(f"Subtotal {tipo}: {subtotal} convocatorias")

    print(f"\n=== TOTAL GENERAL: {total_general} convocatorias ===")
    logging.info(f"TOTAL GENERAL: {total_general} convocatorias")

if __name__ == "__main__":
    main()




