import csv
import os
import requests
import time
import logging
from datetime import datetime
from pathlib import Path

URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
RUTA_LOGS = Path("logs")
RUTA_DESCARGAS = Path("csv/convocatorias")
RUTA_LOGS.mkdir(parents=True, exist_ok=True)
RUTA_DESCARGAS.mkdir(parents=True, exist_ok=True)
PRIMER_EJERCICIO = 2008

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_descarga.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def descargar_csv(tipo_admin: str, anio: int) -> list[dict]:
    """Descarga el CSV de convocatorias desde el endpoint /exportar con paginación."""
    filas_totales = []
    page = 0
    page_size = 10000
    fecha_desde = f"01/01/{anio}"
    fecha_hasta = f"31/12/{anio}"

    while True:
        params = {
            "vpd": "GE",
            "fechaDesde": fecha_desde,
            "fechaHasta": fecha_hasta,
            "tipoDoc": "csv",
            "page": page,
            "pageSize": page_size,
            "tipoAdministracion": tipo_admin,
        }
        url = f"{URL_BASE}/convocatorias/exportar"

        try:
            response = requests.get(url, params=params, timeout=60)
            status = response.status_code

            if status == 204:
                print(f"[204] No hay datos para {tipo_admin}-{anio} página {page}")
                logging.info(f"[204] No hay datos para {tipo_admin}-{anio} página {page}")
                break
            elif status != 200:
                print(f"[{status}] Entrypoint mal formado o error inesperado: {response.url}")
                logging.warning(f"[{status}] Entrypoint mal formado o error inesperado: {response.url}")
                break

            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type:
                print(f"Respuesta HTML inesperada para {tipo_admin}-{anio} página {page}")
                logging.warning(f"Respuesta HTML inesperada para {tipo_admin}-{anio} página {page}")
                break

            response.encoding = "latin-1"
            texto = response.text.strip()
            if not texto or texto.startswith("<"):
                print(f"Contenido vacío o no válido para {tipo_admin}-{anio} página {page}")
                logging.warning(f"Contenido vacío o no válido para {tipo_admin}-{anio} página {page}")
                break

            # Convertir la descarga separada por comas a datos estructurados.
            # ``csv`` gestiona las comillas de los textos con comas internas.
            # Algunos ficheros pueden venir ya con punto y coma, por lo que se
            # detecta el separador automáticamente para evitar fallos en la
            # paginación.
            primera_linea = texto.splitlines()[0]
            try:
                dialecto = csv.Sniffer().sniff(primera_linea)
                separador = dialecto.delimiter
            except Exception:
                separador = ","
            lector = csv.DictReader(texto.splitlines(), delimiter=separador, quotechar='"')
            filas = []
            for fila in lector:
                fila_limpia = {
                    clave.strip(): valor.replace('"', '').replace("'", '').strip() if isinstance(valor, str) else valor
                    for clave, valor in fila.items()
                }
                filas.append(fila_limpia)

            if not filas:
                logging.info(f"CSV vacío para {tipo_admin}-{anio} página {page}")
                break

            filas_totales.extend(filas)
            logging.info(f"{len(filas)} convocatorias descargadas para {tipo_admin}-{anio} página {page}")

            if len(filas) < page_size:
                break
            page += 1

        except Exception as e:
            logging.warning(f"Error al descargar CSV {tipo_admin}-{anio} página {page}: {e}")
            break

    return filas_totales

def guardar_csv(filas: list[dict], tipo_admin: str, anio: int):
    if not filas:
        return
    archivo = RUTA_DESCARGAS / f"convocatorias_{tipo_admin}_{anio}.csv"

    campos = list(filas[0].keys())

    with open(archivo, "w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(
            f,
            fieldnames=campos,
            delimiter=";",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )
        escritor.writeheader()
        escritor.writerows(filas)

def procesar_tipo_y_anio(tipo_admin: str, anio: int) -> int:
    logging.info(f"Procesando {tipo_admin}-{anio}")
    filas = descargar_csv(tipo_admin, anio)
    guardar_csv(filas, tipo_admin, anio)
    cantidad = len(filas)
    logging.info(f"{cantidad} convocatorias descargadas para {tipo_admin}-{anio}")
    return cantidad

def main():
    tipos_admin = ["C", "A", "L", "O"]
    anios = range(PRIMER_EJERCICIO, datetime.now().year + 1)
    inicio = time.time()

    total_global = 0
    for tipo_admin in tipos_admin:
        for anio in anios:
            cantidad = procesar_tipo_y_anio(tipo_admin, anio)
            total_global += cantidad
            print(f"{cantidad} convocatorias {tipo_admin} en {anio}")

    duracion = time.time() - inicio
    duracion_str = time.strftime("%H:%M:%S", time.gmtime(duracion))
    print(f"\nTotal general: {total_global}")
    print(f"Tiempo total de ejecución: {duracion_str}")
    logging.info(f"Total general: {total_global}")
    logging.info(f"Tiempo total de ejecución: {duracion_str}")

if __name__ == "__main__":
    main()
