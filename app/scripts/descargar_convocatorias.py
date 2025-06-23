import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
import requests

from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_
import unicodedata

# --- Cargar contexto del proyecto ---
from db.session import SessionLocal, get_db_url
from db.models import Convocatoria, Organo

# --- Configuraciones ---
URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
RUTA_JSONS = Path("json/convocatorias")
RUTA_LOGS = Path("logs")
RUTA_LOGS.mkdir(parents=True, exist_ok=True)
RUTA_JSONS.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_poblar_convocatorias.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def normalizar(texto):
    if texto is None:
        return None
    return unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("utf-8").strip().upper()

def buscar_organo_id(session, nivel1, nivel2, nivel3):
    n1, n2, n3 = normalizar(nivel1), normalizar(nivel2), normalizar(nivel3)
    organo = session.query(Organo).filter(
        and_(
            Organo.nivel1 == n1,
            Organo.nivel2 == n2,
            Organo.nivel3 == n3
        )
    ).first()
    return organo.id if organo else None

def descargar_convocatorias(tipo, anio, session):
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
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            contenido = data.get("content", [])
            for fila in contenido:
                organo_id = buscar_organo_id(session, fila.get("nivel1"), fila.get("nivel2"), fila.get("nivel3"))
                fila["organo_id"] = organo_id
                if organo_id is None:
                    logging.error(f"Órgano no encontrado: {fila.get('nivel1')} - {fila.get('nivel2')} - {fila.get('nivel3')}")
            resultados.extend(contenido)
            if total_esperado is None:
                total_esperado = data.get("totalElements", 0)
            print(f"{len(contenido)} convocatorias descargadas para {tipo}-{anio} página {page}")
            logging.info(f"{len(contenido)} convocatorias descargadas para {tipo}-{anio} página {page}")
            if data.get("last", True):
                break
            page += 1
        except Exception as e:
            logging.error(f"Error al descargar {tipo}-{anio} página {page}: {e}")
            break

    if total_esperado is not None and len(resultados) != total_esperado:
        logging.error(f"Diferencia en totalElements: esperados {total_esperado}, obtenidos {len(resultados)}")
        print(f"[ERROR] Mismatch: esperados {total_esperado}, obtenidos {len(resultados)}")

    archivo = RUTA_JSONS / f"convocatorias_{tipo}_{anio}.json"
    with open(archivo, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    logging.info(f"{len(resultados)} convocatorias descargadas para {tipo}-{anio}")

def cargar_json_convocatorias(archivo):
    with open(archivo, "r", encoding="utf-8") as f:
        return json.load(f)

def poblar_convocatorias(desde_archivo, session):
    total = 0
    exitos = 0
    fallos = 0
    actualizaciones = 0
    datos = cargar_json_convocatorias(desde_archivo)

    for entrada in datos:
        codigo_bdns = entrada.get("numeroConvocatoria")
        if not codigo_bdns:
            continue

        organo_id = entrada.get("organo_id")
        existente = session.get(Convocatoria, codigo_bdns)

        try:
            if existente:
                existente.descripcion = entrada.get("descripcion")
                existente.descripcion_leng = entrada.get("descripcionLeng")
                existente.fecha_recepcion = entrada.get("fechaRecepcion")
                existente.mrr = entrada.get("mrr", False)
                existente.organo_id = organo_id
                actualizaciones += 1
            else:
                nueva = Convocatoria(
                    codigo_bdns=codigo_bdns,
                    descripcion=entrada.get("descripcion"),
                    descripcion_leng=entrada.get("descripcionLeng"),
                    fecha_recepcion=entrada.get("fechaRecepcion"),
                    mrr=entrada.get("mrr", False),
                    organo_id=organo_id
                )
                session.add(nueva)
                exitos += 1

            session.commit()
        except IntegrityError as e:
            session.rollback()
            logging.error(f"Error al guardar {codigo_bdns}: {e}")
            fallos += 1
        except Exception as e:
            session.rollback()
            logging.error(f"Error inesperado con {codigo_bdns}: {e}")
            fallos += 1

        total += 1

    print(f"Convocatorias procesadas: {total}")
    print(f"Exitosas: {exitos}")
    print(f"Actualizadas: {actualizaciones}")
    print(f"Fallidas: {fallos}")
    logging.info(f"Total: {total}, Exitosas: {exitos}, Actualizadas: {actualizaciones}, Fallidas: {fallos}")
    return total, exitos, actualizaciones, fallos

def main():
    parser = argparse.ArgumentParser(description="Descargar y poblar la tabla Convocatoria desde JSON por año")
    parser.add_argument("anio", type=int, help="Año del archivo JSON")
    args = parser.parse_args()

    session = SessionLocal()
    total_global, exitos_global, actualizaciones_global, fallos_global = 0, 0, 0, 0

    for tipo in ["C", "A", "L", "O"]:
        archivo = RUTA_JSONS / f"convocatorias_{tipo}_{args.anio}.json"
        if not archivo.exists():
            print(f"Descargando {tipo}-{args.anio}...")
            descargar_convocatorias(tipo, args.anio, session)

        if archivo.exists():
            print(f"Procesando {archivo.name}")
            t, e, a, f = poblar_convocatorias(archivo, session)
            total_global += t
            exitos_global += e
            actualizaciones_global += a
            fallos_global += f
        else:
            print(f"No encontrado: {archivo.name}")

    print("\nResumen total del ejercicio:")
    print(f"Procesadas: {total_global}")
    print(f"Exitosas: {exitos_global}")
    print(f"Actualizadas: {actualizaciones_global}")
    print(f"Fallidas: {fallos_global}")
    logging.info(f"Resumen final - Total: {total_global}, Exitosas: {exitos_global}, Actualizadas: {actualizaciones_global}, Fallidas: {fallos_global}")

if __name__ == "__main__":
    main()

