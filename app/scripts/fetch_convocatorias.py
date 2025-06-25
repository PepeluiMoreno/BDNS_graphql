# fetch_convocatorias.py: Descarga y enriquece convocatorias BDNS por tipo, mes y año con concurrencia

import argparse
import calendar
import json
import logging
import multiprocessing
import os
import queue
import sys
import time
from collections import defaultdict
from datetime import datetime
from multiprocessing import Manager, Pool
from pathlib import Path

import requests
from sqlalchemy.orm import sessionmaker
from db.session import engine
from db.utils import normalizar, buscar_organo_id
from db.models import (
    SectorActividad, Instrumento, TipoBeneficiario, SectorProducto, Region,
    Finalidad, Objetivo, Reglamento, Fondo
)

# --- Configuración ---
URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
RUTA_JSONS = Path("json/convocatorias")
RUTA_LOGS = Path("logs")
RUTA_DEBUG = Path("data/debug")
RUTA_FALLIDOS = RUTA_DEBUG / "bloques_fallidos.csv"
TIPO_MAP = {"C": "estatales", "A": "autonomicas", "L": "locales", "O": "otras"}
CATALOGOS = {
    "instrumentos": Instrumento,
    "tiposBeneficiarios": TipoBeneficiario,
    "sectores": SectorActividad,
    "sectoresProducto": SectorProducto,
    "regiones": Region,
    "finalidades": Finalidad,
    "objetivos": Objetivo,
    "reglamentos": Reglamento,
    "fondos": Fondo,
}

# --- Inicialización rutas ---
RUTA_JSONS.mkdir(parents=True, exist_ok=True)
RUTA_LOGS.mkdir(parents=True, exist_ok=True)
RUTA_DEBUG.mkdir(parents=True, exist_ok=True)

# --- Sesión SQLAlchemy ---
Session = sessionmaker(bind=engine)
def get_session():
    return Session()

# --- Logger centralizado ---
def log_listener(queue):
    logging.basicConfig(
        filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_fetch_convocatorias.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s"
    )
    while True:
        try:
            msg = queue.get()
            if msg == "__STOP__":
                break
            logging.info(msg)
        except Exception:
            continue

def log(msg, q):
    print(msg)
    q.put(msg)

def registrar_faltante(nombre_catalogo, descripcion):
    path = RUTA_DEBUG / f"faltantes_{nombre_catalogo}.csv"
    descripcion = descripcion.strip().strip('"')
    with open(path, "a", newline="", encoding="utf-8") as f:
        f.write(f"{descripcion}\n")

def fetch_detalle_convocatoria(session, entrada, retries=3, delay=5):
    cod = entrada.get("numeroConvocatoria")
    url = f"{URL_BASE}/convocatorias?numConv={cod}"
    response = None
    for intento in range(retries):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            try:
                detalle = response.json()
                break  # Éxito
            except json.JSONDecodeError as e:
                if intento == retries - 1:
                    debug_path = RUTA_DEBUG / f"malformado_{cod}.txt"
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(response.text if response and response.text else "<respuesta vacía>")
                    raise
        except Exception:
            if intento == retries - 1:
                debug_path = RUTA_DEBUG / f"malformado_{cod}.txt"
                with open(debug_path, "w", encoding="utf-8") as f:
                    contenido = response.text if response and response.text else "<sin respuesta>"
                    f.write(contenido)
                raise
            time.sleep(delay * (2 ** intento))

    for campo, modelo in CATALOGOS.items():
        valores = detalle.get(campo)
        if not valores:
            continue
        for item in valores:
            if "descripcion" in item:
                item["descripcion"] = item["descripcion"].strip().strip('"')
                norm = normalizar(item["descripcion"])
                existente = session.query(modelo).filter_by(descripcion_norm=norm).first()
            else:
                existente = session.query(modelo).get(item.get("id"))
            if existente:
                item["id"] = existente.id
            else:
                desc = item.get("descripcion") or str(item)
                if modelo in [SectorActividad, Fondo]:
                    registrar_faltante(modelo.__tablename__, desc)

    organo_id = buscar_organo_id(session, entrada.get("nivel1"), entrada.get("nivel2"), entrada.get("nivel3"))
    if organo_id:
        detalle["organo_id"] = organo_id
    entrada.update(detalle)
    return entrada

def safe_request(url, params, retries=3, delay=5):
    for intento in range(retries):
        try:
            r = requests.get(url, params=params, timeout=120)
            r.raise_for_status()
            return r
        except Exception as e:
            if intento < retries - 1:
                time.sleep(delay * (2 ** intento))
            else:
                raise e

def procesar_bloque(args):
    tipo, mes, anio, q = args
    session = get_session()
    nombre_tipo = TIPO_MAP[tipo]
    nombre_mes = calendar.month_name[mes].lower()
    clave = f"{tipo}-{mes:02d}-{anio}"
    ident = f"[{nombre_tipo.capitalize()} {nombre_mes} {anio}]"
    errores = 0
    try:
        fecha_desde = f"01/{mes:02d}/{anio}"
        fecha_hasta = f"31/12/{anio}" if mes == 12 else f"01/{mes + 1:02d}/{anio}"
        params = {
            "page": 0,
            "pageSize": 10000,
            "order": "numeroConvocatoria",
            "direccion": "asc",
            "fechaDesde": fecha_desde,
            "fechaHasta": fecha_hasta,
            "tipoAdministracion": tipo
        }
        r = safe_request(f"{URL_BASE}/convocatorias/busqueda", params)
        datos = r.json().get("content", [])
        log(f"{ident} {len(datos)} convocatorias obtenidas", q)
        enriquecidos = []
        for d in datos:
            cod = d.get("numeroConvocatoria", "<desconocido>")
            try:
                enriquecidos.append(fetch_detalle_convocatoria(session, d))
            except Exception as e:
                log(f"{ident} ERROR procesando convocatoria: {cod}: {e}", q)
                errores += 1

        path = RUTA_JSONS / f"convocatorias_{nombre_tipo}_{anio}.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                anteriores = json.load(f)
        else:
            anteriores = []
        anteriores.extend(enriquecidos)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(anteriores, f, ensure_ascii=False, indent=2)

        log(f"{ident} Extraídas {len(enriquecidos)} convocatorias en {path.name}", q)

        if errores > 0:
            with open(RUTA_FALLIDOS, "a", newline="", encoding="utf-8") as f:
                f.write(f"{tipo},{mes},{anio}\n")
    except Exception as e:
        with open(RUTA_FALLIDOS, "a", newline="", encoding="utf-8") as f:
            f.write(f"{tipo},{mes},{anio}\n")
        log(f"{ident} ERROR procesando bloque {clave}: {e}", q)
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Descargar y enriquecer convocatorias BDNS")
    parser.add_argument("anio", type=int, help="Año a procesar")
    args = parser.parse_args()

    inicio = time.time()
    anio = args.anio
    print(f"=== Procesando convocatorias del {anio} ===")

    manager = Manager()
    q = manager.Queue()
    listener = multiprocessing.Process(target=log_listener, args=(q,))
    listener.start()

    bloques = [(tipo, mes, anio, q) for mes in range(1, 13) for tipo in TIPO_MAP.keys()]

    with Pool(processes=12) as pool:
        pool.map(procesar_bloque, bloques)

    q.put("__STOP__")
    listener.join()

    duracion = time.time() - inicio
    print(f"Proceso completado en {duracion:.2f} segundos.")

if __name__ == "__main__":
    main()


