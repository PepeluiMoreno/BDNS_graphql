# poblamiento_concurrente_convocatorias_detalle.py
import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from multiprocessing import Process
import requests
import csv
from sqlalchemy.orm import sessionmaker
from db.session import engine
from db.utils import normalizar, buscar_organo_id
from db.models import (
    SectorActividad, Instrumento, TipoBeneficiario, SectorProducto, Region,
    Finalidad, Objetivo, Reglamento, Fondo
)

# --- Configuraciones ---
URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
RUTA_JSONS = Path("json/convocatorias")
RUTA_LOGS = Path("logs")
RUTA_DEBUG = Path("data/debug")
RUTA_LOGS.mkdir(parents=True, exist_ok=True)
RUTA_DEBUG.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_detalles_concurrente.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

Session = sessionmaker(bind=engine)

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

def limpiar_csv_duplicados():
    for catalogo in CATALOGOS.values():
        nombre = catalogo.__tablename__
        path = RUTA_DEBUG / f"faltantes_{nombre}.csv"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                lineas = set(line.strip().strip('"') for line in f if line.strip())
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                for linea in sorted(lineas):
                    writer.writerow([linea])
            print(f"✔ CSV deduplicado: {path.name}")

_faltantes_cache = {}

def get_faltantes_csv(catalogo):
    return RUTA_DEBUG / f"faltantes_{catalogo}.csv"

def registrar_faltante(nombre_catalogo, descripcion):
    descripcion = descripcion.strip().strip('"')
    path = get_faltantes_csv(nombre_catalogo)
    if nombre_catalogo not in _faltantes_cache:
        _faltantes_cache[nombre_catalogo] = set()
        if path.exists():
            with open(path, encoding="utf-8") as f:
                _faltantes_cache[nombre_catalogo].update(
                    line.strip().strip('"') for line in f if line.strip()
                )

    if descripcion not in _faltantes_cache[nombre_catalogo]:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([descripcion])
        _faltantes_cache[nombre_catalogo].add(descripcion)

def enriquecer_detalle(session, entrada):
    cod = entrada.get("numeroConvocatoria")
    print(f"→ Enriqueciendo detalle convocatoria {cod}...")
    url = f"{URL_BASE}/convocatorias?numConv={cod}"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        detalle = r.json()
    except Exception as e:
        logging.error(f"Error recuperando detalle {cod}: {e}")
        print(f"[ERROR] No se pudo recuperar el detalle de {cod}")
        return

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
                logging.warning(f"Falta {modelo.__tablename__}: {desc}")
                if modelo in [SectorActividad, Fondo]:
                    registrar_faltante(modelo.__tablename__, desc)

    nivel1 = entrada.get("nivel1")
    nivel2 = entrada.get("nivel2")
    nivel3 = entrada.get("nivel3")
    organo_id = buscar_organo_id(session, nivel1, nivel2, nivel3)
    if organo_id:
        detalle["organo_id"] = organo_id
        print(f"   · Órgano asignado: {organo_id}")
    else:
        logging.warning(f"Órgano no encontrado: {nivel1} / {nivel2} / {nivel3}")
        print("   · Órgano no encontrado")

    entrada.update(detalle)

def procesar_archivo(archivo):
    if not archivo.exists():
        print(f"[AVISO] Archivo no encontrado: {archivo}")
        return

    session = Session()
    try:
        with open(archivo, encoding="utf-8") as f:
            datos = json.load(f)

        print(f"→ Procesando archivo: {archivo.name} ({len(datos)} registros)")

        for entrada in datos:
            enriquecer_detalle(session, entrada)

        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)
        print(f"✔ Archivo enriquecido guardado: {archivo.name}\n")
    finally:
        session.close()

def lanzar_procesos(anio):
    procesos = []
    archivos = list(RUTA_JSONS.glob(f"convocatorias_*_{anio}.json"))
    if not archivos:
        logging.warning(f"No se encontraron archivos JSON para el año {anio} en {RUTA_JSONS}")
        print(f"[AVISO] No se encontraron archivos JSON para el año {anio} en {RUTA_JSONS}")
        return

    for archivo in archivos:
        p = Process(target=procesar_archivo, args=(archivo,))
        p.start()
        procesos.append(p)

    for p in procesos:
        p.join()

def main():
    parser = argparse.ArgumentParser(description="Enriquecer JSONs de convocatorias con detalles y relaciones")
    parser.add_argument("anio", type=int)
    args = parser.parse_args()
    print(f"=== Enriquecer convocatorias del año {args.anio} de forma concurrente ===")
    lanzar_procesos(args.anio)
    limpiar_csv_duplicados()
    print("=== Proceso completado ===")

if __name__ == "__main__":
    main()


