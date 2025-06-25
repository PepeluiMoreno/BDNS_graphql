# test_poblar_detalles.py

import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
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
    filename=RUTA_LOGS / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_test_poblar_detalles.log",
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

FALTANTES_CSV = RUTA_DEBUG / "catalogos_faltantes.csv"

faltantes_registrados = set()

def registrar_faltante(nombre_catalogo, descripcion):
    clave = (nombre_catalogo, descripcion)
    if clave not in faltantes_registrados:
        with open(FALTANTES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([nombre_catalogo, descripcion])
        faltantes_registrados.add(clave)

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
        print(f"   · Procesando catálogo '{campo}'...")
        valores = detalle.get(campo)
        if not valores:
            continue
        for item in valores:
            norm = normalizar(item.get("descripcion")) if "descripcion" in item else None
            if norm:
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

    # Enriquecer con organo_id
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

def procesar_json(anio):
    session = Session()
    for archivo in RUTA_JSONS.glob(f"convocatorias_*_{anio}.json"):
        print(f"Procesando archivo: {archivo.name}")
        with open(archivo, encoding="utf-8") as f:
            datos = json.load(f)

        for entrada in datos[:10]:  # Limitar a 10 convocatorias
            enriquecer_detalle(session, entrada)

        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)
        print(f"Archivo enriquecido guardado: {archivo.name}\n")

    session.close()

def main():
    parser = argparse.ArgumentParser(description="Enriquecer JSONs de convocatorias con detalles y relaciones")
    parser.add_argument("anio", type=int)
    args = parser.parse_args()
    print(f"=== Iniciando enriquecimiento de convocatorias para el año {args.anio} ===\n")
    if FALTANTES_CSV.exists():
        FALTANTES_CSV.unlink()  # limpiar antes de comenzar
    procesar_json(args.anio)
    print("\n=== Proceso completado ===")

if __name__ == "__main__":
    main()


