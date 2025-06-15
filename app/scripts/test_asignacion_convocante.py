#!/usr/bin/env python3
"""Genera un log con el tipo de convocatoria y su órgano convocante.

Uso:
    python -m app.scripts.test_asignacion_convocante 2018
"""
import argparse
import csv
import glob
import logging
from pathlib import Path
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db.models import Organo
from app.utils.organo_finder import encontrar_codigo_convocante

CSV_DIR = Path(__file__).resolve().parent.parent / "csv" / "convocatorias"
TIPOS = {
    "A": "AUTONOMICA",
    "C": "ESTATAL",
    "L": "LOCAL",
    "O": "Otras Administraciones",
}

def preprocess_line(line: str) -> list[str]:
    """Convierte una línea en una lista de campos.

    Algunas descargas envuelven la línea completa entre comillas dobles y usan
    comillas duplicadas para escapar caracteres. Esta función limpia esos casos
    para poder usar ``csv.reader`` de forma fiable.
    """
    line = line.strip()
    if not line:
        return []
    if line.startswith("\"") and line.endswith("\""):
        line = line[1:-1]
    line = line.replace("\"\"", "\"")
    return next(csv.reader([line], delimiter=",", quotechar="\""))

def procesar_archivo(
    ruta: Path, tipo_desc: str, session: Session | None = None
) -> None:
    """Procesa un archivo CSV mostrando la asignación de órganos.

    Si no se proporciona una sesión se crea automáticamente usando
    :class:`SessionLocal` de :mod:`app.db.session`.
    """
    close_session = False
    if session is None:
        session = SessionLocal()
        close_session = True

    with ruta.open(encoding="latin-1") as f:
        _ = preprocess_line(f.readline())  # descartar cabecera
        for linea in f:
            if not linea.strip():
                continue
            fila = preprocess_line(linea)
            if len(fila) < 5:
                continue
            codigo = fila[0].strip()
            administracion = fila[2].strip()
            departamento = fila[3].strip()
            organo = fila[4].strip()

            org_id = encontrar_codigo_convocante(
                administracion, departamento, organo, session=session
            )
            datos_organo = None
            if org_id:
                datos_organo = session.get(Organo, org_id)
            if datos_organo:
                org_desc = f"{datos_organo.nombre} [{datos_organo.id}]"
            else:
                org_desc = "No encontrado"

            # Keep the format string and arguments aligned.
            # There are six %s placeholders with six corresponding values.
            logging.info(
                "Convocatoria %s (%s) -> %s - %s - %s | Órgano: %s",
                codigo,
                tipo_desc,
                administracion,
                departamento,
                organo,
                org_desc,
            )
    if close_session:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Generar log de convocantes")
    parser.add_argument("ejercicio", type=int, help="Año de las convocatorias")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    patron = str(CSV_DIR / f"convocatorias_*_{args.ejercicio}.csv")
    archivos = sorted(glob.glob(patron))
    if not archivos:
        logging.warning("No se encontraron archivos para el ejercicio %s", args.ejercicio)
        return

    with SessionLocal() as session:
        for archivo in archivos:
            prefijo = Path(archivo).stem.split("_")[1]
            tipo_desc = TIPOS.get(prefijo, "Desconocido")
            procesar_archivo(Path(archivo), tipo_desc, session)


if __name__ == "__main__":
    main()
