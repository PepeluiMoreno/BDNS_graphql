#!/usr/bin/env python3
"""Genera un log con el tipo de convocatoria y su órgano convocante.

Uso:
    python -m scripts.test_asignacion_convocante 2018
"""
import argparse
import csv
import glob
import logging
from pathlib import Path
from datetime import datetime
import sys

from db.session import SessionLocal
from db.models import Organo
from utils.organo_finder import (
    encontrar_codigo_convocante,
)

from scripts.poblar_organos import normalizar_texto

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

CSV_DIR = Path(__file__).resolve().parent.parent / "csv" / "convocatorias"
TIPOS = {
    "A": "AUTONOMICA",
    "C": "ESTATAL",
    "L": "LOCAL",
    "O": "Otras Administraciones",
}

# Configuración de logging similar a ``apply_migrations.py``
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"test_asignacion_convocante_{timestamp}.log"

logger = logging.getLogger("test_asignacion_convocante")
logger.setLevel(logging.INFO)


formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")



file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def preprocess_line(line: str) -> list[str]:
    """Convierte una línea en una lista de campos.

    Algunas descargas envuelven la línea completa entre comillas dobles y usan
    comillas duplicadas para escapar caracteres. Esta función limpia esos casos
    para poder usar ``csv.reader`` de forma fiable.
    """
    line = line.strip()
    if not line:
        return []
    if line.startswith('"') and line.endswith('"'):
        line = line[1:-1]
    line = line.replace('""', '"')
    return next(csv.reader([line], delimiter=",", quotechar='"'))


def test_busqueda_sin_acentos() -> None:
    """Comprueba que las búsquedas sin acentos devuelven el mismo órgano."""
    with SessionLocal() as session:
        registro = session.query(Organo).filter(Organo.nivel1.isnot(None)).first()
        if registro is None:
            print("No hay datos disponibles para la prueba")
            return

        admin = registro.nivel1 or ""
        dep = registro.nivel2 or ""
        org = registro.nivel3 or ""

        con_acentos = encontrar_codigo_convocante(admin, dep, org, session=session)
        sin_acentos = encontrar_codigo_convocante(
            normalizar_texto(admin),
            normalizar_texto(dep) if dep else None,
            normalizar_texto(org) if org else None,
            session=session,
        )
        assert con_acentos == sin_acentos
        print("Prueba sin acentos superada para", con_acentos)

def procesar_archivo(ruta: Path, tipo_desc: str) -> None:
    """Procesa un archivo CSV mostrando la asignación de órganos.

    Abre su propia sesión de base de datos usando :class:`SessionLocal`.
    """
    with SessionLocal() as session:
        with ruta.open(encoding="utf-8-sig") as f:
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
                    administracion, departamento, organo
                )
                datos_organo = session.get(Organo, org_id) if org_id else None

                if datos_organo:
                    org_desc = f"{datos_organo.nombre} [{datos_organo.id}]"
                    log_func = logger.info
                else:
                    org_desc = "No encontrado"
                    log_func = logger.warning

                log_func(
                    "Convocatoria %s (%s) - Búsqueda del órgano con "
                    "nivel1:%s, nivel2:%s, nivel3:%s -> %s",

                )


def main():
    parser = argparse.ArgumentParser(description="Generar log de convocantes")
    parser.add_argument("ejercicio", type=int, help="Año de las convocatorias")
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="Ejecutar prueba de búsqueda sin acentos",
    )
    args = parser.parse_args()

    if args.selftest:
        test_busqueda_sin_acentos()
        return

    patron = str(CSV_DIR / f"convocatorias_*_{args.ejercicio}.csv")
    archivos = sorted(glob.glob(patron))
    if not archivos:
        logger.warning(
            "No se encontraron archivos para el ejercicio %s", args.ejercicio
        )
        return

    for archivo in archivos:
        prefijo = Path(archivo).stem.split("_")[1]
        tipo_desc = TIPOS.get(prefijo, "Desconocido")
        procesar_archivo(Path(archivo), tipo_desc)


if __name__ == "__main__":
    main()