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
from datetime import datetime

from app.db.session import SessionLocal
from app.db.models import Organo
from app.utils.organo_finder import (
    encontrar_codigo_convocante,
    normalize_text,
)

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger("test_asignacion_convocante")

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
            normalize_text(admin),
            normalize_text(dep) if dep else None,
            normalize_text(org) if org else None,
            session=session,
        )
        assert con_acentos == sin_acentos
        print("Prueba sin acentos superada para", con_acentos)

def procesar_archivo(ruta: Path, tipo_desc: str) -> None:
    """Procesa un archivo CSV mostrando la asignación de órganos.

    Abre su propia sesión de base de datos usando :class:`SessionLocal`.
    """
    with SessionLocal() as session:
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
                    administracion, departamento, organo
                )
                datos_organo = None
                if org_id:
                    datos_organo = session.get(Organo, org_id)
                if datos_organo:
                    org_desc = f"{datos_organo.nombre} [{datos_organo.id}]"
                else:
                    org_desc = "No encontrado"

                logger.info(
                    "Convocatoria %s (%s) -> %s - %s - %s | Órgano: %s",
                    codigo,
                    tipo_desc,
                    administracion,
                    departamento,
                    organo,
                    org_desc,
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
