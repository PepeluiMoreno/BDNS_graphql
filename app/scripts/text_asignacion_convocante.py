#!/usr/bin/env python3
"""Genera un log con el tipo de convocatoria y su órgano convocante.

Uso:
    python -m app.scripts.text_asignacion_convocante 2018
"""
import argparse
import csv
import glob
import logging
from pathlib import Path

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

def procesar_archivo(ruta: Path, tipo_desc: str):
    with ruta.open(encoding="latin-1") as f:
        _ = preprocess_line(f.readline())  # descartar cabecera
        for linea in f:
            if not linea.strip():
                continue
            fila = preprocess_line(linea)
            if len(fila) < 5:
                continue
            codigo = fila[0].strip()
            nivel1 = fila[2].strip()
            nivel2 = fila[3].strip()
            nivel3 = fila[4].strip() if len(fila) > 4 else ""

            if tipo_desc == "AUTONOMICA":
                # En las convocatorias autonómicas, ``nivel1`` es el nombre de la
                # comunidad y ``nivel2`` el del órgano administrativo dependiente.
                logging.info(
                    "La convocatoria %s es una convocatoria %s de %s - %s",
                    codigo,
                    tipo_desc,
                    nivel1,
                    nivel2 or nivel3,
                )
            else:
                logging.info(
                    "La convocatoria %s es una convocatoria %s de %s - %s-%s",
                    codigo,
                    tipo_desc,
                    nivel1,
                    nivel2,
                    nivel3,
                )


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
    for archivo in archivos:
        prefijo = Path(archivo).stem.split("_")[1]
        tipo_desc = TIPOS.get(prefijo, "Desconocido")
        procesar_archivo(Path(archivo), tipo_desc)


if __name__ == "__main__":
    main()
