import os
import csv
import requests
import logging
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from app.db.models import Organo
from app.db.session import SessionLocal

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROVINCIAS_CSV = Path(__file__).resolve().parent / "provincias.csv"

def cargar_mapeo_provincias():
    mapeo = {}
    with open(PROVINCIAS_CSV, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            mapeo[row["id_provincia"]] = row["id_comunidad"]
    return mapeo

def insertar_organo(session, organo_data):
    try:
        session.merge(Organo(**organo_data))
    except SQLAlchemyError as e:
        logger.error(f"Error al insertar órgano: {organo_data}")
        logger.exception(e)

def poblar_organismos_locales():
    url = "https://www.infosubvenciones.es/bdnstrans/api/organos?vpd=GE&idAdmon=L"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    mapeo_provincias = cargar_mapeo_provincias()

    with SessionLocal() as session:
        for provincia in data:
            id_provincia = provincia["id"]
            nombre_provincia = provincia["descripcion"]
            id_comunidad = int(mapeo_provincias.get(str(id_provincia), 0))

            logger.info(f"Insertando provincia: {nombre_provincia}")
            insertar_organo(session, {
                "id": id_provincia,
                "descripcion": nombre_provincia,
                "id_padre": id_comunidad or None,
                "nivel1": id_provincia,
                "nivel2": None,
                "nivel3": None,
                "tipo": "L"
            })

            for municipio in provincia.get("children", []):
                id_municipio = municipio["id"]
                nombre_municipio = municipio["descripcion"]

                logger.info(f"  Insertando municipio: {nombre_municipio}")
                insertar_organo(session, {
                    "id": id_municipio,
                    "descripcion": nombre_municipio,
                    "id_padre": id_provincia,
                    "nivel1": id_provincia,
                    "nivel2": None,
                    "nivel3": None,
                    "tipo": "G"
                })

                for organo in municipio.get("children", []):
                    id_organo = organo["id"]
                    nombre_organo = organo["descripcion"]

                    logger.info(f"    Insertando órgano local: {nombre_organo}")
                    insertar_organo(session, {
                        "id": id_organo,
                        "descripcion": nombre_organo,
                        "id_padre": id_municipio,
                        "nivel1": id_municipio,
                        "nivel2": id_organo,
                        "nivel3": None,
                        "tipo": "L"
                    })

        session.commit()
        logger.info("Poblamiento de órganos locales completado.")

if __name__ == "__main__":
    poblar_organismos_locales()
