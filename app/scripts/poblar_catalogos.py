# poblar_catalogos.py
import requests
import logging
import csv
from sqlalchemy.exc import IntegrityError
from db.session import SessionLocal, engine
from db.utils import normalizar
from db.models import (
    Base,
    Finalidad,
    Instrumento,
    Objetivo,
    Region,
    Reglamento,
    SectorActividad,
    SectorProducto,
    TipoBeneficiario,
)
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
VPD = "GE"

def poblar_catalogo(session, Model, endpoint, extra_params=None):
    try:
        url = f"{API_BASE}/{endpoint}"
        if extra_params:
            url += "?" + "&".join([f"{k}={v}" for k, v in extra_params.items()])
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            obj = Model(
                id=item["id"],
                descripcion=item["descripcion"],
                descripcion_norm=normalizar(item["descripcion"]),
            )
            session.merge(obj)
        session.commit()
        logger.info(f"{Model.__name__} insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar {Model.__name__}: {e}")

def poblar_regiones(session):
    def insertar_region(item, id_padre=None):
        region = Region(
            id=item["id"],
            descripcion=item["descripcion"],
            descripcion_norm=normalizar(item["descripcion"]),
            id_padre=id_padre,
        )
        session.merge(region)
        for hijo in item.get("children", []):
            insertar_region(hijo, id_padre=item["id"])

    try:
        url = f"{API_BASE}/regiones"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            insertar_region(item)
        session.commit()
        logger.info("Regiones insertadas/actualizadas.")
    except Exception as e:
        logger.exception("Error al poblar regiones: %s", e)

def poblar_sector_actividad_desde_csv(session, ruta_csv):
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            items = list(reader)

        nodos = {}

        for row in items:
            id = row["CODINTEGR"].strip()
            descripcion = row["TITULO_CNAE2009"].strip()
            norm = normalizar(descripcion)

            if id not in nodos:
                nodos[id] = SectorActividad(
                    id=id,
                    descripcion=descripcion,
                    descripcion_norm=norm
                )

        for id, sector in nodos.items():
            if len(id) == 1:
                continue  # Sección (raíz)
            elif len(id) == 3:
                sector.id_padre = id[0]  # División → Sección
            elif len(id) == 4:
                sector.id_padre = id[:3]  # Grupo → División
            elif len(id) == 5:
                sector.id_padre = id[:4]  # Clase → Grupo

        for id, sector in sorted(nodos.items(), key=lambda x: len(x[0])):
            try:
                session.add(sector)
                session.flush()
            except IntegrityError:
                session.rollback()
                logger.warning(f"SectorActividad duplicado ignorado: {id}")

        session.commit()
        logger.info("Sectores de actividad insertados desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar sectores de actividad desde CSV: {e}")

def main():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as session:
        poblar_sector_actividad_desde_csv(session, "data/INE/estructura_cnae2009.csv")
        poblar_catalogo(session, Instrumento, "instrumentos")
        poblar_catalogo(session, TipoBeneficiario, "beneficiarios", {"vpd": VPD})
        poblar_catalogo(session, SectorProducto, "sectores")
        poblar_regiones(session)
        poblar_catalogo(session, Finalidad, "finalidades", {"vpd": VPD})
        poblar_catalogo(session, Objetivo, "objetivos")
        poblar_catalogo(session, Reglamento, "reglamentos")

if __name__ == "__main__":
    main()

