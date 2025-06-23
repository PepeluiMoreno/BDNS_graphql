import requests
import logging
from db.session import SessionLocal,engine
from db.utils import normalizar as normalizar
from db.models import (
    Base, Actividad,  Instrumento, TipoBeneficiario,
    Sector, Region, Finalidad, Objetivo, Reglamento
)
# Añadir la carpeta 'project_root' al sys.path para poder importar 'app'
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
VPD = "GE"

def poblar_actividades(session):
    try:
        url = f"{API_BASE}/actividades"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            inst = Instrumento(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(inst)
        session.commit()
        logger.info("Acrividades insertadas/actualizadas.")
    except Exception as e:
        logger.exception(f"Error al poblar actividades: {e}")

def poblar_instrumentos(session):
    try:
        url = f"{API_BASE}/instrumentos"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            inst = Instrumento(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(inst)
        session.commit()
        logger.info("Instrumentos insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar instrumentos: {e}")

def poblar_tipos_beneficiario(session):
    try:
        url = f"{API_BASE}/beneficiarios?vpd={VPD}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            tb = TipoBeneficiario(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(tb)
        session.commit()
        logger.info("Tipos de beneficiario insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar tipos de beneficiario: {e}")

def poblar_sectores(session):
    try:
        url = f"{API_BASE}/sectores"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            sector = Sector(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(sector)
        session.commit()
        logger.info("Sectores insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar sectores: {e}")

def poblar_regiones(session):
    try:
        url = f"{API_BASE}/regiones"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            region = Region(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(region)
        session.commit()
        logger.info("Regiones insertadas/actualizadas.")
    except Exception as e:
        logger.exception(f"Error al poblar regiones: {e}")

def poblar_finalidades(session):
    try:
        url = f"{API_BASE}/finalidades?vpd={VPD}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            finalidad = Finalidad(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(finalidad)
        session.commit()
        logger.info("Finalidades insertadoas/actualizadas.")
    except Exception as e:
        logger.exception(f"Error al poblar finalidades: {e}")

def poblar_objetivos(session):
    try:
        url = f"{API_BASE}/objetivos"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            objetivo = Objetivo(
                id=item['id'],
                descripcion=item['descripcion']
            )
            session.merge(objetivo)
        session.commit()
        logger.info("Objetivos insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar objetivos: {e}")

def poblar_reglamentos(session):
    try:
        url = f"{API_BASE}/reglamentos"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            reglamento = Reglamento(
                id=item['id'],
                descripcion=item['descripcion'],
                autorizacion=item.get('autorizacion')
            )
            session.merge(reglamento)
        session.commit()
        logger.info("Reglamentos insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar reglamentos: {e}")

def main():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as session:
        poblar_actividades(session)
        poblar_instrumentos(session)
        poblar_tipos_beneficiario(session)
        poblar_sectores(session)
        poblar_regiones(session)
        poblar_finalidades(session)
        poblar_objetivos(session)
        poblar_reglamentos(session)

if __name__ == "__main__":
    main()
