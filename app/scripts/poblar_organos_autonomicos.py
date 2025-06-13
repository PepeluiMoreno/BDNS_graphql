import os
import sys
import logging
import requests

from sqlalchemy.exc import SQLAlchemyError
from db.session import SessionLocal
from db.models import Organo

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Endpoint para comunidades autónomas y órganos dependientes
URL = "https://www.infosubvenciones.es/bdnstrans/api/organos?vpd=GE&idAdmon=A"

def limpiar_texto(texto):
    if not isinstance(texto, str):
        return texto
    return texto.replace('"', '').strip()

def poblar_organos_autonomicos():
    session = SessionLocal()

    try:
        response = requests.get(URL)
        response.encoding = 'utf-8'  # Forzar codificación

        if response.status_code == 204:
            logger.warning("Respuesta 204: sin contenido")
            return
        elif response.status_code != 200:
            logger.error("Entrypoint mal formado. Código de estado: %s", response.status_code)
            return

        datos = response.json()

        for comunidad in datos:
            nombre_comunidad = limpiar_texto(comunidad["descripcion"])
            id_comunidad = comunidad["id"]

            logger.info(f"Insertando comunidad: {nombre_comunidad}")

            # Insertar comunidad como órgano principal
            comunidad_obj = Organo(
                id=id_comunidad,
                descripcion=nombre_comunidad,
                id_padre=None,
                nivel1=id_comunidad,
                nivel2=None,
                nivel3=None,
                tipo='G'
            )
            session.merge(comunidad_obj)

            # Insertar órganos dependientes si los hay
            for hijo in comunidad.get("children", []):
                descripcion_hijo = limpiar_texto(hijo["descripcion"])
                id_hijo = hijo["id"]

                hijo_obj = Organo(
                    id=id_hijo,
                    descripcion=descripcion_hijo,
                    id_padre=id_comunidad,
                    nivel1=id_comunidad,
                    nivel2=id_hijo,
                    nivel3=None,
                    tipo='A'
                )
                session.merge(hijo_obj)

        session.commit()
        logger.info("Inserción completada con éxito.")

    except (requests.RequestException, SQLAlchemyError) as e:
        logger.error("Error durante la inserción: %s", e)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    poblar_organos_autonomicos()
