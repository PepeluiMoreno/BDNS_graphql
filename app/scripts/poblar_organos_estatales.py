import logging
import requests

from sqlalchemy.exc import SQLAlchemyError
from db.session import SessionLocal
from db.models import Organo

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = "https://www.infosubvenciones.es/bdnstrans/api/organos?vpd=GE&idAdmon=C"

def limpiar_texto(texto):
    if not isinstance(texto, str):
        return texto
    return texto.replace('"', '').strip()

def poblar_organos_estatales():
    session = SessionLocal()

    try:
        response = requests.get(URL)
        response.encoding = 'utf-8'

        if response.status_code == 204:
            logger.warning("Respuesta 204: sin contenido")
            return
        elif response.status_code != 200:
            logger.error("Error HTTP: %s", response.status_code)
            return

        datos = response.json()

        # Insertar nodo raíz "ESTADO"
        logger.info("Insertando nodo raíz ESTADO")
        estado_obj = Organo(
            id=0,
            descripcion="ESTADO",
            id_padre=None,
            nivel1=0,
            nivel2=None,
            nivel3=None,
            tipo='G'
        )
        session.add(estado_obj)
        session.flush()  # Asegura que ESTADO exista antes de usarlo como id_padre


        for ministerio in datos:
            nombre_ministerio = limpiar_texto(ministerio["descripcion"])
            id_ministerio = ministerio["id"]

            logger.info(f"Insertando ministerio: {nombre_ministerio}")

            ministerio_obj = Organo(
                id=id_ministerio,
                descripcion=nombre_ministerio,
                id_padre=0,
                nivel1=0,
                nivel2=id_ministerio,
                nivel3=None,
                tipo='C'
            )
            session.merge(ministerio_obj)

            for hijo in ministerio.get("children", []):
                nombre_organo = limpiar_texto(hijo["descripcion"])
                id_organo = hijo["id"]

                logger.info(f"  Insertando órgano: {nombre_organo}")

                organo_obj = Organo(
                    id=id_organo,
                    descripcion=nombre_organo,
                    id_padre=id_ministerio,
                    nivel1=0,
                    nivel2=id_ministerio,
                    nivel3=id_organo,
                    tipo='C'
                )
                session.merge(organo_obj)

        session.commit()
        logger.info("Órganos estatales insertados correctamente.")

    except (requests.RequestException, SQLAlchemyError) as e:
        logger.error("Error durante la inserción: %s", e)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    poblar_organos_estatales()
