import os
import csv
import logging
import requests
import unicodedata
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_
from db.session import SessionLocal
from db.models import Organo
from db.utils import normalizar
from db.enums import TipoOrgano
from datetime import datetime
from sqlalchemy.orm.exc import NoResultFound 

# ─────────────────────────────────────────────────────────────
# Configuración de rutas y logging
# ─────────────────────────────────────────────────────────────

BASE_URL = "https://www.infosubvenciones.es/bdnstrans/api/organos?vpd=GE&idAdmon="
BASE_DIR = Path(__file__).resolve().parent.parent

OUTPUT_DIR = BASE_DIR / 'csv'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_filename = LOG_DIR / f'poblar_organos_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)



# ─────────────────────────────────────────────────────────────
# Datos base y constantes
# ─────────────────────────────────────────────────────────────

PROVINCIAS_CSV = OUTPUT_DIR / "provincias.csv"

MAPA_PROVINCIAS = {
    "ANDALUCIA": ["ALMERIA", "CADIZ", "CORDOBA", "GRANADA", "HUELVA", "JAEN", "MALAGA", "SEVILLA"],
    "ARAGON": ["HUESCA", "TERUEL", "ZARAGOZA"],
    "CANARIAS": ["PALMAS", "LAS PALMAS", "TENERIFE", "SANTA CRUZ DE TENERIFE"],
    "CANTABRIA": ["CANTABRIA"],
    "CASTILLA Y LEON": ["AVILA", "BURGOS", "LEON", "PALENCIA", "SALAMANCA", "SEGOVIA", "SORIA", "VALLADOLID", "ZAMORA"],
    "CASTILLA-LA MANCHA": ["ALBACETE", "CIUDAD REAL", "CUENCA", "GUADALAJARA", "TOLEDO"],
    "CATALUNA": ["BARCELONA", "GIRONA", "LLEIDA", "TARRAGONA"],
    "CIUDAD AUTONOMA DE MELILLA": ["MELILLA"],
    "COMUNIDAD DE MADRID": ["MADRID"],
    "COMUNIDAD FORAL DE NAVARRA": ["NAFARROA / NAVARRA"],
    "COMUNITAT VALENCIANA": ["ALACANT / ALICANTE",  "CASTELLO / CASTELLON", "VALENCIA / VALENCIA"],
    "CUIDAD AUTONOMA DE CEUTA": ["CEUTA"],
    "EXTREMADURA": ["BADAJOZ", "CACERES"],
    "GALICIA": ["A CORUNA", "CORUNA", "LUGO", "OURENSE", "PONTEVEDRA"],
    "ILLES BALEARS": ["ILLES BALEARS / ISLAS BALEARES"],
    "LA RIOJA": ["LA RIOJA"],
    "PAIS VASCO": ["ARABA / ALAVA", "BIZKAIA", "VIZCAYA", "GIPUZKOA", "GUIPUZCOA"],
    "PRINCIPADO DE ASTURIAS": ["ASTURIAS"],
    "REGION DE MURCIA": ["MURCIA"],
}

session = SessionLocal()

def formar_id(tipo: TipoOrgano, id_num: int) -> str:
    return f"{tipo.value}{id_num}"

def generar_id_unico(session, id_base):
    nuevo_id = id_base
    contador = 1
    while session.get(Organo, nuevo_id):  # <- más moderno
        nuevo_id = f"{id_base}_{contador}"
        contador += 1
    return nuevo_id

def insertar_organo(organo_dict):
    
    id_ = organo_dict['id']
    existente = session.query(Organo).filter(Organo.id == id_).one_or_none()

    if existente:
        conflicto = []
        for campo, valor in organo_dict.items():
            valor_existente = getattr(existente, campo, None)
            if valor_existente != valor:
                conflicto.append((campo, valor_existente, valor))

        if conflicto:
            # Limpiar registro existente
            existente_dict = {k: v for k, v in vars(existente).items() if k != '_sa_instance_state'}
            logger.info(
                f"Conflicto de ID: intento insertar {organo_dict} pero ya existe {existente_dict}"
            )
            nuevo_id = generar_id_unico(session, id_)
            logger.info(f"Se asigna nuevo ID con sufijo incremental: {nuevo_id}")
            organo_dict['id'] = nuevo_id
            return insertar_organo(organo_dict)
        else:
            logger.debug(f"Órgano ya existente sin diferencias: ID='{id_}', nombre='{organo_dict.get('nombre')}'")
            return id_

    nuevo = Organo(**organo_dict)
    session.add(nuevo)
    logger.info(
        f"Órgano insertado correctamente: ID='{nuevo.id}', nombre='{nuevo.nombre}', tipo='{nuevo.tipo.name}', "
        f"id_padre='{nuevo.id_padre}'"
    )
    session.commit()
    return organo_dict['id']

def insertar_geografico(id_num, id_padre, nombre):
    id_ = formar_id(TipoOrgano.GEOGRAFICO, id_num)
    id_padre_ = id_padre if id_padre != -1 else None

    insertar_organo({
        'id': id_,
        'id_padre': id_padre_,
        'nombre': nombre,
        'tipo': TipoOrgano.GEOGRAFICO,
       })
    session.commit()
    logger.info(f"Órgano geográfico insertado: ID='{id_}', nombre='{nombre}'")
    
def procesar_autonomicas():
    logger.info("Procesando órganos autonómicos...")
    resp = requests.get(BASE_URL + "A")
    resp.raise_for_status()
    comunidades = resp.json()
    #En el primer nivel estan las comunidades autónomas, y en el segundo nivel los órganos autonómicos
    for comunidad in comunidades:
        comunidad_id = formar_id(TipoOrgano.GEOGRAFICO, (comunidad['id']))
        comunidad_nombre = comunidad['descripcion']
        comunidad_id_padre = 'G0'  # ID del nodo raíz geográfico
        insertar_geografico(comunidad['id'], comunidad_id_padre, comunidad_nombre)
        logger.info(
            f"Insertada la comunidad autónoma: {comunidad_nombre} (ID: {comunidad_id})"
        )
        
        # En el segundo nivel, insertamos los órganos autonómicos
        logger.info (f"Insertando los órganos autonómicos de {comunidad_nombre} (ID: {comunidad_id})")
        for hijo in comunidad.get("children", []):
            hijo_id = formar_id(TipoOrgano.AUTONOMICO, hijo['id'])
            hijo_nombre = hijo['descripcion']
            insertar_organo({
                'id': hijo_id,
                'nombre': hijo['descripcion'],
                'id_padre': comunidad_id,
                'tipo': TipoOrgano.AUTONOMICO,
                'nivel1': comunidad_nombre,
                'nivel2': hijo_nombre,
                'nivel3': None,
                'nivel1_norm': normalizar(comunidad_nombre),
                'nivel2_norm': normalizar(hijo_nombre),  
                'nivel3_norm': None

            })
    logger.info("Terminado el proceso de órganos autonómicos...")
    
def procesar_estado():
    logger.info("Procesando órganos estatales...")
    resp = requests.get(BASE_URL + "C")
    resp.raise_for_status()
    ministerios = resp.json()
    id_padre_raiz = formar_id(TipoOrgano.GEOGRAFICO, 0)
    
    for ministerio in ministerios:
        min_id = formar_id(TipoOrgano.CENTRAL, ministerio['id'])
        min_nombre = ministerio['descripcion']
        insertar_organo({
            'id': min_id,
            'nombre': min_nombre,
            'id_padre': id_padre_raiz,
            'tipo': TipoOrgano.CENTRAL,
            'nivel1': 'ESTADO',
            'nivel2': min_nombre,
            'nivel3': None,
            'nivel1_norm': 'ESTADO',
            'nivel2_norm': normalizar(min_nombre),
            'nivel3_norm': None
        })
        for hijo in ministerio.get("children", []):
            hijo_id = formar_id(TipoOrgano.CENTRAL, hijo['id'])
            hijo_nombre = hijo['descripcion']
            insertar_organo({
                'id': hijo_id,
                'nombre': hijo_nombre,
                'id_padre': min_id,
                'tipo': TipoOrgano.CENTRAL,
                'nivel1': 'ESTADO',
                'nivel2': min_nombre,
                'nivel3': hijo_nombre,
                'nivel1_norm': 'ESTADO',
                'nivel2_norm': normalizar(min_nombre),
                'nivel3_norm': normalizar(hijo_nombre)
            })

def procesar_otros():
    logger.info("Procesando otros órganos ...")
    resp = requests.get(BASE_URL + "O")
    resp.raise_for_status()
    otros = resp.json()
    id_padre_raiz = formar_id(TipoOrgano.GEOGRAFICO, 0)
    
    for otro in otros:
        otro_id = formar_id(TipoOrgano.OTRO, otro['id'])
        otro_nombre = otro['descripcion']
        insertar_organo({
            'id': otro_id,
            'nombre': otro_nombre,
            'id_padre': id_padre_raiz,
            'tipo': TipoOrgano.OTRO,
            'nivel1': 'OTROS',
            'nivel2': otro_nombre,
            'nivel3': None,
            'nivel1_norm': 'OTROS',
            'nivel2_norm': normalizar(otro_nombre),   
            'nivel3_norm': None
            
        })
        for hijo in otro.get("children", []):
            hijo_id = formar_id(TipoOrgano.OTRO, hijo['id'])
            hijo_nombre = hijo['descripcion']
            insertar_organo({
                'id': hijo_id,
                'nombre': hijo_nombre,
                'id_padre': otro_id,
                'tipo': TipoOrgano.OTRO,
                'nivel1': 'OTROS',
                'nivel2': otro_nombre,
                'nivel3': hijo_nombre,
                'nivel1_norm': 'OTROS',
                'nivel2_norm': normalizar(otro_nombre),
                'nivel3_norm': normalizar(hijo_nombre)
            })
            
def procesar_locales():
    logger.info("Procesando órganos locales (provincias, municipios, ayuntamientos)...")
    try:
        resp = requests.get(BASE_URL + "L")
        resp.raise_for_status()
        provincias = resp.json()
        logger.info(f"Se han recibido {len(provincias)} provincias desde la API.")
    except Exception as e:
        logger.error(f"Error al obtener provincias locales desde la API: {e}")
        provincias = []

    for provincia in provincias:
        nombre_prov = provincia['descripcion']
        nombre_prov_norm = normalizar(nombre_prov)
        id_prov = provincia['id']
        id_prov_alfa = formar_id(TipoOrgano.GEOGRAFICO, id_prov)
        # Buscar la comunidad autónoma (padre) para la provincia
        id_ccaa_alfa = None
        ccaa_nombre = None
        for ccaa, lista_provs in MAPA_PROVINCIAS.items():
            lista_norm = [normalizar(p) for p in lista_provs]
            if nombre_prov_norm in lista_norm:
                ccaa_nombre = ccaa
                ccaa_obj = None
                for posible in session.query(Organo).filter(Organo.tipo == TipoOrgano.GEOGRAFICO):
                    if normalizar(posible.nombre) == normalizar(ccaa):
                        ccaa_obj = posible
                        break
                if ccaa_obj:
                    id_ccaa_alfa = ccaa_obj.id
                break

        insertar_organo({
            'id': id_prov_alfa,
            'nombre': nombre_prov,
            'id_padre': id_ccaa_alfa,
            'tipo': TipoOrgano.GEOGRAFICO,
            'nivel1': ccaa_nombre,
            'nivel2': nombre_prov,
            'nivel3': None,
            'nivel1_norm': normalizar(ccaa_nombre) if ccaa_nombre else None,
            'nivel2_norm': nombre_prov_norm,        
            'nivel3_norm': None
        })

        # --- Inserta municipios (también tipo GEOGRAFICO) ---
        for municipio in provincia.get('children', []):
            nombre_muni = municipio['descripcion']
            id_muni = municipio['id']
            id_muni_alfa = formar_id(TipoOrgano.GEOGRAFICO, id_muni)
            insertar_organo({
                'id': id_muni_alfa,
                'nombre': nombre_muni,
                'id_padre': id_prov_alfa,
                'tipo': TipoOrgano.GEOGRAFICO,
                'nivel1': ccaa_nombre,
                'nivel2': nombre_prov,
                'nivel3': nombre_muni,
                'nivel1_norm': normalizar(ccaa_nombre) if ccaa_nombre else None,
                'nivel2_norm': nombre_prov_norm,
                'nivel3_norm': normalizar(nombre_muni)
            })

            # --- Inserta ayuntamientos (tipo LOCAL) ---
            for ayto in municipio.get('children', []):
                nombre_ayto = ayto['descripcion']
                id_ayto = ayto['id']
                id_ayto_alfa = formar_id(TipoOrgano.LOCAL, id_ayto)
                insertar_organo({
                    'id': id_ayto_alfa,
                    'nombre': nombre_ayto,
                    'id_padre': id_muni_alfa,
                    'tipo': TipoOrgano.LOCAL,
                    'nivel1': nombre_muni,
                    'nivel2': nombre_ayto,
                    'nivel3': None,
                    'nivel1_norm': normalizar(nombre_muni),
                    'nivel2_norm': normalizar(nombre_ayto),
                    'nivel3_norm': None
                })


def main():
    logger.info("Inicio del poblamiento de órganos...")

    id_raiz_geo = formar_id(TipoOrgano.GEOGRAFICO, 0)

    if not session.query(Organo).filter(Organo.id == id_raiz_geo).first():
        try:
            insertar_organo({
                'id': id_raiz_geo,
                'id_padre': None,
                'nombre': "ESTADO",
                'tipo': TipoOrgano.GEOGRAFICO,
                 })
            session.commit()
            logger.info("Nodo raíz geográfico 'G0' insertado correctamente.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error al insertar el nodo raíz geográfico 'G0': {e}")

    try:
        procesar_estado()
        session.commit()
        procesar_autonomicas()
        session.commit()
        procesar_locales()
        session.commit()
        procesar_otros()
        session.commit()
        logger.info("Poblamiento completado y cambios guardados.")
        
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error al guardar cambios en la BD: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()


