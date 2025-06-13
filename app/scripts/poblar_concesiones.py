import csv
import io
import logging
from datetime import datetime
from db.session import SessionLocal
from db.models import Concesion, AyudaEstado, Minimis, Convocatoria
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = "https://www.pap.hacienda.gob.es/bdnstrans/api"
BATCH_SIZE = 500

def poblar_entidad(tipo, year, vpd="GE"):
    session = SessionLocal()
    try:
        url = f"{API_BASE_URL}/{tipo}/exportar"
        params = {
            "vpd": vpd,
            "tipoDoc": "csv",
            "fechaDesde": f"01/01/{year}",
            "fechaHasta": f"31/12/{year}",
            "pageSize": BATCH_SIZE
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        csv_data = io.StringIO(response.text)
        reader = csv.DictReader(csv_data, delimiter=';')

        for row in reader:
            try:
                codigoBDNS = int(row['id_convocatoria'])
                if not session.query(Convocatoria).get(codigoBDNS):
                    logger.warning(f"Convocatoria {codigoBDNS} no existe")
                    continue

                if tipo == 'concesiones':
                    entidad = Concesion(
                        codigoBDNS=codigoBDNS,
                        beneficiario=row['beneficiario'],
                        nif_beneficiario=row.get('nif_beneficiario'),
                        fecha_concesion=datetime.strptime(row['fecha_concesion'], '%d/%m/%Y').date(),
                        importe=float(row['importe'].replace(',', '.')) if row.get('importe') else None,
                        metadatos={k: v for k, v in row.items() if k not in [
                            'id_convocatoria', 'beneficiario', 'nif_beneficiario', 
                            'fecha_concesion', 'importe'
                        ]}
                    )
                elif tipo == 'ayudasestado':
                    entidad = AyudaEstado(
                        codigoBDNS=codigoBDNS,
                        beneficiario=row['beneficiario'],
                        nif_beneficiario=row.get('nif_beneficiario'),
                        fecha_concesion=datetime.strptime(row['fecha_concesion'], '%d/%m/%Y').date(),
                        importe=float(row['importe'].replace(',', '.')) if row.get('importe') else None,
                        metadatos={k: v for k, v in row.items() if k not in [
                            'id_convocatoria', 'beneficiario', 'nif_beneficiario', 
                            'fecha_concesion', 'importe'
                        ]}
                    )
                elif tipo == 'minimis':
                    entidad = Minimis(
                        codigoBDNS=codigoBDNS,
                        beneficiario=row['beneficiario'],
                        nif_beneficiario=row.get('nif_beneficiario'),
                        fecha_concesion=datetime.strptime(row['fecha_concesion'], '%d/%m/%Y').date(),
                        importe=float(row['importe'].replace(',', '.')) if row.get('importe') else None,
                        metadatos={k: v for k, v in row.items() if k not in [
                            'id_convocatoria', 'beneficiario', 'nif_beneficiario', 
                            'fecha_concesion', 'importe'
                        ]}
                    )

                session.add(entidad)
                
                if session.new % BATCH_SIZE == 0:
                    session.commit()
                    
            except Exception as e:
                logger.error(f"Error procesando {tipo}: {str(e)}")
                session.rollback()
                continue

        session.commit()
        logger.info(f"{tipo.capitalize()} para {year} importadas correctamente")

    except Exception as e:
        session.rollback()
        logger.error(f"Error crítico en {tipo}: {str(e)}")
    finally:
        session.close()

def poblar_concesiones(year, vpd="GE"):
    poblar_entidad('concesiones', year, vpd)

def poblar_ayudas_estado(year, vpd="GE"):
    poblar_entidad('ayudasestado', year, vpd)

def poblar_minimis(year, vpd="GE"):
    poblar_entidad('minimis', year, vpd)