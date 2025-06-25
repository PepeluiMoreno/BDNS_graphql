
# retry_bloques_fallidos.py: Reintenta bloques que fallaron en fetch_convocatorias
import csv
import argparse
from pathlib import Path
import requests
import json
import time
from db.session import engine
from db.utils import normalizar, buscar_organo_id
from db.models import (
    SectorActividad, Instrumento, TipoBeneficiario, SectorProducto, Region,
    Finalidad, Objetivo, Reglamento, Fondo
)

RUTA_CSV = Path("data/debug/bloques_fallidos.csv")
RUTA_JSONS = Path("json/convocatorias")
RUTA_JSONS.mkdir(parents=True, exist_ok=True)
URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
TIPO_MAP = {"C": "estatales", "A": "autonomicas", "L": "locales", "O": "otras"}
CATALOGOS = {
    "instrumentos": Instrumento,
    "tiposBeneficiarios": TipoBeneficiario,
    "sectores": SectorActividad,
    "sectoresProducto": SectorProducto,
    "regiones": Region,
    "finalidades": Finalidad,
    "objetivos": Objetivo,
    "reglamentos": Reglamento,
    "fondos": Fondo,
}

def get_session():
    from sqlalchemy.orm import sessionmaker
    return sessionmaker(bind=engine)()

def normalizar_fila(row):
    tipo, mes, anio = row
    return tipo.strip(), int(mes), int(anio)

def leer_bloques_fallidos():
    if not RUTA_CSV.exists():
        print("No se encontró bloques_fallidos.csv. Nada que relanzar.")
        return []
    with open(RUTA_CSV, encoding="utf-8") as f:
        reader = csv.reader(f)
        return [normalizar_fila(row) for row in reader if len(row) == 3]

def safe_request(url, params, retries=3, delay=5):
    for intento in range(retries):
        try:
            r = requests.get(url, params=params, timeout=120)
            r.raise_for_status()
            return r
        except Exception as e:
            if intento < retries - 1:
                time.sleep(delay * (2 ** intento))
            else:
                raise e

def registrar_faltante(nombre_catalogo, descripcion):
    path = Path("data/debug") / f"faltantes_{nombre_catalogo}.csv"
    descripcion = descripcion.strip().strip('"')
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="utf-8") as f:
        f.write(f"{descripcion}\n")

def enriquecer_detalle(session, entrada):
    cod = entrada.get("numeroConvocatoria")
    url = f"{URL_BASE}/convocatorias?numConv={cod}"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        detalle = r.json()
    except Exception:
        return entrada

    for campo, modelo in CATALOGOS.items():
        valores = detalle.get(campo)
        if not valores:
            continue
        for item in valores:
            if "descripcion" in item:
                item["descripcion"] = item["descripcion"].strip().strip('"')
                norm = normalizar(item["descripcion"])
                existente = session.query(modelo).filter_by(descripcion_norm=norm).first()
            else:
                existente = session.query(modelo).get(item.get("id"))
            if existente:
                item["id"] = existente.id
            else:
                desc = item.get("descripcion") or str(item)
                if modelo in [SectorActividad, Fondo]:
                    registrar_faltante(modelo.__tablename__, desc)

    organo_id = buscar_organo_id(session, entrada.get("nivel1"), entrada.get("nivel2"), entrada.get("nivel3"))
    if organo_id:
        detalle["organo_id"] = organo_id
    entrada.update(detalle)
    return entrada

def procesar_bloque(tipo, mes, anio):
    fecha_desde = f"01/{mes:02d}/{anio}"
    fecha_hasta = f"31/12/{anio}" if mes == 12 else f"01/{mes + 1:02d}/{anio}"
    params = {
        "page": 0,
        "pageSize": 10000,
        "order": "numeroConvocatoria",
        "direccion": "asc",
        "fechaDesde": fecha_desde,
        "fechaHasta": fecha_hasta,
        "tipoAdministracion": tipo
    }
    url = f"{URL_BASE}/convocatorias/busqueda"
    try:
        r = safe_request(url, params)
        content = r.json().get("content", [])
    except Exception as e:
        print(f"ERROR al descargar {tipo}-{mes:02d}-{anio}: {e}")
        return False
    session = get_session()
    enriquecidos = [enriquecer_detalle(session, item) for item in content]
    session.close()
    nombre = f"convocatorias_{TIPO_MAP[tipo]}_{anio}.json"
    path = RUTA_JSONS / nombre
    if path.exists():
        with open(path, encoding="utf-8") as f:
            anteriores = json.load(f)
    else:
        anteriores = []
    anteriores.extend(enriquecidos)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(anteriores, f, ensure_ascii=False, indent=2)
    print(f"✔ {len(enriquecidos)} registros añadidos para {tipo}-{mes:02d}-{anio} en {nombre}")
    return True

def limpiar_csv():
    if RUTA_CSV.exists():
        RUTA_CSV.unlink()
        print("✔ CSV de bloques fallidos eliminado tras reprocesamiento exitoso.")

def main():
    parser = argparse.ArgumentParser(description="Reintentar bloques fallidos desde CSV")
    args = parser.parse_args()
    bloques = leer_bloques_fallidos()
    if not bloques:
        return
    fallidos_persistentes = []
    for tipo, mes, anio in bloques:
        ok = procesar_bloque(tipo, mes, anio)
        if not ok:
            fallidos_persistentes.append((tipo, mes, anio))
    if fallidos_persistentes:
        with open(RUTA_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(fallidos_persistentes)
        print("⚠ Algunos bloques siguen fallando, mantenidos en bloques_fallidos.csv")
    else:
        limpiar_csv()

if __name__ == "__main__":
    main()
