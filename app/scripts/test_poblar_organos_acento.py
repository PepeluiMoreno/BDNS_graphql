import importlib
from pathlib import Path
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))
sys.path.append(str(BASE_DIR / "app"))

from app.db import models
from db.enums import TipoOrgano

# Crear motor SQLite en memoria para pruebas
engine = create_engine("sqlite:///:memory:")
SessionLocal = sessionmaker(bind=engine)
models.Base.metadata.create_all(engine)

# Recargar el módulo para que use la sesión de prueba
poblar = importlib.import_module("app.scripts.poblar_organos")
poblar.session = SessionLocal()
poblar.SessionLocal = SessionLocal


def main():
    nombre = "Dirección General de Pruebas"
    poblar.insertar_organo(
        {
            "id": "T1",
            "id_padre": None,
            "nombre": nombre,
            "tipo": TipoOrgano.CENTRAL,
            "nivel1": "ESTADO",
            "nivel2": nombre,
            "nivel3": None,
        }
    )
    res = poblar.session.get(models.Organo, "T1")
    if res and res.nombre == nombre:
        print("Nombre almacenado correctamente:", res.nombre)
    else:
        print("Error: se obtuvo", res.nombre if res else None)


if __name__ == "__main__":
    main()
