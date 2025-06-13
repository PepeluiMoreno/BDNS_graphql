from pathlib import Path
from sqlalchemy.orm import Session

from db.models import Organo
from db.session import SessionLocal


def obtener_arbol(session: Session, organo_id=None):
    """Obtiene la jerarquía de órganos en forma de árbol."""
    query = session.query(Organo).filter(Organo.id_padre == organo_id)
    arbol = []
    for organo in query:
        hijos = obtener_arbol(session, organo.id)
        arbol.append({
            "id": organo.id,
            "nombre": organo.nombre,
            "tipo": organo.tipo.value,
            "nivel1": organo.nivel1,
            "nivel2": organo.nivel2,
            "nivel3": organo.nivel3,
            "hijos": hijos,
        })
    return arbol


if __name__ == "__main__":
    import json

    OUTPUT_FILE = Path(__file__).resolve().parent.parent / "arbol_organos.json"

    with SessionLocal() as session:
        estructura = obtener_arbol(session)
        json_data = json.dumps(estructura, indent=2, ensure_ascii=False)

    with OUTPUT_FILE.open("w", encoding="utf-8") as fh:
        fh.write(json_data)

    print(f"Jerarqu\xeda guardada en {OUTPUT_FILE}")