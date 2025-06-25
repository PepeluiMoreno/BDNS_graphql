# db/utils.py

import re
import unicodedata

def normalizar(texto):
    if not texto:
        return None
    # Eliminar tildes y pasar a ASCII
    texto = unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("ASCII")
    texto = texto.upper().strip()
    # Reemplazar múltiples espacios por uno solo
    texto = re.sub(r"\s+", " ", texto)
    # Quitar espacios antes de puntuación (opcional, si fuera un problema)
    texto = re.sub(r"\s+([.,:;])", r"\1", texto)
    return texto

from db.models import Organo
def buscar_organo_id(session, nivel1, nivel2, nivel3=None):
    n1 = normalizar(nivel1) if nivel1 else None
    n2 = normalizar(nivel2) if nivel2 else None
    n3 = normalizar(nivel3) if nivel3 else None

    query = session.query(Organo)
    if n3:
        organo = query.filter_by(nivel1_norm=n1, nivel2_norm=n2, nivel3_norm=n3).first()
    else:
        organo = query.filter_by(nivel1_norm=n1, nivel2_norm=n2).first()

    return organo.id if organo else None
