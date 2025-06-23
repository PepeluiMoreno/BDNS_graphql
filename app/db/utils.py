import unicodedata

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
