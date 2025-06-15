from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.models import Organo
from db.enums import TipoOrgano
from typing import Optional
import unicodedata

from app.db.session import SessionLocal


def normalize_text(text: str) -> str:
    """Return text without accents and in uppercase."""
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(
        c for c in normalized if not unicodedata.combining(c)
    )
    return without_accents.upper()

def encontrar_codigo_convocante(
    administracion: str,
    departamento: Optional[str] = None,
    organo: Optional[str] = None,
    session: Optional[Session] = None,

) -> Optional[str]:
    """Devuelve el ID del órgano convocante según los textos del CSV.

    La búsqueda principal compara los campos ``nivel1``, ``nivel2`` y
    ``nivel3`` de :class:`Organo` con los valores de ``administracion``,
    ``departamento`` y ``organo``.  Para convocatorias locales el CSV
    almacena el municipio en ``Administracion`` y el ayuntamiento en
    ``Departamento``.  En ese caso se intenta una búsqueda adicional
    usando ``nombre`` y ``nivel3`` del órgano.
    """

    if session is None:
        session = SessionLocal()
        close_session = True
    else:
        close_session = False

    if not administracion:
        if close_session:
            session.close()
        return None

    adm_norm = normalize_text(administracion).strip()

    query = session.query(Organo.id).filter(
        func.upper(func.unaccent(func.trim(Organo.nivel1))) == adm_norm
    )

    if departamento:
        dep_norm = normalize_text(departamento).strip()
        query = query.filter(
            func.upper(func.unaccent(func.trim(Organo.nivel2))) == dep_norm
        )

    if organo:
        org_norm = normalize_text(organo).strip()
        query = query.filter(
            func.upper(func.unaccent(func.trim(Organo.nivel3))) == org_norm
        )

    result = query.first()
    if result:
        if close_session:
            session.close()
        return result[0]


    # Fallback para órganos locales: Administracion = municipio,
    # Departamento = ayuntamiento, sin nivel2 en el CSV.
    if departamento:
        dep_norm = normalize_text(departamento).strip()
        local_query = session.query(Organo.id).filter(
            func.upper(func.unaccent(func.trim(Organo.nombre))) == dep_norm,
            func.upper(func.unaccent(func.trim(Organo.nivel3))) == adm_norm,
            Organo.tipo == TipoOrgano.LOCAL,
        )
        local_result = local_query.first()
        if local_result:
            if close_session:
                session.close()
            return local_result[0]

    if close_session:
        session.close()

    return None

      
