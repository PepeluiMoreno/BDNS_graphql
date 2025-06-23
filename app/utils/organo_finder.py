from sqlalchemy.orm import Session
from sqlalchemy import func

from db.models import Organo
from db.enums import TipoOrgano

from typing import Optional

from app.db.session import SessionLocal
from app.scripts.poblar_organos import normalizar

def encontrar_codigo_convocante(
    administracion: str,
    departamento: Optional[str] = None,
    organo: Optional[str] = None,
    session: Optional[Session] = None,
) -> Optional[str]:
    """Devuelve el ID del órgano convocante para los textos dados.

    Compara ``nivel1``, ``nivel2`` y ``nivel3`` de :class:`Organo` con
    ``administracion``, ``departamento`` y ``organo`` una vez
    normalizados.  Si no se proporciona una ``session`` se abre una
    nueva temporalmente.
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

    adm_norm = normalizar(administracion)


    query = session.query(Organo.id).filter(
        func.upper(func.unaccent(func.trim(Organo.nivel1))) == adm_norm
    )

    if departamento:
        dep_norm = normalizar(departamento)
        query = query.filter(
            func.upper(func.unaccent(func.trim(Organo.nivel2))) == dep_norm
        )

    if organo:
        org_norm = normalizar(organo)
        query = query.filter(
            func.upper(func.unaccent(func.trim(Organo.nivel3))) == org_norm
        )

    result = query.first()

    if close_session:
        session.close()

    return result[0] if result else None

      