from sqlalchemy.orm import Session
from sqlalchemy import func
from db.models import Organo
from db.enums import TipoOrgano
from typing import Optional


from app.db.session import SessionLocal
from app.scripts.poblar_organos import normalizar_texto


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

    adm_norm = normalizar_texto(administracion)


    query = session.query(Organo.id).filter(
        func.upper(func.unaccent(func.trim(Organo.nivel1))) == adm_norm
    )

    if departamento:
        dep_norm = normalizar_texto(departamento)
        query = query.filter(
            func.upper(func.unaccent(func.trim(Organo.nivel2))) == dep_norm
        )

    if organo:
        org_norm = normalizar_texto(organo)
        query = query.filter(
            func.upper(func.unaccent(func.trim(Organo.nivel3))) == org_norm
        )

    result = query.first()
    if result:
        if close_session:
            session.close()
        return result[0]

    candidatos = session.query(Organo).all()
    for cand in candidatos:
        if normalizar_texto(cand.nivel1) != normalizar_texto(administracion):
            continue
        if departamento and normalizar_texto(cand.nivel2) != normalizar_texto(departamento):
            continue
        if organo and normalizar_texto(cand.nivel3) != normalizar_texto(organo):
            continue
        if close_session:
            session.close()
        return cand.id

    # Fallback para órganos locales: Administracion = municipio,
    # Departamento = ayuntamiento, sin nivel2 en el CSV.
    if departamento:

        dep_norm = normalizar_texto(departamento)
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

        for cand in session.query(Organo).filter(Organo.tipo == TipoOrgano.LOCAL).all():
            if normalizar_texto(cand.nombre) == normalizar_texto(departamento) and \
               normalizar_texto(cand.nivel3) == normalizar_texto(administracion):
                if close_session:
                    session.close()

                return cand.id

    if close_session:
        session.close()

    return None

      