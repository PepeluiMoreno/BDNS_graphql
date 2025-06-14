from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.models import Organo
from db.enums import TipoOrgano
from typing import Optional


def encontrar_codigo_convocante(
    session: Session,
    administracion: str,
    departamento: Optional[str] = None,
    organo: Optional[str] = None,
) -> Optional[str]:
    """Devuelve el ID del órgano convocante según los textos del CSV.

    La búsqueda principal compara los campos ``nivel1``, ``nivel2`` y
    ``nivel3`` de :class:`Organo` con los valores de ``administracion``,
    ``departamento`` y ``organo``.  Para convocatorias locales el CSV
    almacena el municipio en ``Administracion`` y el ayuntamiento en
    ``Departamento``.  En ese caso se intenta una búsqueda adicional
    usando ``nombre`` y ``nivel3`` del órgano.
    """
    if not administracion:
        return None

    query = session.query(Organo.id).filter(
        func.upper(func.trim(Organo.nivel1)) == administracion.strip().upper()
    )

    if departamento:
        query = query.filter(
            func.upper(func.trim(Organo.nivel2)) == departamento.strip().upper()
        )

    if organo:
        query = query.filter(
            func.upper(func.trim(Organo.nivel3)) == organo.strip().upper()
        )

    result = query.first()
    if result:
        return result[0]

    # Fallback para órganos locales: Administracion = municipio,
    # Departamento = ayuntamiento, sin nivel2 en el CSV.
    if departamento:
        local_query = session.query(Organo.id).filter(
            func.upper(func.trim(Organo.nombre)) == departamento.strip().upper(),
            func.upper(func.trim(Organo.nivel3)) == administracion.strip().upper(),
            Organo.tipo == TipoOrgano.LOCAL,
        )
        local_result = local_query.first()
        if local_result:
            return local_result[0]

    return None
