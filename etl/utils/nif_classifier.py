import re
from typing import Optional

def classify_nif(nif: Optional[str]) -> str:
    """
    Clasifica el tipo de entidad basado en el NIF.
    
    Reglas de clasificación:
    - A: Sociedades Anónimas
    - B: Sociedades de Responsabilidad Limitada
    - C: Sociedades Colectivas
    - D: Sociedades Comanditarias
    - E: Comunidades de Bienes
    - F: Sociedades Cooperativas
    - G: Asociaciones
    - H: Comunidades de Propietarios
    - J: Sociedades Civiles
    - K: Españoles menores de 14 años
    - L: Españoles residentes en el extranjero
    - M: NIF para extranjeros sin NIE
    - N: Entidades extranjeras
    - P: Corporaciones Locales
    - Q: Organismos Públicos
    - R: Congregaciones e Instituciones Religiosas
    - S: Órganos de la Administración
    - U: Uniones Temporales de Empresas
    - V: Otros tipos no definidos
    - W: Establecimientos permanentes de entidades no residentes
    """
    if not nif:
        return "desconocido"
    
    # Limpiar el NIF
    nif = nif.strip().upper()
    
    # Si está anonimizado (caso de personas físicas)
    if re.match(r'^[*]+$', nif) or len(nif) < 2:
        return "persona_fisica"
    
    # Obtener la primera letra o número
    first_char = nif[0]
    
    # Clasificar según la primera letra
    if first_char == 'A':
        return "sociedad_anonima"
    elif first_char == 'B':
        return "sociedad_limitada"
    elif first_char == 'C':
        return "sociedad_colectiva"
    elif first_char == 'D':
        return "sociedad_comanditaria"
    elif first_char == 'E':
        return "comunidad_bienes"
    elif first_char == 'F':
        return "sociedad_cooperativa"
    elif first_char == 'G':
        return "asociacion"
    elif first_char == 'H':
        return "comunidad_propietarios"
    elif first_char == 'J':
        return "sociedad_civil"
    elif first_char in ['K', 'L', 'M']:
        return "persona_fisica"
    elif first_char == 'N':
        return "entidad_extranjera"
    elif first_char == 'P':
        return "corporacion_local"
    elif first_char == 'Q':
        return "organismo_publico"
    elif first_char == 'R':
        return "entidad_religiosa"
    elif first_char == 'S':
        return "organo_administracion"
    elif first_char == 'U':
        return "ute"
    elif first_char == 'V':
        return "otro_tipo_entidad"
    elif first_char == 'W':
        return "establecimiento_no_residente"
    else:
        # Si no coincide con ninguna letra específica
        return "desconocido"
