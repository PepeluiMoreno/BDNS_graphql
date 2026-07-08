"""
Configuracion de la aplicacion BDNS Portal.

Usa PortalSettings de bdns_core para mantener consistencia.
"""
from bdns_core.config import get_portal_settings

# Re-exportar para compatibilidad con imports existentes
get_settings = get_portal_settings
settings = get_portal_settings()
