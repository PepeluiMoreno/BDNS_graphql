import requests
import time
import logging
from typing import Dict, Any, List, Optional, Generator
from datetime import date, datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from etl.config import get_etl_settings

settings = get_etl_settings()
logger = logging.getLogger(__name__)

class BDNSExtractor:
    """Extractor de datos de la API BDNS"""
    
    def __init__(self):
        self.base_url = settings.BDNS_API_URL
        self.delay = settings.BDNS_REQUEST_DELAY
        self.page_size = settings.BDNS_PAGE_SIZE
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Realizar petición a la API con reintentos"""
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Realizando petición a {url} con parámetros {params}")
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # Esperar para evitar saturar la API
        time.sleep(self.delay)
        
        return response.json()
    
    def extract_convocatorias(
        self, 
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Extraer convocatorias de la API BDNS"""
        params = {
            "page_size": self.page_size,
            "page": 1
        }
        
        if fecha_desde:
            params["fecha_desde"] = fecha_desde.isoformat()
        if fecha_hasta:
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        while True:
            data = self._request("convocatorias", params)
            
            if not data.get("results"):
                break
            
            for item in data["results"]:
                yield item
            
            # Si no hay más páginas, terminar
            if not data.get("next"):
                break
            
            params["page"] += 1
    
    def extract_concesiones(
        self, 
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Extraer concesiones de la API BDNS"""
        params = {
            "page_size": self.page_size,
            "page": 1
        }
        
        if fecha_desde:
            params["fecha_desde"] = fecha_desde.isoformat()
        if fecha_hasta:
            params["fecha_hasta"] = fecha_hasta.isoformat()
        
        while True:
            data = self._request("concesiones", params)
            
            if not data.get("results"):
                break
            
            for item in data["results"]:
                yield item
            
            # Si no hay más páginas, terminar
            if not data.get("next"):
                break
            
            params["page"] += 1
    
    def extract_concesion_by_id(self, concesion_id: str) -> Optional[Dict[str, Any]]:
        """Extraer una concesión específica por su ID"""
        try:
            return self._request(f"concesiones/{concesion_id}", {})
        except Exception as e:
            logger.error(f"Error al extraer concesión {concesion_id}: {e}")
            return None
