from fastapi import APIRouter, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

router = APIRouter()

@router.get("/health")
async def health_check():
    """Endpoint para verificar la salud de la aplicación"""
    return {"status": "ok"}

@router.get("/metrics")
async def metrics():
    """Endpoint para exponer métricas de Prometheus"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
