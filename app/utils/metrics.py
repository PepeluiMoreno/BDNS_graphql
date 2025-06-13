from prometheus_client import Counter, Histogram, Info
from prometheus_client.metrics import MetricWrapperBase
from fastapi import FastAPI
import time

# Métricas
REQUEST_COUNT = Counter(
    "bdns_graphql_request_count", 
    "Contador de peticiones HTTP",
    ["method", "endpoint", "status_code"]
)

REQUEST_LATENCY = Histogram(
    "bdns_graphql_request_latency_seconds", 
    "Latencia de peticiones HTTP",
    ["method", "endpoint"]
)

GRAPHQL_QUERY_COUNT = Counter(
    "bdns_graphql_query_count", 
    "Contador de consultas GraphQL",
    ["operation_name"]
)

GRAPHQL_QUERY_LATENCY = Histogram(
    "bdns_graphql_query_latency_seconds", 
    "Latencia de consultas GraphQL",
    ["operation_name"]
)

API_INFO = Info(
    "bdns_graphql_info", 
    "Información de la API GraphQL BDNS"
)

def setup_metrics(app: FastAPI):
    """Configurar métricas de Prometheus para FastAPI"""
    # Establecer información de la API
    from app.config import get_settings
    settings = get_settings()
    API_INFO.info({
        "version": settings.APP_VERSION,
        "name": settings.APP_NAME
    })
    
    # Middleware para métricas de peticiones HTTP
    @app.middleware("http")
    async def metrics_middleware(request, call_next):
        start_time = time.time()
        response = await call_next(request)
        
        # Registrar métricas
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code
        ).inc()
        
        REQUEST_LATENCY.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(time.time() - start_time)
        
        return response
    
    return app
