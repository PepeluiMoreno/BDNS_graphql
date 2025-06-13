from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from strawberry.fastapi import GraphQLRouter
from app.graphql.schema import schema
from app.api.health import router as health_router
from app.config import get_settings
from app.utils.metrics import setup_metrics
import time

settings = get_settings()

# Crear aplicación FastAPI
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="API GraphQL para consulta de datos de la BDNS",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware para métricas de tiempo de respuesta
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Configurar métricas de Prometheus
setup_metrics(app)

# Crear router GraphQL
graphql_app = GraphQLRouter(
    schema,
    graphiql=settings.DEBUG,  # Habilitar GraphiQL solo en modo debug
)

# Incluir routers
app.include_router(graphql_app, prefix="/graphql")
app.include_router(health_router, prefix="/api")

# Manejador de excepciones global
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"message": "Error interno del servidor"},
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)
