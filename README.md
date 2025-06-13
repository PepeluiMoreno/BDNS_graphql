# README.md - API GraphQL BDNS

## Descripción

Este proyecto implementa una API GraphQL para la Base de Datos Nacional de Subvenciones (BDNS) de España, siguiendo un enfoque híbrido que combina una base de datos local con consultas a la API original cuando es necesario. Está diseñado específicamente para analizar patrones de concesión de subvenciones, detectar favoritismo y concentración por tipo de entidad.

## Características principales

- **API GraphQL**: Interfaz flexible para consultas complejas sobre subvenciones
- **Enfoque híbrido**: Almacenamiento local + consultas a la API original
- **Optimizado para Oracle Cloud Free Tier**: Diseñado para operar dentro de las limitaciones gratuitas
- **Análisis de patrones**: Consultas específicas para detectar concentración de subvenciones
- **Clasificación por NIF**: Identificación automática del tipo de entidad según el NIF
- **Actualizaciones diarias**: Sincronización incremental con la BDNS

## Requisitos

- Python 3.9+
- Oracle Database (ATP en Oracle Cloud)
- Redis (para caché)
- Acceso a la API de la BDNS

## Instalación

1. Clonar el repositorio:
```bash
git clone https://github.com/PepeluiMoreno/BDNS_graphql.git
cd BDNS_graphql
```

2. Crear entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

4. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus credenciales
```

5. Configurar la base de datos:
```bash
python scripts/setup_db.py
```

## Uso

### Iniciar la API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Ejecutar ETL inicial

```bash
python -m etl.tasks.initial_load
```

### Programar actualizaciones diarias

```bash
python -m etl.scheduler
```

## Estructura del proyecto

- `app/`: Aplicación principal (FastAPI + GraphQL)
- `etl/`: Procesos de extracción, transformación y carga
- `scripts/`: Scripts de utilidad y configuración
- `tests/`: Pruebas automatizadas

## Consultas GraphQL de ejemplo

### Buscar concesiones por tipo de entidad
```graphql
query {
  estadisticas_por_tipo_entidad(filtros: {
    año: 2023,
    tipo_entidad: "entidad_religiosa"
  }) {
    tipo_entidad
    año
    numero_concesiones
    importe_total
  }
}
```

### Analizar concentración de subvenciones
```graphql
query {
  concentracion_subvenciones(
    año: 2023,
    tipo_entidad: "sociedad_limitada",
    limite: 10
  ) {
    beneficiario_nombre
    numero_concesiones
    importe_total
  }
}
```

## Licencia

Este proyecto está licenciado bajo [MIT License](LICENSE).

## Contacto

Para más información, contactar a [tu_email@example.com](mailto:tu_email@example.com).
