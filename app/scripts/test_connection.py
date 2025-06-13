from sqlalchemy import create_engine, text

def test_connection():
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/bdnsdb")

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Resultado de la consulta:", result.scalar())
    except Exception as e:
        print("Error al conectar o ejecutar consulta:", e)

if __name__ == "__main__":
    test_connection()

