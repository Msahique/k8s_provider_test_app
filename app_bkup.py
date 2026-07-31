from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
import os
import pymysql

app = FastAPI(
    title="Simple FastAPI Database API",
    version="1.0"
)


# ----------------------------------------------------
# Database Configuration
# ----------------------------------------------------
DB_HOST = os.getenv("IM_DB_HOST", "mysql")
DB_PORT = int(os.getenv("IM_DB_PORT", "3306"))
DB_USER = os.getenv("IM_DB_USER", "root")
DB_PASS = os.getenv("IM_DB_PASS", "root")
DB_NAME = os.getenv("IM_DB_NAME", "")


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME if DB_NAME else None,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


# ----------------------------------------------------
# Models
# ----------------------------------------------------
class QueryRequest(BaseModel):
    qry: str


# ----------------------------------------------------
# Health
# ----------------------------------------------------
@app.get("/health")
def health():
    try:
        conn = get_connection()
        conn.close()

        db_status = "connected"

    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "healthy",
        "database": db_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ----------------------------------------------------
# Hello
# ----------------------------------------------------
@app.get("/hello")
def hello():
    return {
        "application": "Simple FastAPI Database API",
        "message": "Hello from FastAPI!",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ----------------------------------------------------
# Execute Query
# ----------------------------------------------------
@app.post("/qry")
def execute_query(req: QueryRequest):

    try:
        conn = get_connection()

        with conn.cursor() as cursor:

            cursor.execute(req.qry)

            if req.qry.strip().lower().startswith("select"):

                rows = cursor.fetchall()

                return {
                    "success": True,
                    "rows": len(rows),
                    "data": rows
                }

            else:

                affected = cursor.rowcount

                return {
                    "success": True,
                    "affected_rows": affected,
                    "message": "Query executed successfully."
                }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            conn.close()
        except:
            pass