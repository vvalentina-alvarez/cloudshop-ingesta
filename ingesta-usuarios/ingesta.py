import os
import csv
import io
import logging
from datetime import datetime, timezone

import psycopg2
import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ingesta-usuarios")

POSTGRES_HOST = os.environ["POSTGRES_HOST"] #IP privada de la MV de bases de datos
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "cloudshop_usuarios") #nombre de la base
POSTGRES_USER = os.environ["POSTGRES_USER"] #usuario de SOLO LECTURA (ingesta_pg)
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

S3_BUCKET = os.environ.get("S3_BUCKET", "cloudshop-data-lake-2026-g05")

#plantilla, para agregar mas tablas -> agregar otra entrada
EXTRACCIONES = {
    "usuarios": (
        "SELECT * FROM usuarios",
        "usuarios/usuarios.csv",
    ),
    "direcciones_envio": (
        "SELECT * FROM direcciones_envio",
        "usuarios/direcciones_envio.csv",
    ),
}


def extraer_tabla(cur, consulta):
    #ejecuta la consulta y devuelve (columnas, filas)
    cur.execute(consulta)
    columnas = [descripcion[0] for descripcion in cur.description]
    filas = cur.fetchall()
    return columnas, filas


def a_csv(columnas, filas):
    #convierte columnas + filas en bytes CSV
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columnas)
    writer.writerows(filas)
    return buffer.getvalue().encode("utf-8")


def main():
    log.info("Conectando a PostgreSQL %s:%s/%s ...", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    #boto3 toma automaticamente las credenciales del IAM Role de la EC2.
    s3 = boto3.client("s3")

    total_ok = 0
    try:
        with conn.cursor() as cur:
            for nombre, (consulta, destino) in EXTRACCIONES.items():
                columnas, filas = extraer_tabla(cur, consulta)
                cuerpo = a_csv(columnas, filas)
                s3.put_object(Bucket=S3_BUCKET, Key=destino, Body=cuerpo)
                total_ok += 1
                log.info(
                    "OK | tabla=%s | filas=%d | destino=s3://%s/%s | %s",
                    nombre,
                    len(filas),
                    S3_BUCKET,
                    destino,
                    datetime.now(timezone.utc).isoformat(),
                )
    finally:
        conn.close()

    log.info("Ingesta finalizada. Tablas cargadas correctamente: %d", total_ok)


if __name__ == "__main__":
    main()
