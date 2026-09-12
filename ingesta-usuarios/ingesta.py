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

PG_HOST = os.environ["PG_HOST"] #IP privada de la MV de bases de datos
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ["PG_DB"] #nombre de la base
PG_USER = os.environ["PG_USER"] #usuario de SOLO LECTURA
PG_PASS = os.environ["PG_PASS"]

S3_BUCKET = os.environ.get("S3_BUCKET", "cloudshop-data-lake-2026-g05")

#plantilla, para agregar mas tablas -> agregar otra entrada.
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
    log.info("Conectando a PostgreSQL %s:%s/%s ...", PG_HOST, PG_PORT, PG_DB)
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
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
