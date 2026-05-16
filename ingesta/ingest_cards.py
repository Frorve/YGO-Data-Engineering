import os
import uuid
import json
import requests
from datetime import datetime, timezone
from snowflake.connector import connect

# ===========================================================================
# CONFIGURACIÓN
# ===========================================================================

SNOWFLAKE_CONFIG = {
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user":      os.environ.get("SNOWFLAKE_USER"),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
    "role":      "ACCOUNTADMIN",
    "warehouse": "WH-YGO",
    "database":  "PRE_BRONZE_YGO_DB",
    "schema":    "LANDING",
}

API_URL     = "https://db.ygoprodeck.com/api/v7/cardinfo.php?misc=yes"
ENTORNO     = "pre"
BATCH_SIZE  = 100  # número de cartas por INSERT para no superar límites de Snowflake

# ===========================================================================
# LLAMADA A LA API
# ===========================================================================

def fetch_cards():
    print(f"[INFO] Llamando a la API: {API_URL}")
    response = requests.get(API_URL, timeout=60)
    response.raise_for_status()
    data = response.json()
    cards = data.get("data", [])
    print(f"[INFO] Cartas recibidas: {len(cards)}")
    return cards, response.status_code

# ===========================================================================
# INSERCIÓN EN SNOWFLAKE
# ===========================================================================

def insert_batch(cursor, batch, ingesta_id, ingesta_ts, status_code):
    payload = json.dumps({"data": batch}, ensure_ascii=False)
    cursor.execute(
        """
        INSERT INTO PRE_BRONZE_YGO_DB.LANDING.RAW_API_INGESTAS
            (ingesta_id, entorno, endpoint_url, http_status_code,
             raw_payload, ingesta_ts, es_mock)
        SELECT
            %(ingesta_id)s,
            %(entorno)s,
            %(endpoint_url)s,
            %(http_status_code)s,
            PARSE_JSON(%(raw_payload)s),
            %(ingesta_ts)s::TIMESTAMP_NTZ,
            %(es_mock)s
        """,
        {
            "ingesta_id":       str(ingesta_id),
            "entorno":          ENTORNO,
            "endpoint_url":     API_URL,
            "http_status_code": status_code,
            "raw_payload":      payload,
            "ingesta_ts":       ingesta_ts.isoformat(),
            "es_mock":          False,
        }
    )

def insert_cards(cards, status_code):
    print("[INFO] Conectando a Snowflake...")
    conn = connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    ingesta_ts = datetime.now(timezone.utc)

    try:
        total   = len(cards)
        batches = range(0, total, BATCH_SIZE)
        print(f"[INFO] Insertando {total} cartas en {len(list(batches))} batches de {BATCH_SIZE}...")

        for i, start in enumerate(range(0, total, BATCH_SIZE)):
            batch      = cards[start:start + BATCH_SIZE]
            ingesta_id = uuid.uuid4()
            insert_batch(cursor, batch, ingesta_id, ingesta_ts, status_code)
            print(f"[INFO] Batch {i + 1} insertado: cartas {start + 1} a {min(start + BATCH_SIZE, total)}")

        conn.commit()
        print(f"[OK] Ingesta completada. {total} cartas insertadas en {len(list(range(0, total, BATCH_SIZE)))} batches.")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Fallo durante la inserción: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    try:
        cards, status_code = fetch_cards()
        insert_cards(cards, status_code)
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Fallo en la llamada a la API: {e}")
    except Exception as e:
        print(f"[ERROR] Error inesperado: {e}")
        raise