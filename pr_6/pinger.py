import os
import time
import psycopg2
import hvac


VAULT_ADDR = os.getenv("VAULT_ADDR", "http://vault:8200")
VAULT_ROLE_ID = os.getenv("VAULT_ROLE_ID")
VAULT_SECRET_ID = os.getenv("VAULT_SECRET_ID")
VAULT_SECRET_PATH = os.getenv("VAULT_SECRET_PATH", "secret/data/db-creds")

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "sigmabd")


def get_db_creds_from_vault():
    client = hvac.Client(url=VAULT_ADDR)

    auth_response = client.auth.approle.login(
        role_id=VAULT_ROLE_ID,
        secret_id=VAULT_SECRET_ID
    )

    client.token = auth_response["auth"]["client_token"]

    secret_path = VAULT_SECRET_PATH.replace("secret/data/", "")
    secret = client.secrets.kv.v2.read_secret_version(
        mount_point="secret",
        path=secret_path
    )

    data = secret["data"]["data"]
    return data["username"], data["password"]


def ping_db():
    username, password = get_db_creds_from_vault()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=username,
        password=password
    )

    cur = conn.cursor()
    cur.execute("SELECT 1;")
    result = cur.fetchone()

    cur.close()
    conn.close()

    return result[0]


if __name__ == "__main__":
    while True:
        try:
            result = ping_db()
            print(f"DB ping success: {result}")
        except Exception as e:
            print(f"DB ping failed: {e}")
        time.sleep(10)
