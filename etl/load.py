import psycopg2
from psycopg2.extras import execute_batch

def load_table(conn, table, columns, df):
    placeholders = ",".join(["%s"] * len(columns))
    cols = ",".join(columns)

    sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING;
    """

    records = df[columns].itertuples(index=False, name=None)

    with conn.cursor() as cur:
        execute_batch(cur, sql, records)

    conn.commit()

