import configparser
import psycopg2
from sql_queries import demo_queries


def run_demo(cur, conn):
    for query in demo_queries:
        print('Executing', query)
        cur.execute(query)
        conn.commit()
        print('Query results:\n', cur.fetchall())


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    run_demo(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()