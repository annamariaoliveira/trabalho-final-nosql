import time
import requests
from datetime import datetime
from redis import Redis
from pymongo import MongoClient
#from cassandra.cluster import Cluster
from neo4j import GraphDatabase

try:
    redis_client = Redis(host='localhost', port=6379, decode_responses=True)

    mongo_client = MongoClient('mongodb://localhost:27017/')
    db_mongo = mongo_client['fintech_db']

    # print("[SCYLLA] Conectando ao ScyllaDB...")
    # scylla_cluster = Cluster(['127.0.0.1'])
    # session_scylla = scylla_cluster.connect()
    # session_scylla.execute("CREATE KEYSPACE IF NOT EXISTS mercado WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    # session_scylla.set_keyspace('mercado')
    # session_scylla.execute("""
    #     CREATE TABLE IF NOT EXISTS historico_precos (
    #         moeda text, data_coleta timestamp, preco float, PRIMARY KEY (moeda, data_coleta)
    #     ) WITH CLUSTERING ORDER BY (data_coleta DESC);
    # """)

    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "aluno123"))

except Exception as e:
    print(f"ERRO DE CONEXÃO: {e}")
    exit()

def setup_neo4j():
    with neo4j_driver.session() as session:
        session.run("MERGE (i:Investidor {nome: 'Adail'}) "
                    "MERGE (m:Moeda {simbolo: 'BTCUSDT'}) "
                    "MERGE (i)-[:ACOMPANHA]->(m)")

def monitorar():
    setup_neo4j()
    print("\n--- SISTEMA ONLINE ---")
    
    while True:
        simbolo = "BTCUSDT"
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={simbolo}"

        preco_cache = redis_client.get(simbolo)
        if preco_cache:
            preco = float(preco_cache)
            print(f"[REDIS] Cache Hit: {preco}")
        else:
            print(f"[REDIS] Cache Miss! Consultando API...")
            res = requests.get(url).json()
            preco = float(res['price'])
            redis_client.set(simbolo, preco, ex=10)

        try:
            db_mongo.cotacoes.insert_one({"moeda": simbolo, "valor": preco, "data": datetime.now()})
            print("[MONGO] Documento salvo.")
        except:
            print("[MONGO] Erro ao salvar (Banco offline?).")

        # session_scylla.execute(
        #     "INSERT INTO historico_precos (moeda, data_coleta, preco) VALUES (%s, %s, %s)",
        #     (simbolo, datetime.now(), preco)
        # )
        # print(f"[SCYLLA] Histórico gravado: {preco}")

        with neo4j_driver.session() as session:
            res = session.run("MATCH (i:Investidor)-[:ACOMPANHA]->(m:Moeda {simbolo: $s}) RETURN i.nome as n", s=simbolo)
            for r in res:
                print(f"[NEO4J] Notificando investidor: {r['n']}")

        time.sleep(10)

if __name__ == "__main__":
    monitorar()