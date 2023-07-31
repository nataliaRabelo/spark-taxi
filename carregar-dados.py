#TCC00336 Banco de Dados Não Convencionais
#Trabalho Final de Disciplina - Marcos Bedo
#
#Autora: Natália Rabelo
#Finalidade: Carregar os dados dos taxis amarelos de New York em uma tabela fato
#para análise posterior através do Apache Spark
#Dependencias: PySpark corretamente configurado 
#
#Uso:
#python3 carregar-dados.py {caminho-dados} {tabela-fato}
#caminho-dados: Opcional, deve conter o caminho para os dados dos taxis amarelos em parquet. Valor padrão: ./data/
#tabela-fato: Opcional, deve apontar para um arquivo onde a tabela fato vai ser salva. Valor padrão: ./df-ftable/
#
#!!!ATENÇÃO!!!: o caminho dado por {tabela-fato} vai ser sobrescrito!!! 
#Se desejar manter o antigo, faça backup.

import logging
import sys
import os
from pyspark.sql import SparkSession

#Configuração para imprimir debug
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

logging.debug("Processando os parametros passados por argumento")
data_path = "./data/"
data_frame = "./df-ftable/"
if len(sys.argv) > 1:
    data_path = sys.argv[1]
if len(sys.argv) > 2:
    data_frame = sys.argv[2]
logging.debug(f"Caminho para os arquivos dos taxis amarelos: {data_path}")
logging.debug(f"Caminho para salvar a tabela fato: {data_frame}")

logging.debug(f"Lendo os arquivos no caminho: {data_path}")
yellow_taxi_files = []
if not os.path.exists(data_path):
    logging.debug("Script finalizado. Caminho providenciado nao existe")
    exit(0)
for file in os.listdir(data_path):
    logging.debug(file)
    if not file.endswith(".parquet"):
        logging.debug("Script finalizado. Arquivo no caminho dado nao esta em formato parquet")
        exit(0)
    yellow_taxi_files.append(data_path + '/' + file)

#Configuração para o Spark não imprimir um monte de lixo
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

logging.debug("Criando sessão do Spark")
spark = SparkSession.builder.appName("yellow-taxi").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logging.debug("Criando o data frame da tabela fato com os arquivos dos taxis")
fact_table = spark.read.parquet(*yellow_taxi_files)

logging.debug(f"Salvando o data frame da tabela fato em: {data_frame}")
if os.path.exists(data_frame):
    os.remove(data_frame)
fact_table.write.parquet(data_frame)

logging.debug("Fechando a sessão spark")
spark.stop()
exit(0)
