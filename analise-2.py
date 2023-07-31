#TCC00336 Banco de Dados Não Convencionais
#Trabalho Final de Disciplina - Marcos Bedo
#
#Autora: Natália Rabelo
#Finalidade: Ler uma tabela fato já preparada em um data frame dos táxis amarelos
#de New York para extrair os dados de faturamento de cada agência de taxi.
#Dependencias: PySpark corretamente configurado 
#
#Uso:
#python3 analise-2.py {texto-analise} {tabela-fato}
#texto-analise: Opcional, deve conter um caminho para um arquivo onde será impresso o material para análise. Valor padrão: ./resposta-2.txt
#tabela-fato: Opcional, deve apontar para a tabela fato. Valor padrão: ./df-ftable/
#
#!!!ATENÇÃO!!!: o caminho dado por {texto-analise} vai ser sobrescrito!!! 
#Se desejar manter o antigo, faça backup.

import io
import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, desc, col, sum, count, when, lit
import math

#---Globais---
month_list = [ "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

#Configuração para imprimir debug
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

logging.debug("Processando os parametros passados por argumento")
result_data = "./resposta-2.txt"
data_frame = "./df-ftable/"
if len(sys.argv) > 1:
    result_data = sys.argv[1]
if len(sys.argv) > 2:
    data_frame = sys.argv[2]
logging.debug(f"Caminho para salvar a resposta: {result_data}")
logging.debug(f"Caminho para salvar a tabela fato: {data_frame}")

logging.debug("Criando sessão do Spark")
spark = SparkSession.builder.appName("yellow-taxi").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logging.debug(f"Lendo o data frame armazenado em: {data_frame}")
fact_table = spark.read.parquet(data_frame)

logging.debug("Agregando uma nova coluna para taxa de 0.02 quando se usa cartao de credito")
fact_table = fact_table.withColumn("total_amount_with_tax", when(col("payment_type") == 1, col("total_amount") + col("fare_amount") * 0.02).otherwise(col("total_amount")))

logging.debug("Calculando a media geral de ganho por milha")
total_amount_sum = fact_table.select(sum("total_amount")).first()["sum(total_amount)"]
total_distance_sum = fact_table.select(sum("trip_distance")).first()["sum(trip_distance)"]
gain_per_mile = total_amount_sum / total_distance_sum
total_amount_with_tax_sum = fact_table.select(sum("total_amount_with_tax")).first()["sum(total_amount_with_tax)"]
gain_per_mile_with_tax = total_amount_with_tax_sum / total_distance_sum

logging.debug("Calculando o ganho medio de cada agencia por corrida")
vendor_data = fact_table.groupBy("VendorID").agg(sum("total_amount").alias("total_earnings"), count("*").alias("trip_count"), sum("total_amount_with_tax").alias("total_earnings_with_tax"))
vendor_data = vendor_data.withColumn("average_earning_per_trip", col("total_earnings") / col("trip_count"))
vendor_data = vendor_data.withColumn("average_earning_per_trip_with_tax", col("total_earnings_with_tax") / col("trip_count"))

logging.debug("Separando os dados por mes de ocorrencia baseado na data de dropoff.\n Agrupando o ganho e a quantidade de milhas por agencia de taxi por mes.")
data_per_month = []
for month_num in range(1, 13):
    month_data = fact_table.filter(month("tpep_dropoff_datetime") == month_num)
    vendor_stat = month_data.groupBy("VendorID").agg(sum(col("trip_distance").cast("double")).alias("total_distance"), sum(col("total_amount").cast("double")).alias("total_amount"), sum(col("total_amount_with_tax").cast("double")).alias("total_amount_with_tax"))
    data_per_month.append(vendor_stat)

logging.debug("Escrevendo a media de ganho por milha a cada mes de cada agencia.")
result_data_file = open(result_data, "w")
result_data_file.write("Ganho medio geral no ano por milha: " + str(round(gain_per_mile, 2)) + "$ Com taxa de CC: " + str(round(gain_per_mile_with_tax, 2)) + "$.\n\n")

logging.debug("Escrevendo a media de ganho de cada agencia")
for row in vendor_data.collect():
    vendor_id = row["VendorID"]
    trip_count = row["trip_count"]
    average_earning_per_trip = row["average_earning_per_trip"]
    average_earning_per_trip_with_tax = row["average_earning_per_trip_with_tax"]
    result_data_file.write(f"Id Agencia {vendor_id} Total de viagens: {trip_count} Ganho medio: " + str(round(average_earning_per_trip, 2)) + "$ Ganho medio com taxa CC: " + str(round(average_earning_per_trip_with_tax, 2)) +  "$\n")
result_data_file.write("\n")

result_data_file.write("Ganho medio por milha de cada agencia separados por mes de ocorrencia.\n")
for month_num in range(12):
    month_data = data_per_month[month_num]
    result_data_file.write(month_list[month_num] + '\n')
    vendor_stats = month_data.withColumn("earnings_per_mile", col("total_amount") / col("total_distance"))
    vendor_stats = vendor_stats.withColumn("earnings_per_mile_with_tax", col("total_amount_with_tax") / col("total_distance"))
    for row in vendor_stats.collect():
        vendor_id = row["VendorID"]
        earnings_per_mile = row["earnings_per_mile"]
        earnings_per_mile_with_tax = row["earnings_per_mile_with_tax"]
        total_distance = row["total_distance"]
        result_data_file.write(f"Id agencia: {vendor_id} Ganho medio por milha: " + str(round(earnings_per_mile, 2)) + "$ Ganho medio por milha com CC: " + str(round(earnings_per_mile_with_tax, 2)) + f"$ Distancia total percorrida: {total_distance}\n")
    result_data_file.write('\n') 

