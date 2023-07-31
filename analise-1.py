#TCC00336 Banco de Dados Não Convencionais
#Trabalho Final de Disciplina - Marcos Bedo
#
#Autoras: Natália Rabelo
#Finalidade: Ler uma tabela fato já preparada em um data frame dos táxis amarelos
#de New York para extrair os dados das cinco regiões mais solicitadas por mês
#Dependencias: PySpark corretamente configurado 
#
#Uso:
#python3 analise-1.py {texto-analise} {tabela-fato}
#texto-analise: Opcional, deve conter um caminho para um arquivo onde será impresso o material para análise. Valor padrão: ./resposta-1.txt
#tabela-fato: Opcional, deve apontar para a tabela fato. Valor padrão: ./df-ftable/
#
#!!!ATENÇÃO!!!: o caminho dado por {texto-analise} vai ser sobrescrito!!! 
#Se desejar manter o antigo, faça backup.

import io
import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, desc

#---Globais---
month_list = [ "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
location_list = [ "None", "EWR Newark Airport EWR", "Queens Jamaica Bay Boro Zone", "Bronx Allerton/Pelham Gardens Boro Zone", "Manhattan Alphabet City Yellow Zone", "Staten Island Arden Heights Boro Zone", "Staten Island Arrochar/Fort Wadsworth Boro Zone", "Queens Astoria Boro Zone", "Queens Astoria Park Boro Zone", "Queens Auburndale Boro Zone", "Queens Baisley Park Boro Zone", "Brooklyn Bath Beach Boro Zone", "Manhattan Battery Park Yellow Zone", "Manhattan Battery Park City Yellow Zone", "Brooklyn Bay Ridge Boro Zone", "Queens Bay Terrace/Fort Totten Boro Zone", "Queens Bayside Boro Zone", "Brooklyn Bedford Boro Zone", "Bronx Bedford Park Boro Zone", "Queens Bellerose Boro Zone", "Bronx Belmont Boro Zone", "Brooklyn Bensonhurst East Boro Zone", "Brooklyn Bensonhurst West Boro Zone", "Staten Island Bloomfield/Emerson Hill Boro Zone", "Manhattan Bloomingdale Yellow Zone", "Brooklyn Boerum Hill Boro Zone", "Brooklyn Borough Park Boro Zone", "Queens Breezy Point/Fort Tilden/Riis Beach Boro Zone", "Queens Briarwood/Jamaica Hills Boro Zone", "Brooklyn Brighton Beach Boro Zone", "Queens Broad Channel Boro Zone", "Bronx Bronx Park Boro Zone", "Bronx Bronxdale Boro Zone", "Brooklyn Brooklyn Heights Boro Zone", "Brooklyn Brooklyn Navy Yard Boro Zone", "Brooklyn Brownsville Boro Zone", "Brooklyn Bushwick North Boro Zone", "Brooklyn Bushwick South Boro Zone", "Queens Cambria Heights Boro Zone", "Brooklyn Canarsie Boro Zone", "Brooklyn Carroll Gardens Boro Zone", "Manhattan Central Harlem Boro Zone", "Manhattan Central Harlem North Boro Zone", "Manhattan Central Park Yellow Zone", "Staten Island Charleston/Tottenville Boro Zone", "Manhattan Chinatown Yellow Zone", "Bronx City Island Boro Zone", "Bronx Claremont/Bathgate Boro Zone", "Manhattan Clinton East Yellow Zone", "Brooklyn Clinton Hill Boro Zone", "Manhattan Clinton West Yellow Zone", "Bronx Co-Op City Boro Zone", "Brooklyn Cobble Hill Boro Zone", "Queens College Point Boro Zone", "Brooklyn Columbia Street Boro Zone", "Brooklyn Coney Island Boro Zone", "Queens Corona Boro Zone", "Queens Corona Boro Zone", "Bronx Country Club Boro Zone", "Bronx Crotona Park Boro Zone", "Bronx Crotona Park East Boro Zone", "Brooklyn Crown Heights North Boro Zone", "Brooklyn Crown Heights South Boro Zone", "Brooklyn Cypress Hills Boro Zone", "Queens Douglaston Boro Zone", "Brooklyn Downtown Brooklyn/MetroTech Boro Zone", "Brooklyn DUMBO/Vinegar Hill Boro Zone", "Brooklyn Dyker Heights Boro Zone", "Manhattan East Chelsea Yellow Zone", "Bronx East Concourse/Concourse Village Boro Zone", "Queens East Elmhurst Boro Zone", "Brooklyn East Flatbush/Farragut Boro Zone", "Brooklyn East Flatbush/Remsen Village Boro Zone", "Queens East Flushing Boro Zone", "Manhattan East Harlem North Boro Zone", "Manhattan East Harlem South Boro Zone", "Brooklyn East New York Boro Zone", "Brooklyn East New York/Pennsylvania Avenue Boro Zone", "Bronx East Tremont Boro Zone", "Manhattan East Village Yellow Zone", "Brooklyn East Williamsburg Boro Zone", "Bronx Eastchester Boro Zone", "Queens Elmhurst Boro Zone", "Queens Elmhurst/Maspeth Boro Zone", "Staten Island Eltingville/Annadale/Prince's Bay Boro Zone", "Brooklyn Erasmus Boro Zone", "Queens Far Rockaway Boro Zone", "Manhattan Financial District North Yellow Zone", "Manhattan Financial District South Yellow Zone", "Brooklyn Flatbush/Ditmas Park Boro Zone", "Manhattan Flatiron Yellow Zone", "Brooklyn Flatlands Boro Zone", "Queens Flushing Boro Zone", "Queens Flushing Meadows-Corona Park Boro Zone", "Bronx Fordham South Boro Zone", "Queens Forest Hills Boro Zone", "Queens Forest Park/Highland Park Boro Zone", "Brooklyn Fort Greene Boro Zone", "Queens Fresh Meadows Boro Zone", "Staten Island Freshkills Park Boro Zone", "Manhattan Garment District Yellow Zone", "Queens Glen Oaks Boro Zone", "Queens Glendale Boro Zone", "Manhattan Governor's Island/Ellis Island/Liberty Island Yellow Zone", "Manhattan Governor's Island/Ellis Island/Liberty Island Yellow Zone", "Manhattan Governor's Island/Ellis Island/Liberty Island Yellow Zone", "Brooklyn Gowanus Boro Zone", "Manhattan Gramercy Yellow Zone", "Brooklyn Gravesend Boro Zone", "Staten Island Great Kills Boro Zone", "Staten Island Great Kills Park Boro Zone", "Brooklyn Green-Wood Cemetery Boro Zone", "Brooklyn Greenpoint Boro Zone", "Manhattan Greenwich Village North Yellow Zone", "Manhattan Greenwich Village South Yellow Zone", "Staten Island Grymes Hill/Clifton Boro Zone", "Manhattan Hamilton Heights Boro Zone", "Queens Hammels/Arverne Boro Zone", "Staten Island Heartland Village/Todt Hill Boro Zone", "Bronx Highbridge Boro Zone", "Manhattan Highbridge Park Boro Zone", "Queens Hillcrest/Pomonok Boro Zone", "Queens Hollis Boro Zone", "Brooklyn Homecrest Boro Zone", "Queens Howard Beach Boro Zone", "Manhattan Hudson Sq Yellow Zone", "Bronx Hunts Point Boro Zone", "Manhattan Inwood Boro Zone", "Manhattan Inwood Hill Park Boro Zone", "Queens Jackson Heights Boro Zone", "Queens Jamaica Boro Zone", "Queens Jamaica Estates Boro Zone", "Queens JFK Airport Airports", "Brooklyn Kensington Boro Zone", "Queens Kew Gardens Boro Zone", "Queens Kew Gardens Hills Boro Zone", "Bronx Kingsbridge Heights Boro Zone", "Manhattan Kips Bay Yellow Zone", "Queens LaGuardia Airport Airports", "Queens Laurelton Boro Zone", "Manhattan Lenox Hill East Yellow Zone", "Manhattan Lenox Hill West Yellow Zone", "Manhattan Lincoln Square East Yellow Zone", "Manhattan Lincoln Square West Yellow Zone", "Manhattan Little Italy/NoLiTa Yellow Zone", "Queens Long Island City/Hunters Point Boro Zone", "Queens Long Island City/Queens Plaza Boro Zone", "Bronx Longwood Boro Zone", "Manhattan Lower East Side Yellow Zone", "Brooklyn Madison Boro Zone", "Brooklyn Manhattan Beach Boro Zone", "Manhattan Manhattan Valley Yellow Zone", "Manhattan Manhattanville Boro Zone", "Manhattan Marble Hill Boro Zone", "Brooklyn Marine Park/Floyd Bennett Field Boro Zone", "Brooklyn Marine Park/Mill Basin Boro Zone", "Staten Island Mariners Harbor Boro Zone", "Queens Maspeth Boro Zone", "Manhattan Meatpacking/West Village West Yellow Zone", "Bronx Melrose South Boro Zone", "Queens Middle Village Boro Zone", "Manhattan Midtown Center Yellow Zone", "Manhattan Midtown East Yellow Zone", "Manhattan Midtown North Yellow Zone", "Manhattan Midtown South Yellow Zone", "Brooklyn Midwood Boro Zone", "Manhattan Morningside Heights Boro Zone", "Bronx Morrisania/Melrose Boro Zone", "Bronx Mott Haven/Port Morris Boro Zone", "Bronx Mount Hope Boro Zone", "Manhattan Murray Hill Yellow Zone", "Queens Murray Hill-Queens Boro Zone", "Staten Island New Dorp/Midland Beach Boro Zone", "Queens North Corona Boro Zone", "Bronx Norwood Boro Zone", "Queens Oakland Gardens Boro Zone", "Staten Island Oakwood Boro Zone", "Brooklyn Ocean Hill Boro Zone", "Brooklyn Ocean Parkway South Boro Zone", "Queens Old Astoria Boro Zone", "Queens Ozone Park Boro Zone", "Brooklyn Park Slope Boro Zone", "Bronx Parkchester Boro Zone", "Bronx Pelham Bay Boro Zone", "Bronx Pelham Bay Park Boro Zone", "Bronx Pelham Parkway Boro Zone", "Manhattan Penn Station/Madison Sq West Yellow Zone", "Staten Island Port Richmond Boro Zone", "Brooklyn Prospect-Lefferts Gardens Boro Zone", "Brooklyn Prospect Heights Boro Zone", "Brooklyn Prospect Park Boro Zone", "Queens Queens Village Boro Zone", "Queens Queensboro Hill Boro Zone", "Queens Queensbridge/Ravenswood Boro Zone", "Manhattan Randalls Island Yellow Zone", "Brooklyn Red Hook Boro Zone", "Queens Rego Park Boro Zone", "Queens Richmond Hill Boro Zone", "Queens Ridgewood Boro Zone", "Bronx Rikers Island Boro Zone", "Bronx Riverdale/North Riverdale/Fieldston Boro Zone", "Queens Rockaway Park Boro Zone", "Manhattan Roosevelt Island Boro Zone", "Queens Rosedale Boro Zone", "Staten Island Rossville/Woodrow Boro Zone", "Queens Saint Albans Boro Zone", "Staten Island Saint George/New Brighton Boro Zone", "Queens Saint Michaels Cemetery/Woodside Boro Zone", "Bronx Schuylerville/Edgewater Park Boro Zone", "Manhattan Seaport Yellow Zone", "Brooklyn Sheepshead Bay Boro Zone", "Manhattan SoHo Yellow Zone", "Bronx Soundview/Bruckner Boro Zone", "Bronx Soundview/Castle Hill Boro Zone", "Staten Island South Beach/Dongan Hills Boro Zone", "Queens South Jamaica Boro Zone", "Queens South Ozone Park Boro Zone", "Brooklyn South Williamsburg Boro Zone", "Queens Springfield Gardens North Boro Zone", "Queens Springfield Gardens South Boro Zone", "Bronx Spuyten Duyvil/Kingsbridge Boro Zone", "Staten Island Stapleton Boro Zone", "Brooklyn Starrett City Boro Zone", "Queens Steinway Boro Zone", "Manhattan Stuy Town/Peter Cooper Village Yellow Zone", "Brooklyn Stuyvesant Heights Boro Zone", "Queens Sunnyside Boro Zone", "Brooklyn Sunset Park East Boro Zone", "Brooklyn Sunset Park West Boro Zone", "Manhattan Sutton Place/Turtle Bay North Yellow Zone", "Manhattan Times Sq/Theatre District Yellow Zone", "Manhattan TriBeCa/Civic Center Yellow Zone", "Manhattan Two Bridges/Seward Park Yellow Zone", "Manhattan UN/Turtle Bay South Yellow Zone", "Manhattan Union Sq Yellow Zone", "Bronx University Heights/Morris Heights Boro Zone", "Manhattan Upper East Side North Yellow Zone", "Manhattan Upper East Side South Yellow Zone", "Manhattan Upper West Side North Yellow Zone", "Manhattan Upper West Side South Yellow Zone", "Bronx Van Cortlandt Park Boro Zone", "Bronx Van Cortlandt Village Boro Zone", "Bronx Van Nest/Morris Park Boro Zone", "Manhattan Washington Heights North Boro Zone", "Manhattan Washington Heights South Boro Zone", "Staten Island West Brighton Boro Zone", "Manhattan West Chelsea/Hudson Yards Yellow Zone", "Bronx West Concourse Boro Zone", "Bronx West Farms/Bronx River Boro Zone", "Manhattan West Village Yellow Zone", "Bronx Westchester Village/Unionport Boro Zone", "Staten Island Westerleigh Boro Zone", "Queens Whitestone Boro Zone", "Queens Willets Point Boro Zone", "Bronx Williamsbridge/Olinville Boro Zone", "Brooklyn Williamsburg (North Side) Boro Zone", "Brooklyn Williamsburg (South Side) Boro Zone", "Brooklyn Windsor Terrace Boro Zone", "Queens Woodhaven Boro Zone", "Bronx Woodlawn/Wakefield Boro Zone", "Queens Woodside Boro Zone", "Manhattan World Trade Center Yellow Zone", "Manhattan Yorkville East Yellow Zone", "Manhattan Yorkville West Yellow Zone", "Unknown NV N/A", "Unknown NA N/A" ]

#Configuração para imprimir debug
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

logging.debug("Processando os parametros passados por argumento")
result_data = "./resposta-1.txt"
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

logging.debug("Separando os dados por mes de ocorrencia baseado na data de dropoff")
data_per_month = []
for month_num in range(1, 13):
    data_per_month.append(fact_table.filter(month("tpep_dropoff_datetime") == month_num))

logging.debug(f"Escrevendo as 5 ocorrencias mais frequentes por mes em {result_data}")
result_data_file = open(result_data, "w")
result_data_file.write("Regioes de destino por mes ranqueadas de acordo com a frequencia.\n")
for month_num in range(12):
    month_data = data_per_month[month_num]
    result_data_file.write(month_list[month_num] + '\n')
    top_drop_locations =  month_data.groupBy("DOLocationID").count().orderBy(desc("count")).limit(5).collect()
    rank = 1
    for row in top_drop_locations:
        result_data_file.write(str(rank) + ". Regiao: " + location_list[row.DOLocationID] + " Frequencia: " + str(row['count']) + '\n')
        rank += 1
    result_data_file.write('\n')

logging.debug("Fechando o arquivo de resposta")
result_data_file.close()

logging.debug("Fechando a sessão spark")
spark.stop()
