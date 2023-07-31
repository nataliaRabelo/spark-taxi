# Projeto Spark NY <!-- omit in toc -->

O projeto Spark consiste em análise de dados de corridas de taxi de Nova York no período de janeeiro a dezembro de 2022.

* Data: 07/07/2023
* Versão atual: 1.0 

## 1. Pré-Requisitos

* Linux
  * Python 3
  * Apache Spark 3.1.1
  * Hadoop 3.3.5

## 2. Estrutura deste Repositório

Este repositório está estruturado da seguinte forma:

* carregar-dados.py
* analise-1.py
* analise-2.py

## 3 Carregamento de dados 

O arquivo `carregar-dados.py` é responsável por carregar os dados dos arquivos .parquet das corridas de taxi de Nova York em um dataframe salvo na memória secundária . 

### 3.1 Instruções de compilação e execução

Execute a seguinte linha de comando com parâmetros opcionais:
* {caminho-dados}: Opcional, deve conter o caminho para os dados dos taxis amarelos em parquet. 
    * Valor padrão: ./data/
* {tabela-fato}: Opcional, deve apontar para um arquivo onde a tabela fato vai ser salva. 
    * Valor padrão: ./df-ftable/

```
python3 carregar-dados.py {caminho-dados} {tabela-fato}
```

## 4 Análise de dados

Os arquivos `analise-1.py`, `analise-2.py` são responsável por analisar os dados dos taxis de Nova York respondendo os enunciados da pergunta 1 e 2. 

Execute a seguinte linha de comando com parâmetros opcionais:

* {texto-analise} Opcional, deve conter um caminho para um arquivo onde será impresso o material para análise. 
    * Valor padrão: ./resposta-1.txt
* {tabela-fato} Opcional, deve apontar para a tabela fato. 
    * Valor padrão: ./df-ftable/

* {caminho-dados}: Opcional, deve conter o caminho para os dados dos taxis amarelos em parquet.
    * Valor padrão: ./data/
* {tabela-fato}: Opcional, deve apontar para um arquivo onde a tabela fato vai ser salva.
    * Valor padrão: ./df-ftable/

```
python3 analise-1.py {texto-analise} {tabela-fato}
python3 analise-2.py {texto-analise} {tabela-fato}
```

### 4.1. Análise 1

* Os top - 5 destinos (regiões) por taxistas por mês durante o ano de 2022; 
    * Na estrutura fornecida não possui identificação dos taxistas, somente a agência. Desta forma ficou inviável listar os dados por taxistas. Portanto, as regiões foram listadas por mês durante o ano de 2022.
* Acumular todos os top-5 destinos e mostrar a distribuição (número de taxistas) que frequentaram os destinos em cada mês do ano de 2022;
    * Os dados foram acumulados e agrupados por frequência de corridas solicitadas para cada localização, devido não possuir identificação dos taxistas.
* Apresentar a quantidade de corridas para os top-5 destinos e suas adjacências imediatas (regiões que fazem fronteira) por mês para o ano de 2022. 
    * Não feita


Realiza-se uma análise dos dados de táxis amarelos da cidade Nova York para extrair as cinco regiões mais solicitadas por mês. O objetivo é ler uma tabela fato pré-preparada, armazenada em um data frame na memória secundária, e escrever os resultados em um arquivo de resposta.

Inicia-se definindo algumas variáveis globais para mapear os meses e as regiões de destino. Em seguida, processa os parâmetros passados como argumentos no script. Se forem fornecidos argumentos suficientes, ele atualiza os caminhos para o arquivo de resposta e para o data frame da tabela fato. 

Logo após, o código lê o data frame que contém a tabela fato a partir do caminho especificado. Prosseguindo, o código separa os dados por mês de ocorrência, com base na coluna de data de dropoff dos registros. Cria-se uma lista chamada data_per_month, onde cada elemento da lista contém um subconjunto dos dados correspondentes a um mês específico (dado retirado do mês que mostra a data do taxímetro que o cliente foi deixado).

Desta forma, o código então escreve as cinco ocorrências mais frequentes por mês em um arquivo de resposta especificado pelo caminho result_data. Itera-se sobre os dados de cada mês, ordena-os por contagem decrescente e limita o resultado às cinco regiões mais frequentes de dropoff. Em seguida, escreve essas informações no arquivo de resposta, incluindo o ranking da região, o nome da região, e a contagem de ocorrências. Após concluir a escrita no arquivo de resposta, o código fecha o arquivo.

Finalizando, ele registra mensagens de log informando que está fechando a sessão do Spark e encerra a sessão do Spark.

Os resultados são apresentados em um arquivo de forma ordenada por contagem decrescente dos meses, limitando o resultado às cinco regiões mais frequentes. 


### 4.1. Análise 2

* O ganho médio por milha por cada taxista durante todo o ano de 2022;
    * Como não possui identificação dos taxistas, os dados foram gerados para o ganho médio por milha por mês para cada agência.
* A taxa de corridas canceladas por mês para os taxistas que possuem o ganho médio por milha superior à média geral para o ano de 2022;
    * Na estrutura fornecida não possui as taxas de corridas canceladas por mês. Portanto, foi realizado o ganho médio por milha de todas as agências e a média geral do ano inteiro em relação a cada corrida.
* O ganho médio para o top-10 taxistas que mais rodaram por mês durante o ano de 2022;
    * Na estrutura fornecida não possui identificação dos taxistas. Portanto, foi realizado o ganho médio de todas as agências separadas por mês.
* Suponha que incida uma taxa de 2% sobre o cartão de crédito (realizar para as três análises acima).
    * A taxa foi acrescida sobre o valor total da corrida

Realiza-se uma análise dos dados de táxis amarelos de Nova York para extrair informações sobre o faturamento das agências de táxi. O objetivo é ler uma tabela fato pré-preparada, armazenada em um data frame, e escrever os resultados em um arquivo de resposta.

O código define algumas variáveis globais para mapear os meses e configura o registro de log para exibir mensagens de depuração.

Em seguida, o código processa os parâmetros passados como argumentos no script. Se forem fornecidos argumentos suficientes, atualiza-se os caminhos para o arquivo de resposta e para o data frame da tabela fato. 

Então cria-se uma sessão do Spark lê-se o data frame que contém a tabela fato a partir do caminho especificado. A seguir, o código realiza algumas operações de transformação nos dados. Adiciona-se uma nova coluna ao data frame chamada "total_amount_with_tax", que calcula o valor total com uma taxa de 0,02 quando o pagamento é feito com cartão de crédito.

Em seguida, ele calcula a média geral de ganho por milha e a média de ganho por corrida para cada agência de táxi. Também agrupa os dados por mês de ocorrência, calculando o ganho e a quantidade de milhas percorridas por agência de táxi em cada mês.

Desta forma, escreve-se no arquivo de resposta a média geral de ganho por milha para o ano, incluindo a taxa de cartão de crédito. Em seguida, escreve a média de ganho de cada agência por corrida.

Após isso, escreve-se no arquivo de resposta a média de ganho por milha de cada agência, separada por mês de ocorrência. Ele calcula essa média para os dados de cada agência em cada mês, incluindo a taxa de cartão de crédito. Também inclui a distância total percorrida por cada agência em cada mês. 

Finalizando, o código fecha o arquivo de resposta e registra mensagens de log informando que está fechando a sessão do Spark. Em seguida, encerra a sessão do Spark.

