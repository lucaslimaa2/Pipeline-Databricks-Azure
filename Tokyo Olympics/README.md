# Pipeline de dados com Azure Data Factory e Databricks
![Slide1](https://github.com/user-attachments/assets/41770670-d235-4b88-83fd-33758efdd1c7)

Aproveitando o clima dos jogos olímpicos de Paris 2024 que está acontecendo no momento em que escrevo, desenvolvi esse Pipeline de dados onde utilizo dados dos jogos olimpicos de Tokyo 2020.
Resumo do projeto:
1 - Extrair os dados diretamente do Kaggle e carregar no repositório do Github
2 - Realizar a ingestão dos dados utilizando o Data Factory
3 - Armazenar os dados brutos em um data lake na Azure (Gen 2)
4 - Ler os dados na camada raw, transformar e armazenar na camada bronze
5 - Utilizar o Synapse Analytics para escrever queries em SQL nos dados transformados.

O foco deste projeto é estruturar bem todo o caminho que os dados percorrerão com o intuito de praticar o Data Factory e o Synapse Analytics. Ou seja, o processamento dos dados no Databricks será bem simples, pois irei focar nisso em outros projetos.

## 1) Fonte de dados
Primeiramente, baixei todos os arquivos diretamente do Kaggle e armazenei em uma pasta no repositório deste projeto.
Link dos datasets: https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo

![image](https://github.com/user-attachments/assets/70e745b4-ecd3-4bc2-8590-d9446e78419c)

A extração do dataset no Kaggle e o carregamento no Github foram feitos manualmente para realizar a ingestão dos dados no Data Factory por meio de uma requisição HTTP.

## 2) Ingestão dos dados no Data Factory e armazenamento no Data Factory
Com os dados no Github, montei as etapas de ingestão no Data Factory de cada um dos datasets por meio de requisição HTTP (acessando o link dos arquivos RAW do github). Cada etapa desta ingere um dataset diferente e armazena na camada RAW do datalake.

![Data Factory](https://github.com/user-attachments/assets/08c22789-1825-457f-aed5-3c735352f350)

## 3) Processamento
Com todos os 5 datasets carregados no datalake, torna-se possível a leitura deles no Databricks com o spark. Como dito anteriormente, a etapa de "processamento" não era o foco do projeto e consistiu apenas na leitura, alguns códigos básicos em spark e no salvamento dos arquivos na camada bronze do Data Lake. O arquivo está neste repositório (https://github.com/lucaslimaa2/Pipelines-Azure-DataFactory/blob/main/Tokyo%20Olympics/1-Processamento%20de%20dados.py)

![databricks](https://github.com/user-attachments/assets/c20bc385-62ee-4456-85c1-6e0978df6791)

## 4) Analytics
Por fim, a última etapa é utilizar o Synapse Analytics para ler os arquivos do data lake e para escrever algumas queries em SQL e obter alguns insights dos dados. Todo o passo a passo mostrado anteriormente poderia ter sido feito somente no Synapse, que é uma baita ferramenta da Azure.

![synapse analytics](https://github.com/user-attachments/assets/73833485-eef0-4786-bef1-7d1fe48d214a)

É possível também conectar o Power BI diretamente no Synapse Analytics para ter acesso as tabelas que ali estão armazenadas e construir diversas visualizações. 

