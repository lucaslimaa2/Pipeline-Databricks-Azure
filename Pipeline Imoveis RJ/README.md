# Pipeline-Databricks-Azure

![arquitetura](https://github.com/user-attachments/assets/b5cf2db3-ee56-474f-853d-512e677c1918)

Este é o meu primeiro projeto de desenvolvimento de um Pipeline de dados em um ambiente cloud. O foco aqui foi na integração da nuvem da Azure com o Databricks para: 
1) Criar um Data Lake (Azure Data Lake Storage Gen 2);
2) Estruturar as camadas do Data Lake (Raw, Bronze, Silver) e a governança (IAM e ACL);
3) Processar os dados utilizando a linguagem Scala nos notebooks do Databricks;
4) Armazenar os dados de forma eficiente nas devidas camadas do Data Lake utilizando o formato Delta;
5) Desenvolver, gerenciar e implementar em produção o pipeline com o Azure Data Factory.

O dataset utilizado possui milhares de observações com características de imóveis do Rio de Janeiro e pode ser encontrado no site https://www.data.rio/.

O primeiro passo do projeto consistiu em configurar os recursos da Azure: subscription, grupo de recursos, conta de armazenamento e os registros de aplicativos.

![Resource Group](https://github.com/user-attachments/assets/cc54407a-01b7-425b-81b9-be0ce73a18c0)

Após essa configuração inicial, criei o Data Lake e estruturei as suas camadas (Raw, Bronze, Silver). Também é necessário atribuir as permissões ao Data Lake (IAM e ACL) para que o Databricks possa conectar-se ao Data Lake.

![Datalake](https://github.com/user-attachments/assets/50c30955-c9f7-4b89-9297-4412c29d80f3)

Para conectar o Databricks no Data Lake, é necessário realizar um *mount* dos dados com um código em escala já pronto que o Databricks fornece. Após isso, o Data Lake fica acessível nos notebooks do Databricks.

![image](https://github.com/user-attachments/assets/056d7751-41a8-4048-953a-4310be50c111)


Com isso, desenvolvi os dois notebooks para realizar o processamento dos dados utilizando um pouco a linguagem Scala e PySpark. O objetivo dos notebooks não era realizar processamentos complexos, mas somente ter um contato com a linguagem Scala e estruturar as etapas do Pipeline.

Notebook 1: Leitura dos dados na camada Raw e armazenamento no formato Delta na camada Bronze.

![image](https://github.com/user-attachments/assets/5ca52728-82e6-4f80-8b90-bbf8d4285a2c)
![image](https://github.com/user-attachments/assets/a8d135bb-c0e7-4fce-861f-cc4085aab5f4)

Notebook 2: Leitura dos dados na camada Bronze e armazenamento no formato Delta na camada Silver no formato desejado.

![image](https://github.com/user-attachments/assets/1fba9eda-b91e-40e2-ae51-a0ff512f325e)

Com os notebooks criados no databricks, o pipeline foi desenvolvido no Azure Data Factory para orquestração na nuvem, contendo duas etapas para acionar o notebook no Databricks. Foi configurado também um trigger para executar o pipeline a cada hora diariamente.

![Data factory](https://github.com/user-attachments/assets/af3ce28d-6b3b-437e-9665-d5c78f4ac212)
![Data factory 2](https://github.com/user-attachments/assets/a7d71738-2293-4494-b3cd-834721fdbf0b)

Visualização dos job runs no Databricks 

![Databricks Workflows](https://github.com/user-attachments/assets/759f9c3e-0368-4f01-8450-6faf44784fb7)


