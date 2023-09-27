# Relatório Sprint 6: AWS

## Fundamento de análise de dados
Solução de Big Data é diferente de Data Analytics. D&A consiste na solução de coleta, armazenamento, processamento e por fim a análise/visualização dos dados. É o processo analítico específico sendo aplicado. E Big Data seria o desafio de trabalhar com grandes quantidades de dados(5Vs).
As empresas usam essas soluções de dados para ganhar vantagem no mercado. O objetivo principal é obter valor através desses dados. 

Uma solução de avaliação dos dados contém os seguintes passos:
Coleta > Armazenamento > Análise > Visualização

* **Coleta:** Quando absorvemos dados de uma grande variedade de fontes.

* **Armazenamento:** Armazenamos dados no momento da coleta, armazenamos enquanto estão sendo processados e armazenamos também no momento da visualização. 10% dos dados gerados ou coletados pelas empresas são estruturados, 10% semiestruturados, 80% não estruturados.

* **Análise:** Processamento de dados é coletar e manipular os dados para produzir informações significativas. Esses processamentos podem ser em lote/batch(processamento em intervalos), ou streaming(tempo real/processamento continuo).

* **Visualização:** Extrair dos dados informações importantes.


### Os 5Vs do BIG DATA e soluções na nuvem
1. Volume: Quando as empresas têm mais dados do que conseguem processar e analisar, elas têm um problema de volume. *Amazon Simple Storage Service (Amazon S3).*

2. Velocidade: Quando as empresas precisam de informações rápidas dos dados que estão coletando, mas os sistemas implantados simplesmente não conseguem atender às necessidades, há um problema de velocidade. *Amazon EMR, Amazon Kinesis.*

3. Variedade: Quando a empresa fica sobrecarregada pelo grande número de origens dos dados para analisar e você não consegue encontrar sistemas para fazer a análise, sabe que tem um problema de variedade. *Amazon RDS, Amazon Redshift, Amazon DynamoDB*

4. Veracidade: Quando se tem dados que não são controlados, provenientes de vários sistemas diferentes e não consegue fazer curadoria dos dados de maneiras significativas, você sabe que tem um problema de veracidade. *Amazon EMR, AWS Glue.*

5. Valor: Quando há grandes volumes de dados usados para corroborar algumas informações valiosas, você pode estar perdendo o valor dos seus dados. *Amazon QuickSight*

## D&A com a AWS
Coleta: 
- Amazon Kinesis Data Streams = tempo real.
- Amazon Kinesis Data Firehose = integrado com Redshift e S3 e anáslise para linguagem de consulta SQL em tempo real.
- Aws Snowball = Grandes quantidades de dados estáticos, carregar dados e enviar de volta.
- Aws Direct Connect = terabytes ou menos, migração para a nuvem.

Armazenamento:
- S3 = landing zone padrão, escalável e de baixo custo para dados(padrão).
- S3 Glacier = armazenamento de dados frios/ regulamentação de conformidade/ acesso infrequente.
- DynamoDB = Tabela de BD, armazena e recupera qualquer quantidade de dados.
- RDS = Facilita a operação e escala de BD relacional na nuvem.
- Amazon ES = Implantar, operar e dimensionar clusters de pesquisa elásticos.
- CloudSearch = Recurso de pesquisa.
- Aurora = BD relacional.

Processamento/Análise de dados:
- Amazom EMR: clusters dinâmicos totalmente gerenciados para executar Hadoop, Spark, Presto ou HBase.
- Amazon Athena: consulta interativa em dados do S3 usando SQL sem infra para gerenciar.
- Redshift: serviço gerenciado de data Warehouse em escala de petabytes.
- Kinesis D&A: Usando para consultas também em dados em streaming.
- Amazon SageMaker: Análise de dados.

Visualização:
- Amazon QuickSigth: visualizar dados.
- Kibana: codigo aberto para visualizacao com Amazon Elasticsearch Service.

Automatização:
- AWS Database Migration Service: migração de bd e replicação de processo de transação online (OLTP) e data warehouse para o Amazon Redshift
- AWS GLUE: serve para agendamento e orquestração de dados

## Amazon Kinesis Streams
O Amazon Kinesis facilita a coleta, o processamento e a análise de dados de streaming em tempo real, em qualquer escala de forma econômica. Pode consumir dados em tempo real como vídeo, áudio, logs de aplicativos, clickstreams de sites e dados de telemetria de IoT para machine learning, análises. Você pode criar apps que ingerem dados em streaming, gerar insights a partir dele e reagir em tempo real. 

## Amazon Kinesis Analytics (Amazon Managed Service for Apache Flink)
O Amazon Kinesis Data Analytics é a maneira mais fácil de processar streams de dados em tempo real com SQL ou Apache Flink sem a necessidade de aprender novas linguagens de programação ou estruturas de trabalho de processamento.

## Amazon EMR
É um serviço que permite facilmente executar e dimensionar clusters do Hadoop no ambiente AWS.
O Apache Hadoop é um sistema escalável de armazenamento e processamento de dados em batch. Ele usa hardware de servidor de commodity e fornece tolerância a falhas por meio de software.

## Amazon Athena 
O Amazon Athena é um serviço de análise interativo e sem servidor criado em frameworks de código aberto, com suporte a formatos de tabela e arquivo abertos. Basta apontar os dados para o S3, definir o esquema e começar a consultar usando o editor de consultas incorporado. funciona com vários formatos padrões.

## Amazon QuickSight
Ferramenta de BI intuitiva, poderosa e de baixo custo, permite analisar grandes quantidades de dados de várias fontes, entendê-los rapidamente e apresentá-los aos usuários finais em painéis dinâmicos fáceis de usar.

## AWS IoT Analytics
O AWS IoT Analytics é um serviço totalmente gerenciado que operacionaliza análises e é escalonado automaticamente para oferecer suporte a petabytes de dados de IoT. O AWS IoT Analytics permite analisar dados de milhões de dispositivos e criar aplicativos de IoT rápidos e responsivos sem necessidade de gerenciar hardware ou infraestrutura.

## Amazon Redshift
É um data warehouse em colunas, em nuvem e totalmente gerenciado. Você pode usá-lo para executar consultas analíticas complexas de grandes conjuntos de dados por meio da tecnologia de processamento massivamente paralelo (MPP). Conjuntos de dados podem variar de gigabytes a petabytes.

## Por que Analytics para games?
As análises permitem que você tome decisões rápidas e bem-informadas para melhorar a jogabilidade, promover o engajamento e aumentar a receita. Quanto mais rápido você analisar e entender o que deve ser melhorado, mais rápido poderá fazer as alterações. E ainda pode usar a análise para melhorar o design do jogo, assim, aumentando o engajamento e retenção dos jogadores.



