# anomaly-detection-spark-streaming

## English
This project is included in the Master in Research of the [UIMP](http://www.uimp.es/), in the subject 'Big Data'.

The project consists of carrying out, mainly, two tasks:
- To train offline a KMeans and a BisectionKMeans algorithm, with the aim of detecting anomalies in a series of incoming invoices.
- Develop a pipeline that:
  + Read a series of purchases as input.
  + Format these purchases as invoices to feed the algorithms.
  + Detects and separates erroneous purchases.
  + Feed 4 Kafka topics with the information of erroneous invoices, cancelled invoices, and anomalies found by each algorithm.
  
The basis of the project, i.e. the skeleton files *Clustering.scala*, *train.scala*, *InvoicePipeline.scala*, *InvoiceDataProducer.scala* as well as some of the execution files *.sh* have been provided by the teachers of the course. All the pipeline logic, data formatting, calculation of results and presentation of results through Kafka is self-made.

To execute this project, the following is needed in this order (example in Ubuntu):
- Compile the project with  (from project folder):
''' 
sbt clean assembly
'''
- Set up the Zookeeper and Kafka servers (from the Kafka installation folder):
''' 
sudo bin/zookeeper-server-start.sh config/zookeeper.properties &
sudo bin/kafka-server-start.sh config/server.properties &
'''
- Create the required topics (purchases, facturas_erroneas, cancelaciones, anomalias_kmeans, anomalias_buscet_kmeans):
'''
sudo bin/kafka-topics.sh --create bin/kafka-topics.sh --topic {topic} --partitions 1 –replication-factor 1 –zookeeper localhost:2181
'''
- Connect to the output topics (facturas_erroneas, cancelaciones, anomalias_kmeans, anomalias_buscet_kmeans):
'''
sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic {topic}
'''
- Execution of the training (from the project folder):
'''
sudo ./start_training.sh
'''
- Execution of the pipeline (from the project folder):
'''
sudo ./start_pipeline.sh
'''
- Execution of the producer (from the project folder):
'''
sudo ./production.sh
'''


## Spanish
Este proyecto se engloba en el Máster en Investigación de la [UIMP](http://www.uimp.es/), en la asignatura 'Big Data'.

El proyecto consiste en realizar, prinicpalmente, dos tareas:
- Entrenar de manera offline un algoritmo KMeans y otro de BisectionKMeans, con el objetivo de detectar anomalías en una serie de facturas de entrada.
- Desarrollar un pipeline que:
  + Lea una serie compras como entrada.
  + Formatee estas compras como facturas para alimentar a los algoritmos.
  + Detecte y separe las compras erróneas.
  + Alimente a 4 topics de Kafka con la informaicón de facturas erróneas, canceladas, y las anomalías encontradas por cada aalgoritmo.
  
La base del proyecto, es decir,el esqueleto de los ficheros *Clustering.scala*, *train.scala*, *InvoicePipeline.scala*, *InvoiceDataProducer.scala* así como algunos de los ficheros de ejecución *.sh* han sido proporcionados por los profesores de la asignatura. Toda la lógica del pipeline, formateo de los datos, cálculo de resultados y presentación de resultados por Kafka es de elaboración propia.

Para ejecutar el proyecto, es necesario realziar los siguiente en este orden (ejemplo en Ubuntu):
- Compilar el proyecto (desde el directorio del proyecto): 
'''
sbt clean assembly
'''
- Arrancar el servidor Zookeeper y el de Kafka (desde el directorio de isntalación de Kafka): 
'''
sudo bin/zookeeper-server-start.sh config/zookeeper.properties &
sudo bin/kafka-server-start.sh config/server.properties &
'''
- Crrear los topics necesarios (purchases, facturas_erroneas, cancelaciones, anomalias_kmeans, anomalias_buscet_kmeans):
'''
sudo bin/kafka-topics.sh --create bin/kafka-topics.sh --topic {topic} --partitions 1 –replication-factor 1 –zookeeper localhost:2181
'''
- Conectar con los topics de salida (facturas_erroneas, cancelaciones, anomalias_kmeans, anomalias_buscet_kmeans):
'''
sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic {topic}
'''
- Ejecución del entrenamiento (desde el directorio del proyecto):
'''
sudo ./start_training.sh
'''
- Ejecutar el pipeline (desde el directorio del proyecto):
'''
sudo ./start_pipeline.sh
'''
- Ejecutar el productor de datos (desde el directorio del proyecto):
'''
sudo ./production.sh
'''
