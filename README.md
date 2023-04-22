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
