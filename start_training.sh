#!/bin/bash

FILE=./src/main/resources/training.csv
./execute.sh es.dmr.uimp.clustering.KMeansClusterInvoices ${FILE} ./clustering ./threshold
./execute.sh es.dmr.uimp.clustering.BisectionKMeansClusterInvoices ${FILE} ./clustering_bisect ./threshold_bisect
