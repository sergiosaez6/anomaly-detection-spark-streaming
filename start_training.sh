#!/bin/bash

execute.sh es.dmr.uimp.clustering.KMeansClusterInvoices ./clustering ./threshold
execute.sh es.dmr.uimp.clustering.BisectionKMeansClusterInvoices ./clustering_bisect ./threshold_bisect
