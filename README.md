# RUCIO_indexer

Collects data from Rucio AMQ and sends to Elasticsearch

requires environment variables:

Mandatory:

* MQ_HOST = 'atlas-mb.cern.ch'
* MQ_USER = 'XXXXX'
* MQ_PASS = 'XXXXX'

Optional:

* ES_USER
* ES_PASS
* ES_HOST

NB: for ATLAS Analytics this collector runs at UofC River k8s cluster in "collectors" namespace.
