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
* LOGSTASH_URL (default `http://uc-ls-event-loop.collectors.svc:80`)

rucio-nongrid-traces are not indexed directly; they are POSTed to Logstash
(with `User-Agent: xAODRootAccess`, which the Logstash pipeline requires).

NB: for ATLAS Analytics this collector runs at UofC River k8s cluster in "collectors" namespace.

At some point this should be changed so also rucio-events and rucio-traces go to a logstash collector and not straight to Elasticsearch. Disconnected ES never reconnects by itself.
