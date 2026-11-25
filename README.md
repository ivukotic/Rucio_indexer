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

## Sources

| AMQ queue | Destination |
| --- | --- |
| `/queue/Consumer.ftsucanalytics.rucio.tracer` | ES `rucio-traces-YYYY.MM.DD` |
| `/queue/Consumer.ftsucanalytics.rucio.events` | ES `rucio-events-YYYY.MM.DD` |
| `/queue/Consumer.ftsucanalytics.rucio.nongrid_tracer` | Logstash (`rucio-nongrid-traces`) |
| `/queue/Consumer.ftsucanalytics.rucio.inject` | ES `rucio-injector-YYYY.MM` |

### rucio-injector

Events from the Data Challenge injector, which creates replication rules on
src → dst links to reach a target throughput. One JSON document per event,
told apart by the `event` field:

* `rule_created` / `rule_failed` — one per DID the injector tried to replicate
  (`did`, `bytes`, `files`, `rule_id`, `lifetime`, `expires_at`; on failure
  `error_type` and `error`).
* `cycle` — summary of one injection cycle on a link (`target_mbps`,
  `injected_bytes`, `injected_mbps`, `target_fraction`, `rules_created`,
  `rules_failed`, `skipped_*`, `stop_reason`, ...).
* `heartbeat` — periodic injector status (`uptime_s`, `plans_active`,
  `links_active`, `rules_alive`, `bytes_alive`, `files_alive`).

Every document carries `time`, `timestamp_ms`, `run_id`, `host`, `account`,
`dryrun`, `severity`, and (except heartbeat) `plan`, `src`, `dst`.
`@timestamp` is taken from `timestamp_ms`.

The source is low volume and can be silent for days, so it goes to monthly
indices (aliased as `rucio-injector`) instead of daily ones. Documents are
flushed to ES at least every 30 s regardless of batch size, so they show up
promptly. The no-message reconnect watchdog looks at all queues together, so
silence on this queue alone does not trigger reconnects. The index template
is in [ES_template.jsonl](ES_template.jsonl).

## Notes

rucio-nongrid-traces are not indexed directly; they are POSTed to Logstash
(with `User-Agent: xAODRootAccess`, which the Logstash pipeline requires).

NB: for ATLAS Analytics this collector runs at UofC River k8s cluster in "collectors" namespace.

At some point this should be changed so also rucio-events and rucio-traces go to a logstash collector and not straight to Elasticsearch. Disconnected ES never reconnects by itself.
