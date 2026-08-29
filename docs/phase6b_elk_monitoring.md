# Phase 6B (bonus) — ELK pipeline monitoring

## Architecture decision

The obvious reading of "insert → log → MongoDB → Elastic/Logstash → Kibana"
is a single chain: Mongo feeds Logstash feeds Elasticsearch. We didn't build
it that way. Instead the consumer (`nw_de_consumer`) writes each processed
CDC event to **two independent sinks** directly from the same in-process
event, and Logstash reads the **same underlying Kafka topics** the consumer
already reads (`nw.cdc.orders`, `nw.cdc.order_details`), not Mongo:

```
                 ┌─→ MongoDB (monitoring.cdc_events)      durable event log
Kafka (CDC) ─→ consumer ─┤
                 └─→ Elasticsearch (nw-consumer-events-*)  processing outcome
                          (status, error, latency_ms)

Kafka (CDC) ─────────────→ Logstash ─→ Elasticsearch (nw-cdc-events-*) ─→ Kibana
                          (native Kafka input, index template)   search/viz
```

Reasons:
- **Mongo is a log, not a queue.** Chaining Logstash off Mongo would mean
  Logstash has to poll a collection for new documents (its `mongodb` input
  plugin is a third-party plugin, not core, and lags behind writes). Reading
  Kafka natively is what Logstash's Kafka input is built for: consumer
  groups, offsets, at-least-once delivery, no polling.
- **Independent failure domains.** If Elasticsearch or Logstash falls over,
  MongoDB still gets every event — nothing is lost from the durable log. If
  Mongo falls over, Logstash's Kafka→ES path is completely unaffected
  because it never goes through Mongo. Neither monitoring path can affect
  the actual ClickHouse pipeline either: every monitoring write in
  `consumer.py` is wrapped in its own try/except and logged as non-fatal.
- **Two ES indices, two purposes.** `nw-cdc-events-*` (via Logstash) is a
  faithful mirror of the raw Kafka change events — what happened upstream.
  `nw-consumer-events-*` (written directly by the consumer) additionally
  carries the *processing outcome* (`status`, `error`, `latency_ms`) that
  only the consumer knows, since Logstash never sees whether the consumer's
  apply-to-ClickHouse step actually succeeded.

If the professor specifically wants the literal Mongo→Logstash chain, the
swap is small: point `cdc.conf`'s input block at Logstash's `mongodb` input
plugin instead of `kafka`, reading `monitoring.cdc_events`. We didn't do
this because it trades a native, well-supported input for a polling one
with no corresponding gain — the events are the same either way.

## Services added

All on `nw_de_net`, brought up one at a time (ES → Kibana → Logstash →
Mongo → beats) per the resource note. Elasticsearch runs single-node,
security disabled, heap capped at `-Xms512m -Xmx512m` (local dev only).

| Service | Host port | Purpose |
|---|---|---|
| elasticsearch | 9200 | Stores `nw-cdc-events-*`, `nw-consumer-events-*`, `filebeat-*`, `packetbeat-*` |
| kibana | 5601 | Visualization + the Pipeline Monitoring dashboard |
| mongodb | 27018 → 27017 | Durable CDC event log, `monitoring.cdc_events` (27017 was already bound by an unrelated host process; container-internal port and in-network `mongodb:27017` refs are unaffected) |
| logstash | 5044, 9600 | Kafka → ES pipeline (`cdc.conf`) |
| filebeat | — | Ships producer/consumer/airflow container logs to `filebeat-*` |
| packetbeat | — (`network_mode: host`) | Flow-level capture on ports 9092 (Kafka) and 1433 (SQL Server) → `packetbeat-*` |

Packetbeat is flow-level only: 8.15's default build has no Kafka
application-layer decoder for this config path (logs "Unknown protocol
plugin: kafka", non-fatal, simply skipped) and none at all for SQL Server's
proprietary TDS protocol. Source/dest ports, byte counts, and duration are
still captured and confirmed present in `packetbeat-*` for both ports.

## Index templates

`nw-cdc-events-template` and `nw-consumer-events-template`
(`infra/elasticsearch/templates/`) are applied once via
`PUT _index_template/...` before first write, so field types (dates,
keywords, integers) are correct from the first document rather than
inferred by ES's dynamic mapping.

## Kibana provisioning

`infra/kibana/pipeline_monitoring.ndjson` is a saved-objects export
(2 data views, 4 visualizations, 1 saved search, 1 dashboard) importable via
`POST /api/saved_objects/_import?overwrite=true`, so the "Pipeline
Monitoring" dashboard is reproducible from a clean stack rather than
requiring manual UI clicking. Panels: events/min by op (I/U/D), events by
table, avg latency over time, error count (`status:error`), recent events
table.

## A pitfall worth recording

Python's default `str(datetime)` (and `json.dumps(..., default=str)`)
renders a **space** between date and time (`"2026-08-29 11:57:34+00:00"`),
not the `T` separator ISO8601 requires. Elasticsearch's default date parser
rejects that with a plain `400 Bad Request` and no field-specific detail,
which reads like a mapping problem rather than a formatting one. Fixed by
explicitly calling `.isoformat()` on every `datetime` field before
serializing (`consumer.py`'s `_index_to_es`).
