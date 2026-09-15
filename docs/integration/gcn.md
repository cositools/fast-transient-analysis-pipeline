# GCN integration

FasTP scientific tasks do not connect directly to Kafka. They use the COSIflow
GCN service's MySQL boundary:

```text
GCN Kafka -> receiver -> gcn_inbound_notices -> FasTP queries
FasTP task -> gcn_outbound_notices -> outbox worker -> GCN Kafka
```

[`query_relevant_grb_notices`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/pipeline/fast_transient_pipeline/gcn/query.py)
searches around a trigger or science interval. Missing PyMySQL, unavailable
database access, and schema/query errors return structured `unavailable`
metadata instead of failing the scientific task.

Final BGO, GeD, and ARM-selection tasks call the
[`outbox` helper](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/pipeline/fast_transient_pipeline/gcn/outbox.py).
It writes a JSON alert artifact, derives an idempotency key from DAG/task/run
identity, and inserts or updates a durable outbox row. Database and insert
errors on this final queueing path are not fail-open.

## Variables read by FasTP

| Area | Variables |
| --- | --- |
| Database | `GCN_DB_HOST`, `GCN_DB_PORT`, `GCN_DB_NAME`, `GCN_DB_USER`, `GCN_DB_PASSWORD`, `GCN_DB_CONNECT_TIMEOUT` |
| Inbox query | `GCN_QUERY_ENABLED`, `GCN_QUERY_WINDOW_MARGIN_SECONDS`, `GCN_QUERY_MAX_ROWS` |
| Topic selection | `GCN_BGO_OUTBOUND_TOPIC`, `GCN_GED_OUTBOUND_TOPIC`, `GCN_ARMSELECTION_OUTBOUND_TOPIC`, `GCN_OUTBOUND_TOPIC_DEFAULT` |
| Outbox row | `GCN_OUTBOUND_PRIORITY`, `GCN_MAX_ATTEMPTS` |
| Alert defaults | `GCN_COSI_ALERT_SCHEMA_URL`, `GCN_OUTBOUND_EVENT_ID`, `GCN_OUTBOUND_EVENT_NAME`, `GCN_OUTBOUND_ALERT_TENSE`, `GCN_OUTBOUND_RECORD_NUMBER` |
| Optional products | `GCN_OUTBOUND_CLASSIFICATION_JSON`, `GCN_OUTBOUND_DATA_ARCHIVE_PAGE`, `GCN_COSI_DATA_ARCHIVE_BASE_URL`, `GCN_OUTBOUND_PROB_SHORT`, `GCN_BGO_RATE_ENERGY_RANGE`, `GCN_BGO_TRIGGERED_SHIELD_SIGMA_THRESHOLD`, `GCN_CONTAINMENT_PROBABILITY` |

Variables such as `GCN_PRODUCER_ENABLED`, `GCN_DRY_RUN`, and
`GCN_REQUIRE_TEST_TOPICS` belong to the COSIflow outbox worker rather than
FasTP. Keep publication disabled/dry-run for development. The transport-side
contract is documented in the
[COSIflow repository documentation](https://github.com/cositools/cosiflow).

Topic selection for ARM selection falls back through its own topic, the GeD
topic, the default topic, and finally the built-in test topic. BGO and GeD use
their branch topic, then the default, then a built-in test topic.
