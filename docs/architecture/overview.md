# Architecture overview

FasTP is loaded into COSIflow as a module. COSIflow supplies Airflow, managed
external Python environments, shared storage, and the GCN database boundary.
FasTP supplies the staging and scientific DAG definitions plus their callables.

`init_pipelines` does not directly trigger a scientific DAG. It creates a dated
`products` directory below a destination root. An unpaused COSIDAG sensor then
discovers that directory according to its monitoring policy.

| Layer | Owned by | Responsibility |
| --- | --- | --- |
| Orchestration | COSIflow | Airflow, COSIDAG folder monitoring, module loading |
| Input staging | FasTP | Validate or download input selections, create product handoff |
| Scientific analysis | FasTP | BGO, binned GeD, and ARM-selection task graphs |
| Notice transport | COSIflow | GCN receiver, inbox database, outbox worker, Kafka client |
| Notice mapping | FasTP | Query relevant notices and queue COSI alert payloads |

The [data-flow page](data-flow.md) describes the filesystem handoff. Runtime
isolation is documented separately in [runtime environments](runtime-environments.md).
