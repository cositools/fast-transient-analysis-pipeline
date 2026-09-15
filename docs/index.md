# FasTP

The Fast Transient Analysis Pipeline (FasTP) is a COSIflow module for rapid
analysis of transient events observed by the
[COSI mission](https://cosi.ssl.berkeley.edu/). It ships one staging DAG and
three filesystem-driven scientific branches:

| DAG | Responsibility |
| --- | --- |
| `init_pipelines` | Resolve and stage GeD or BGO inputs |
| `cosidag_BGO` | BGO duration, background, light curve, significance, and non-imaging localization |
| `cosidag_GeD` | Binned GeD likelihood, TS-map, light-curve, and duration analysis |
| `cosidag_ARMselection` | Unbinned ARM-gated light curve and HEALPix localization |

Start with [installation](getting-started/installation.md), then follow the
[first-run guide](getting-started/first-run.md). The
[DAG catalog](reference/dags.md) records the current graphs and explicitly
identifies placeholder nodes.

Pipeline developers can use FasTP as a worked COSIflow module example. The
[development guide](development/using-fastp-as-example.md) identifies the
reusable structure and the scientific details that should not be copied as a
generic contract.

## Support boundary

This documentation describes the checked-in `dev` branch contract. Scientific
results and mutable state remain owned by the individual DAGs. Placeholder
tasks are not presented as implemented features, and external GCN publication
must remain disabled unless it has been separately authorized and configured.
