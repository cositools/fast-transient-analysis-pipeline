# First run

## Confirm the installation

Open the Airflow web interface on the port selected during installation
(`8080` by default). The DAG list should include:

- `init_pipelines`;
- `cosidag_BGO`;
- `cosidag_GeD`;
- `cosidag_ARMselection`.

If they do not appear, use **Develop Tools → Refresh DAGs List** in COSIflow or
run `docker compose exec airflow airflow dags list` from its Compose directory.

## GeD and ARM-selection smoke run

1. Pause `cosidag_BGO`.
2. Unpause `cosidag_GeD`, `cosidag_ARMselection`, or both.
3. Trigger `init_pipelines` with `pipeline_branch=GeD` and
   `destination=tdrss`.
4. Keep the four input paths at `__default__` for the configured DC4 example.
5. Wait for `background_cut` and `initialization_complete`.

The unpaused GeD-family COSIDAGs independently discover the resulting
`products` directory. `cosidag_GeD` writes `pipeline_config.yaml` and
`cosidag_ARMselection` writes `armsel_pipeline_config.yaml`.

## BGO smoke run

1. Pause `cosidag_GeD` and `cosidag_ARMselection`.
2. Unpause `cosidag_BGO`.
3. Trigger `init_pipelines` with `pipeline_branch=BGO`.
4. Keep the five BGO paths at `__default__` for the configured run17 data.
5. Confirm that `background_cut` is skipped and staging completes.

All scientific COSIDAGs monitor the same `tdrss` root, and file matching occurs
after folder selection. Pause unrelated branches during focused smoke tests so
they do not claim an incompatible `products` directory.

See the [DAG reference](../reference/dags.md) for exact task graphs, patterns,
and monitoring settings.
