# First run

## Confirm the installation

Open the Airflow web interface on the port selected during installation
(`8080` by default). The DAG list should include:

- `init_pipelines`;
- `cosidag_BGO`;
- `cosidag_GeD`.

If they do not appear, use **Develop Tools → Refresh DAGs List** in COSIflow or
run `docker compose exec airflow airflow dags list` from its Compose directory.

## GeD smoke run

1. Pause `cosidag_BGO`.
2. Unpause `cosidag_GeD`.
3. Trigger `init_pipelines` with `pipeline_branch=GeD` and
   `destination=tdrss`.
4. Keep the four input paths at `__default__` for the configured DC4 example.
5. Wait for `background_cut` and `initialization_complete`.

The unpaused `cosidag_GeD` discovers the resulting `products` directory and
writes `pipeline_config.yaml`.

## BGO smoke run

1. Pause `cosidag_GeD`.
2. Unpause `cosidag_BGO`.
3. Trigger `init_pipelines` with `pipeline_branch=BGO`.
4. Keep the five BGO paths at `__default__` for the configured run17 data.
5. Confirm that `background_cut` is skipped and staging completes.

All scientific COSIDAGs monitor the same `tdrss` root, and file matching occurs
after folder selection. Pause unrelated branches during focused smoke tests so
they do not claim an incompatible `products` directory.

## Initialize both families in one run

Trigger `init_pipelines` with:

```json
{
  "pipeline_branch": "both",
  "destination": "tdrss",
  "data_challenge": "DC4"
}
```

Choose the GeD and BGO inputs independently in the trigger form. Both presets
are staged, but the products are kept in two distinct run directories with the
existing date/index layout. After `initialization_complete` succeeds, inspect
`create_products_dir` XCom: `products_dirs.GeD` and `products_dirs.BGO` identify
the exact outputs. `background_cut` runs only on the GeD products; both selected
postprocessing paths must finish successfully.

This convenience option does not launch the scientific COSIDAGs, modify their
monitoring logic or repair existing queued/claimed paths. Because their shared
root can still contain incompatible families, inspect the actual folder claimed
by each scientific run. Preparing both sets does not resolve a BGO run already
waiting on an old GeD folder. Treat family-aware candidate filtering as a
separate selection fix before relying on unattended simultaneous consumers.

See the [DAG reference](../reference/dags.md) for exact task graphs, patterns,
and monitoring settings.
