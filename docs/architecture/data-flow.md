# Data flow

## Staging handoff

`init_pipelines` resolves only the inputs for the selected branch, stages them
under the shared raw root, and creates links in a unique dated products folder:

```text
<destination>/YYYY_MM/YYMMDDXXX/products
```

For GeD it stages source, background, orientation, and response files, then
creates a source-window background FITS product. BGO stages a light curve,
three localization lookup tables, and an orientation file; it bypasses the GeD
background cut and always selects `tdrss`.

```text
init_pipelines(GeD) -> tdrss/.../products
                         |- cosidag_GeD -> pipeline_config.yaml
                         `- cosidag_ARMselection -> armsel_pipeline_config.yaml

init_pipelines(BGO) -> tdrss/.../products
                         `- cosidag_BGO -> pipeline_config.yaml
```

The GeD and ARM-selection branches resolve the same input family but keep
separate mutable YAML state. Large scientific products remain on disk; task
XCom values carry configuration paths and compact result mappings.

## Other GeD destinations

The initializer accepts `lcurve`, `tsmap`, `fast`, and `tdrss` destination
roots. The currently shipped scientific COSIDAGs monitor only `tdrss`, so a
different destination stages files without creating an automatic handoff to
those DAGs.
