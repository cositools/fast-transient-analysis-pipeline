# Runtime environments

The module configuration currently selects `install_mode: environment` and
enables three managed Python environments:

| Environment | Python | Requirements file | Current use |
| --- | --- | --- | --- |
| `cosipy` | 3.12 | `env/requirements_cosipy.txt` | staging, GeD background cut, GeD, ARM selection, BGO preprocessing/significance |
| `bct` | 3.11 | `env/requirements_bct.txt` | BGO duration/background/light curves and GeD duration |
| `nimcosipy` | 3.12 | `env/requirements_nimcosipy.txt` | BGO non-imaging localization |

Staging and the GeD background cut are `BashOperator` tasks. They explicitly
invoke `/home/gamma/envs/cosipy/bin/python` by default through the
`COSIPY_PYTHON` environment variable. They do **not** use the optional module
Docker image. This is the current runtime contract in
[`cosipipe_initpipeline.py`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/dags/cosipipe_initpipeline.py).

The repository also contains a Python 3.12 Dockerfile that installs the COSIpy
requirements. The loader builds it only for `container` or `both` installation
modes, and none of the current FasTP DAG tasks launch that image.

The environment split isolates incompatible scientific dependency trees. It is
not a complete release lock: some application requirements still reference
moving VCS branches, and the image base is not digest-pinned. Resolve that
separately before claiming bit-for-bit application-runtime reproducibility.
