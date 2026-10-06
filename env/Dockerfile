# =============================================================================
#  Dockerfile — Runtime Container for FAST Pipeline
#  Base: Python 3.12 Slim (Debian)
#  Description: Lightweight container for executing scientific scripts with cosipy
# =============================================================================

# FROM python:3.10-slim
FROM python:3.12-slim-bookworm@sha256:76d4b7b6305788c6b4c6a19d6a22a3921bf802e9af4d5e1e5bd771208dba74bf

# Debian repositories are mutable even when the base image is pinned. Resolve
# OS packages from one immutable snapshot for the 1.0.0 release candidate.
ARG DEBIAN_SNAPSHOT=20261001T000000Z
RUN sed -i \
      -e "s|http://deb.debian.org/debian|https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}|g" \
      -e "s|http://deb.debian.org/debian-security|https://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}|g" \
      /etc/apt/sources.list.d/debian.sources \
    && printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99snapshot

# ------------------------------ System Dependencies ------------------------------
# Install minimal libraries required for scientific packages (HDF5, GSL, compilers)
# We use apt-get for Debian-based images (like python:slim)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    wget \
    curl \
    libhdf5-dev \
    libgsl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------ Non-root user ------------------------------
ARG UID=501
ARG GID=20

# Create group and user with specific UID/GID
RUN groupadd -g "${GID}" gamma || true && \
    useradd -m -u "${UID}" -g "${GID}" -s /bin/bash gamma || true

# Create shared directories with correct permissions
RUN mkdir -p /shared_dir /data01 /data02 && \
    chown -R "${UID}:${GID}" /shared_dir /data01 /data02

USER gamma
WORKDIR /home/gamma

# ------------------------------ Python Environments ------------------------------

# --- Environment: cosipy (Stable) ---
RUN python -m venv /home/gamma/envs/cosipy
COPY --chown=gamma:gamma locks/cosipy-py312.lock /home/gamma/cosipy-py312.lock
# Activate env to install packages
RUN . /home/gamma/envs/cosipy/bin/activate && \
    python -m pip install --upgrade pip==26.1.2 setuptools==80.9.0 wheel==0.47.0 && \
    pip install -r /home/gamma/cosipy-py312.lock && \
    python -m pip check

# ------------------------------ Default Configuration ------------------------------
# Default to the stable environment
ENV VIRTUAL_ENV=/home/gamma/envs/cosipy
ENV PATH="/home/gamma/envs/cosipy/bin:$PATH"

CMD ["/bin/bash"]
