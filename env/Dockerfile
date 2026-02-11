# =============================================================================
#  Dockerfile — Runtime Container for FAST Pipeline
#  Base: Python 3.10 Slim (Debian)
#  Description: Lightweight container for executing scientific scripts with cosipy
# =============================================================================

FROM python:3.10-slim

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
ARG UID=
ARG GID=

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
COPY --chown=gamma:gamma requirements.txt /home/gamma/requirements.txt
# Activate env to install packages
RUN . /home/gamma/envs/cosipy/bin/activate && \
    python -m pip install --upgrade pip && \
    pip install -r /home/gamma/requirements.txt

# ------------------------------ Default Configuration ------------------------------
# Default to the stable environment
ENV VIRTUAL_ENV=/home/gamma/envs/cosipy
ENV PATH="/home/gamma/envs/cosipy/bin:$PATH"

CMD ["/bin/bash"]
