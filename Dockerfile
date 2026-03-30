FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/opt/dagster/app/src

WORKDIR /opt/dagster/app

# Keep system deps minimal for scientific Python wheels on CPU images.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY workspace.yaml ./workspace.yaml

RUN python -m pip install --upgrade pip && \
    pip install --extra-index-url https://download.pytorch.org/whl/cpu torch==2.10.0 torchvision==0.25.0 && \
    pip install . && \
    pip install dagster-webserver==1.12.12 dagster-postgres==0.28.12

RUN mkdir -p ${DAGSTER_HOME} /opt/dagster/app/data/models /opt/dagster/app/data/calibrations

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace.yaml"]
