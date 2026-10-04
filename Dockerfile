FROM python:3.13-slim

WORKDIR /bridge

# git is needed to install the pinned maxapi-python fork from requirements.txt
# (a PEP 508 direct reference is fetched by git, not from PyPI). Installed in
# its own layer so it doesn't ship in the final image, and purged afterwards.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

COPY main.py .
COPY app/ app/

RUN mkdir -p cache data

# PYTHONUNBUFFERED: see prints/logs in `docker logs`
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
