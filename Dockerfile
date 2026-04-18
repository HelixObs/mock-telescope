# Build context must be the HelixObs root (set in deploy/docker-compose.yml):
#   build:
#     context: ../
#     dockerfile: mock-telescope/Dockerfile
#
# This lets us install helixobs from source (client-python/) without PyPI.

FROM python:3.13-slim
WORKDIR /app

COPY client-python/ ./client-python/
RUN pip install --no-cache-dir ./client-python

COPY mock-telescope/pyproject.toml .
COPY mock-telescope/chime/ ./chime/
RUN pip install --no-cache-dir ".[dev]"

COPY mock-telescope/simulate.py .
CMD ["python", "-u", "simulate.py"]
