FROM python:3.13-slim
WORKDIR /app

COPY pyproject.toml .
COPY chime/ ./chime/
RUN pip install --no-cache-dir ".[dev]"

COPY simulate.py .
CMD ["python", "-u", "simulate.py"]
