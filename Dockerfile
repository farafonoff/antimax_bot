FROM python:3.13-slim

WORKDIR /bridge

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY app/ app/

RUN mkdir -p cache data

# PYTHONUNBUFFERED: see prints/logs in `docker logs`
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
