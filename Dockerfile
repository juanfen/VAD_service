FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/python:3.10

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    VAD_HOST=0.0.0.0 \
    VAD_PORT=8766 \
    VAD_MODEL_DIR=/app/models/snakers4_silero-vad

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends libgomp1 libopus0 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

COPY app.py .

EXPOSE 8766

CMD ["python", "app.py"]
