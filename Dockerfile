FROM vad-base:v1.0

ENV VAD_HOST=0.0.0.0 \
    VAD_PORT=8766 \
    VAD_MODEL_DIR=/app/models/snakers4_silero-vad

WORKDIR /app

COPY app.py .

EXPOSE 8766

CMD ["python", "app.py"]