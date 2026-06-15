FROM python:3.11-slim

WORKDIR /app

# Optimize layer caching
RUN pip install --no-cache-dir \
        "fastapi==0.111.0" \
        "uvicorn[standard]==0.30.1" \
        "psutil==5.9.8"

COPY main.py .

# Dynamic binding via environment $PORT handled natively
EXPOSE 8000

CMD ["python", "main.py"]