FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY backfill_all.py ./

# Chạy một lần rồi thoát
CMD ["python", "/app/backfill_all.py"]
