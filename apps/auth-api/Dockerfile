# FROM python:3.11-slim
# WORKDIR /app
# ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
# # RUN pip install --no-cache-dir fastapi uvicorn[standard] asyncpg passlib[bcrypt] pyjwt python-multipart email-validator
# COPY main.py /app/main.py
# CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]

FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]

