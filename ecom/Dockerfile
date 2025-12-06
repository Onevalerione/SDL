FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/main.py .

RUN mkdir -p /app/logs

CMD ["python3", "main.py"]
