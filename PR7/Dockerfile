FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .
RUN mkdir -p /app/logs

EXPOSE 5000
CMD ["python", "main.py"]
