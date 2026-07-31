FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# inbox.json + api_logs.json live here. Mount a volume on /app/data to keep
# them across pod restarts; without one they survive container restarts only.
ENV IM_DATA_DIR=/app/data
RUN mkdir -p /app/data

EXPOSE 8000

#CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8000","--limit-concurrency","100"]
