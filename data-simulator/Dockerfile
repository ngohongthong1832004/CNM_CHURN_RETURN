FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulate.py .

# Default: chạy 1 lần khi container start
# Override CMD khi cần thiết
ENTRYPOINT ["python", "simulate.py"]
CMD ["--help"]
