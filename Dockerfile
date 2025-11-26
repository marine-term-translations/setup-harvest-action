FROM python:3.11-slim

WORKDIR /action

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY harvest.py .

ENTRYPOINT ["python", "/action/harvest.py"]
