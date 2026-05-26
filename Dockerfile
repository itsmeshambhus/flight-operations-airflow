FROM apache/airflow:2.9.3

# Install project dependencies at build time
# FIX: doing this in the image (not at runtime) means all containers share the same
# installed packages and startup is instant — no pip install on every docker compose up
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
