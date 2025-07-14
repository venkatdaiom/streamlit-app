FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Health check
HEALTHCHECK --interval=30s --timeout=5s \
    CMD curl -f http://localhost:8081/_health || exit 1

# Run both servers
CMD ["sh", "-c", \
    "streamlit run app.py --server.port=8080 --server.address=0.0.0.0 & \
     flask run --host=0.0.0.0 --port=8081"]
