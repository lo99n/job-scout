FROM python:3.11-slim

# Install system deps for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium for JS-heavy scrapers
RUN pip install playwright && playwright install --with-deps chromium

# Copy all source files
COPY . .

ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Berlin

CMD ["python", "main.py"]
