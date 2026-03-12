FROM python:3.12-slim

# Install Firefox ESR and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    firefox-esr \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install geckodriver
RUN GECKODRIVER_VERSION=$(wget -qO- https://api.github.com/repos/mozilla/geckodriver/releases/latest | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'])") && \
    wget -q "https://github.com/mozilla/geckodriver/releases/download/${GECKODRIVER_VERSION}/geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz" && \
    tar -xzf geckodriver-*.tar.gz -C /usr/local/bin/ && \
    rm geckodriver-*.tar.gz && \
    chmod +x /usr/local/bin/geckodriver

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DATA_DIR=/data
ENV HEADLESS=1
ENV DOCKER_MODE=1
ENV FLASK_HOST=0.0.0.0

EXPOSE 5001

VOLUME /data

CMD ["python", "server.py"]
