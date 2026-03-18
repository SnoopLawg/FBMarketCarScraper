FROM python:3.12-slim

# Install Firefox ESR and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    firefox-esr \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install geckodriver (pinned version to avoid GitHub API rate limits)
ARG GECKODRIVER_VERSION=v0.35.0
RUN wget -q "https://github.com/mozilla/geckodriver/releases/download/${GECKODRIVER_VERSION}/geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz" && \
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

STOPSIGNAL SIGTERM

CMD ["python", "server.py"]
