FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    HOME=/tmp \
    TMPDIR=/tmp \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      nodejs npm default-jdk gradle \
    && pip install --no-cache-dir pytest \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
USER 10001:10001
