# Python 3.12 기반의 Airflow 3.2.1 이미지 사용
FROM apache/airflow:3.2.1-python3.12

USER root

# 시스템 의존성 설치 (컴파일러 및 기본 도구)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# pip 및 빌드 도구 최신화
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# requirements.txt 복사 및 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt