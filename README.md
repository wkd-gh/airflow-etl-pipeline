# equity-derivative-etl

<p align="left">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=Python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=Apache%20Airflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=Databricks&logoColor=white"/>
</p>

---

## English Version

### Project Overview
**equity-derivative-etl** is a robust ETL pipeline designed to ingest and process public financial data provided by the Financial Services Commission (FSC) of Korea. This project automates the collection of KRX listing information, stock prices, and securities market data to build a reliable data foundation within a Data Lakehouse environment.

### Key Features
* **Automated Data Ingestion**: Seamlessly extracts financial data from the Public Data Portal (FSC) Open APIs.
* **Workflow Orchestration**: Manages complex data pipelines, scheduling, and monitoring using **Apache Airflow**.
* **Data Lakehouse Integration**: Optimized for loading and processing large-scale financial datasets into the **Databricks** platform.
* **Scalable Architecture**: Built to handle daily financial updates with modular ETL logic.

### Tech Stack
* **Language**: Python
* **Orchestration**: Apache Airflow
* **Platform**: Databricks
* **Data Source**: Public Data Portal (FSC Open API)
    * KRX Listed Securities Information
    * Securities Product Price Information
    * Stock Price Information

### Pipeline Flow
1. **Extract**: Fetch raw data from FSC Open APIs via REST requests.
2. **Transform**: Cleanse and structure the data using Python/Spark.
3. **Load**: Sink the processed data into Databricks Delta tables for analytical use.

---

## 한국어 버전

### 프로젝트 개요
**equity-derivative-etl**은 금융위원회에서 제공하는 금융 공공데이터를 수집 및 처리하기 위한 고도화된 ETL 파이프라인입니다. 본 프로젝트는 KRX 상장 종목 정보, 주식 및 증권 상품 시세 데이터의 수집을 자동화하여 데이터 레이크하우스(Data Lakehouse) 환경 내에 신뢰할 수 있는 데이터 기반을 구축합니다.

### 주요 특징
* **데이터 수집 자동화**: 공공데이터포털(금융위원회) API를 통해 금융 데이터를 안정적으로 추출합니다.
* **워크플로우 오케스트레이션**: Apache Airflow를 활용하여 복잡한 데이터 파이프라인의 스케줄링 및 모니터링을 수행합니다.
* **데이터 레이크하우스 통합**: 대규모 금융 데이터셋을 Databricks 플랫폼에 최적화된 방식으로 적재하고 처리합니다.
* **확장 가능한 아키텍처**: 모듈화된 ETL 로직을 통해 일일 금융 업데이트를 처리할 수 있도록 설계되었습니다.

### 기술 스택
* **언어**: Python
* **워크플로우 관리**: Apache Airflow
* **플랫폼**: Databricks
* **데이터 소스**: 공공데이터포털 (금융위원회 API)
    * KRX 상장종목 정보
    * 증권상품 시세 정보
    * 주식 시세 정보

### 파이프라인 흐름
1. **Extract (추출)**: REST API를 통해 금융위원회 공공데이터로부터 로우(Raw) 데이터를 가져옵니다.
2. **Transform (변환)**: Python 및 Spark를 사용하여 데이터를 정제하고 구조화합니다.
3. **Load (적재)**: 처리된 데이터를 분석용 Databricks Delta 테이블에 적재합니다.
