# Multi-Cloud Serverless Analytics Platform

A cloud-native analytics system built across **AWS and Google Cloud Platform**, demonstrating serverless architecture, scalable data processing, and containerised deployment.

Developed as part of MSc Cloud Computing coursework to showcase multi-cloud engineering, event-driven systems, and production-style deployment practices.

---

## 🚀 Project Overview

This project consists of two independent but complementary cloud deployments:

### 🔵 AWS: Serverless Text Analytics Engine
A scalable, event-driven text processing system built using:

- AWS Lambda
- API Gateway (HTTP API)
- Amazon S3 (event triggers)
- CloudWatch (monitoring)

The system performs:
- Top 20 word frequency analysis (stopword filtered)
- Top 10 sentence-start word analysis
- Sentence length statistics (mean, median, standard deviation)

Supports:
- API-based invocation
- Parallel batch processing via S3 triggers
- Automatic horizontal scaling

---

### 🔴 GCP: Cloud Run BigQuery Analytics Web App
A containerised Flask web application deployed to:

- Google Cloud Run
- Google BigQuery
- Docker
- Gunicorn

Features:
- Web interface to execute analytical SQL queries
- BigQuery-powered business analytics
- Secure authentication via Application Default Credentials
- Fully serverless query execution
- Automatic scaling to zero when idle

---

## 🏗 Architecture Summary

### AWS Flow
User → API Gateway → Lambda → JSON Response  
OR  
User → S3 Upload → Lambda Trigger → Output Bucket → CloudWatch

### GCP Flow
User → Cloud Run (Flask App) → BigQuery → Results Returned

---

## 🛠 Tech Stack

- Python
- AWS Lambda
- API Gateway
- Amazon S3
- CloudWatch
- Google Cloud Run
- BigQuery
- Docker
- Flask
- Gunicorn
- REST APIs

---

## ☁️ Cloud Engineering Concepts Demonstrated

- Multi-cloud architecture
- Serverless computing
- Event-driven processing
- Horizontal scalability
- Stateless workload design
- Containerisation & deployment
- Secure IAM-based authentication
- Cloud-native monitoring

---

## 📂 Repository Structure
