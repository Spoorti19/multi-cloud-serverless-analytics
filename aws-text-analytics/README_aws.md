# AWS Serverless Text Analytics System

## Overview
This project implements a serverless text analysis pipeline using AWS Lambda, API Gateway, and Amazon S3.  
The system performs lexical and structural analysis on input text and supports automatic scaling through event-driven execution.

The solution was developed as part of a Cloud Computing coursework project to demonstrate serverless architecture, scalability, and monitoring.

---

## Features

- Top 20 word frequency analysis (excluding stopwords)
- Top 10 sentence-start word frequency
- Sentence length statistics (mean, median, standard deviation)
- Event-driven processing via API Gateway
- Parallel execution using S3 event triggers
- Automatic scaling with AWS Lambda
- Monitoring via Amazon CloudWatch

---

## Architecture

### Single Request Flow (API-based)

User → API Gateway (HTTP POST) → AWS Lambda → JSON Response

### Batch Processing Flow (Parallel via S3)

User uploads files → S3 bucket → Lambda triggered per file → Results stored in output bucket → Monitored via CloudWatch

---

## Technologies Used

- Python 3.x
- AWS Lambda
- Amazon API Gateway
- Amazon S3
- Amazon CloudWatch
- RESTful API integration

---

## Example Input (Event Payload)

```json
{
  "text": "Cloud computing enables scalable applications. Serverless functions scale automatically.",
  "analyses": {
    "word_freq": true,
    "sentence_starts": true,
    "sentence_stats": true
  }
}