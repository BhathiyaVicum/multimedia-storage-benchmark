# 📘 Multimedia Storage Benchmark

> Evaluating HDFS, MinIO & MongoDB for Big Data Multimedia Applications

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-green.svg)](https://www.docker.com/)

## 📋 Overview

Empirical evaluation of three storage models for multimedia big data:
- **HDFS** (File-based)
- **MinIO** (Object-based)
- **MongoDB GridFS** (NoSQL)

| Metric | Value |
|--------|-------|
| **Dataset Size** | 2.2GB across 59 files |
| **Total Tests** | 120+ operations |
| **Success Rate** | 100% |

---

## 🎯 Key Results

### Performance Rankings
| Operation | 1st | 2nd | 3rd |
|-----------|-----|-----|-----|
| **Upload** | HDFS (50.1 MB/s) | MinIO (40.9 MB/s) | MongoDB (25.0 MB/s) |
| **Download** | MinIO (60.2 MB/s) | HDFS (26.9 MB/s) | MongoDB (26.5 MB/s) |

### File Size Impact
| Category | Avg Speed | File Count | Total Size |
|----------|-----------|------------|------------|
| **Large** | 115.9 MB/s | 2 files | 1.5 GB |
| **Medium** | 15.8 MB/s | 23 files | 600 MB |
| **Small** | 10.1 MB/s | 34 files | 105 MB |

---

## ⚙️ Tech Stack
- **Python 3.11**
- **Dockerized Environment**
- **Pandas & Matplotlib** (for data analysis)
- **HDFS / MinIO / MongoDB GridFS**

---

## 🧩 Conclusion
- **MinIO** → Best for streaming & fast access workloads  
- **HDFS** → Ideal for distributed batch processing  
- **MongoDB GridFS** → Suited for metadata-heavy applications  

---

---

## 📁 Repository Structure
