# Feast Feature Store Service

> Production-ready feature store for the Washing Machine Anomaly Detection System

[![Feast](https://img.shields.io/badge/Feast-0.39+-blue.svg)](https://feast.dev)
[![Redis](https://img.shields.io/badge/Redis-7.0-red.svg)](https://redis.io)
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://python.org)

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#️-architecture)
- [Data Flow](#data-flow)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Feature Definitions](#-feature-definitions)
- [Additional Resources](#-additional-resources)


## 🎯 Overview

The Feast Feature Store service provides a production-grade feature serving layer for real-time anomaly detection in industrial washing machines. It manages **15 features** across two feature views per machine, enabling:

- **Real-time inference** with sub-100ms latency
- **Point-in-time correct** historical features for training
- **Feature versioning** and lineage tracking
- **Consistent serving** between training and inference

### Key Features

- ✅ **13 streaming features** (raw sensors + rolling-window aggregations)
- ✅ **2 batch features** (daily / weekly long-horizon aggregations)
- ✅ **Online store** (Redis) for real-time serving
- ✅ **Offline store** (Parquet) for training data
- ✅ **HTTP API** for easy integration
- ✅ **Python SDK** for advanced use cases
- ✅ **Push & batch** ingestion support

---

## 🏗️ Architecture


### Feature Store Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                   FEAST FEATURE STORE                          │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Feature Server (HTTP API - Port 8001)                  │   │
│  │  • GET  /health                                         │   │
│  │  • POST /get-online-features                            │   │
│  │  • POST /push                                           │   │
│  │  • POST /materialize                                    │   │
│  └────────────┬─────────────────────────┬──────────────────┘   │
│               │                         │                      │
│    ┌──────────▼──────────┐   ┌─────────▼──────────┐            │
│    │  Online Store       │   │  Offline Store      │           │
│    │  (Redis)            │   │  (Parquet Files)    │           │
│    │                     │   │                     │           │
│    │  • Sub-100ms reads  │   │  • Training data    │           │
│    │  • Real-time serve  │   │  • Point-in-time    │           │
│    │  • TTL: 12h (stream)│   │  • Historical data  │           │
│    │  • TTL: 7d (batch)  │   │                     │           │
│    └─────────────────────┘   └─────────────────────┘           │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Registry (SQLite)                                      │   │
│  │  • Entity: machine (Machine_ID: Int64)                  │   │
│  │  • Feature Views: stream (12h TTL), batch (7d TTL)      │   │
│  │  • Feature Service: machine_anomaly_service_v1          │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Training Flow:
  Parquet Files → Offline Store → get_historical_features() → ML Model

Inference Flow:
  Redis → Online Store → get_online_features() → ML Model (Prediction)

Streaming Flow:
  Kafka/Sensors → push() → Online Store → Available for Inference
```

---

## 🚀 Quick Start


```bash
make run_feature_store
```

### 2. TEST with POSTMAN (First Time Only)

```bash
IMPORT FILE WITH POSTMAN
```

------> [FILE](\Feast_API_Tests.postman_collection.json)


**🎉 You're ready to go!**

---

## 📁 Project Structure

```
feature_store_service/
├── config/
│   └── feature_store.yaml          # Feast configuration
├── src/
│   ├── __init__.py                 # Package exports
│   ├── data_sources.py             # Batch & streaming sources
│   ├── entity.py                   # Machine entity definition
│   ├── features.py                 # Feature view definitions
│   ├── feature_services.py         # Feature service (ML contract)
│   └── test_functionality.py       # Integration tests
├── Dockerfile                      # Container definition
└── README.md                       # This file

# Generated at runtime:
data/
├── registry/
    └── registry.db
```



## 🔧 Feature Definitions

### Entity: Machine

| Field | Type | Description |
|-------|------|-------------|
| `Machine_ID` | Int64 | Unique identifier for each washing machine |

---

### Feature Views

#### machine_streaming_features (Real-time)

**TTL:** 12 hours  
**Source:** `washing_stream_push` (PushSource → backed by `washing_batch_source` for history)  
**Use:** Online inference, real-time anomaly detection

| Feature | Type | Source | Description | Unit |
|---------|------|--------|-------------|------|
| `Cycle_Phase_ID` | Int64 | Raw sensor | Current wash cycle phase (1–4) | — |
| `Current_L1` | Float32 | Raw sensor | Phase L1 current draw | Amps |
| `Current_L2` | Float32 | Raw sensor | Phase L2 current draw | Amps |
| `Current_L3` | Float32 | Raw sensor | Phase L3 current draw | Amps |
| `Voltage_L_L` | Float32 | Raw sensor | Line-to-line voltage | Volts |
| `Water_Temp_C` | Float32 | Raw sensor | Water temperature | °C |
| `Motor_RPM` | Float32 | Raw sensor | Motor speed | RPM |
| `Water_Flow_L_min` | Float32 | Raw sensor | Water flow rate | L/min |
| `Vibration_mm_s` | Float32 | Raw sensor | Instantaneous vibration level | mm/s |
| `Water_Pressure_Bar` | Float32 | Raw sensor | Water pressure | Bar |
| `Current_Imbalance_Ratio` | Float32 | Streaming pipeline | `(max(L1,L2,L3) − min) / mean` — instantaneous 3-phase imbalance scalar | — |
| `Vibration_RollingMax_10min` | Float32 | Streaming pipeline | 10-minute rolling maximum of `Vibration_mm_s` per machine | mm/s |
| `Current_Imbalance_RollingMean_5min` | Float32 | Streaming pipeline | 5-minute rolling mean of `Current_Imbalance_Ratio` per machine | — |

---

#### machine_batch_features (Historical Aggregations)

**TTL:** 7 days  
**Source:** `washing_batch_source` (FileSource → partitioned Parquet written by PySpark)  
**Use:** Training data, long-horizon deterioration signals

| Feature | Type | Aggregation window | Description | Unit |
|---------|------|--------------------|-------------|------|
| `Daily_Vibration_PeakMean_Ratio` | Float32 | Per machine, per calendar day | `max(Vibration_mm_s) / mean(Vibration_mm_s)` — high ratio signals repeated shock events and mechanical deterioration | — |
| `Weekly_Current_StdDev` | Float32 | Per machine, per week | Standard deviation of `Current_L1` over the week — grows as motor insulation degrades or mechanical resistance increases | Amps |

---

### Feature Service

#### machine_anomaly_service_v1

Combines both feature views into a single versioned contract for the anomaly detection model. A single entity-row lookup on `Machine_ID` returns the full 15-feature vector at inference time.

| Included view | Features | TTL |
|---------------|----------|-----|
| `machine_streaming_features` | 13 (raw sensors + rolling windows) | 12 hours |
| `machine_batch_features` | 2 (daily / weekly aggregations) | 7 days |

---

## 📚 Additional Resources

### Documentation

- [Feast Official Docs](https://docs.feast.dev)
- [Redis Documentation](https://redis.io/docs)
- [Feature Store Concepts](https://docs.feast.dev/getting-started/concepts)


## 🤝 Contributing

### Adding a New Feature

1. Update `features.py` with new field
2. Run `make run_feature_store`
3. Update documentation
4. Test with sample data

---

**Built with ❤️ for production ML systems**
