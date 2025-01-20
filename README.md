# Real-Time Kafka Streaming Pipeline

## Overview
This project implements a real-time streaming data pipeline using Kafka and Docker for processing user login events. The pipeline ingests streaming data, performs real-time processing, and stores enriched data in a new Kafka topic.

## Architecture & Data Flow
1. **Data Ingestion**
   - Source: Data generator producing to 'user-login' topic
   - Format: JSON messages containing user login events
   - Ingestion Rate: Real-time streaming

2. **Processing Layer**
   - Consumer Group: Scalable consumer implementation
   - Real-time Processing:
     - Timestamp enrichment
     - Geographic analysis based on IP
     - Session tracking
     - Device and version analytics
     - Hourly activity patterns

3. **Output Layer**
   - Processed data stored in 'processed-user-login' topic
   - Enriched schema with additional analytics
   - Real-time statistics logging

## Design Choices

### Efficiency
- Batch processing capability for high-volume scenarios
- Optimized JSON serialization/deserialization
- Memory-efficient data structures using defaultdict
- Minimal data copying in processing pipeline

### Scalability
- Consumer group support for horizontal scaling
- Stateless processing enabling multiple instances
- Independent processing of each message
- Configurable batch sizes and processing windows

### Fault Tolerance
- Comprehensive error handling at each step
- Automatic message commitment after processing
- Failed message logging for debugging
- Graceful shutdown handling
- Container restart policies

## Setup Instructions

1. Prerequisites:
```bash
# Install Docker and Docker Compose
# Clone this repository
git clone <repository-url>
cd kafka-streaming-pipeline
```

2. Start the Environment:
```bash
# Start all containers
docker-compose up -d

# Verify containers are running
docker ps
```

3. Run the Consumer:
```bash
# Install Python dependencies
pip install kafka-python pandas numpy

# Start the consumer
python kafka_consumer.py
```

## Monitoring & Maintenance

### Metrics Tracked
- Message processing rates
- Geographic distribution
- Device type distribution
- App version distribution
- Session statistics
- Error rates

### Health Checks
- Container health monitoring
- Consumer lag monitoring
- Error rate monitoring
- Processing time tracking

## Production Considerations

### Deployment Strategy
1. Use Kubernetes for container orchestration
2. Implement CI/CD pipeline
3. Set up monitoring and alerting
4. Use configuration management

### Additional Components for Production
1. Elasticsearch for log aggregation
2. Prometheus/Grafana for monitoring
3. Redis for caching
4. Multiple Kafka brokers for redundancy

### Scaling Strategies
1. Horizontal scaling of consumers
2. Kafka partition management
3. Load balancing
4. Data retention policies

## Code Structure
```
├── docker-compose.yml      # Docker environment configuration
├── kafka_consumer.py       # Main consumer implementation
└── README.md              # Project documentation
```