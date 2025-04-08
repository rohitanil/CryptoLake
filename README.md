# Real-Time Weather Data Pipeline with Kafka, Spark and Iceberg with LLM Support

## Pipeline Architecture
![Real-Time Weather Data Pipeline](artifacts/Producer.png)

A scalable, real-time data pipeline that ingests weather data from public APIs, processes it using Spark Structured Streaming, and stores it in an Iceberg data lake with Nessie version control.

## Features
1. Real-time ingestion of weather data from multiple locations
2. Kafka-based event streaming for reliable data delivery
3. Spark Structured Streaming for processing and transformation
4. Iceberg tables with Nessie version control for data governance
5. MinIO (S3-compatible) storage backend
6. AI-assisted querying using Claude 3 for natural language to SQL conversion
7. Docker-based deployment for easy local development

## Tech Stack
| Component          | Purpose                              |
|:-------------------|:-------------------------------------|
| Apache Kafka       | Distributed event streaming          |
| Apache Spark       | Stream processing engine             |
| Apache Iceberg     | Open table format for analytics      |
| Project Nessie     | Git-like version control for data    |
| MinIO              | S3-compatible object storage         |
| Claude 3 Sonnet    | AI for SQL generation                |

## Getting Started
### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Java 11 (for Spark)
- AWS credentials (for MinIO access)
- Weather API token (set as WEATHER_API_TOKEN)
- Anthropic API key (set as ANTHROPIC_KEY) for AI queries

### Installation
Clone the repository:
```
git clone https://github.com/yourusername/weather-data-pipeline.git
```
Start the infrastructure:
```
docker compose up -d
```
Set up environment variables:
```
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_S3_ENDPOINT=http://localhost:9000
export WEATHER_API_TOKEN=your_api_token
export ANTHROPIC_KEY=your_anthropic_key
```
Running the Pipeline
Start the data producer:
```
python main.py
```
In a separate terminal, start the Spark stream processor:
```
spark-submit stream_processor.py
```
To query the data:
```
python query_iceberg_table.py
```

### Example Queries
The AI query interface supports natural language questions like:
1. `"Show me the average temperature by country for the last 24 hours"`
2. `"Find locations with rainfall exceeding 10mm in the past hour"`
3. `"What was the maximum wind gust recorded in California yesterday?"`

### Monitoring
Access these services locally:
1. MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
2. Spark UI: http://localhost:4040
3. Nessie API: http://localhost:19120/api/v1

### Contributing
Contributions are welcome! Please open an issue or submit a pull request.

## License
Apache License 2.0
Note: This is a simplified example. For production use, consider adding:
Proper error handling and retries
Monitoring and alerting
Security configurations
CI/CD pipelines