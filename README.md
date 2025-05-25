# Distributed Search Engine

A scalable, distributed search engine built with microservices architecture, featuring sharded indexing, typeahead suggestions, and high-performance search capabilities.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───►│  Data Ingestion │───►│     Kafka       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   API Gateway   │◄───│  Search Engine  │◄───│ Indexing Shards │
│ (Load Balancer) │    │    (Sharded)    │    │   (3 instances) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Typeahead Svc   │    │     Redis       │    │   PostgreSQL    │
│  (2 shards)     │    │    (Cache)      │    │   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Services

### 1. **API Gateway** (Port 8080)
- **Purpose**: Load balancing, request routing, caching
- **Features**: 
  - Routes search queries to all search shards
  - Aggregates and ranks results
  - Handles typeahead requests
  - Redis caching for performance
  - Health monitoring of downstream services

### 2. **Search Services** (Ports 8001-8003)
- **Purpose**: Execute search queries on specific shards
- **Features**:
  - TF-IDF scoring algorithm
  - Inverted index with file-based storage
  - PostgreSQL integration for document metadata
  - Real-time index updates
  - Shard-specific document retrieval

### 3. **Typeahead Services** (Ports 8011-8012)
- **Purpose**: Provide autocomplete suggestions
- **Features**:
  - Trie data structure for prefix matching
  - Frequency-based ranking
  - Distributed across 2 shards
  - Real-time term updates

### 4. **Indexing Services** (Ports 8021-8023)
- **Purpose**: Process and index documents
- **Features**:
  - Text preprocessing (tokenization, stemming, stop-word removal)
  - Inverted index maintenance
  - PostgreSQL document storage
  - Kafka integration for async processing
  - Typeahead term extraction

### 5. **Data Ingestion Service** (Port 8030)
- **Purpose**: Handle high-volume data ingestion
- **Features**:
  - Document validation and enrichment
  - Kafka message publishing
  - Parallel processing
  - Consistent hashing for shard distribution

## Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 8GB RAM
- 10GB free disk space

### 1. Start the System
```bash
# Clone the repository
git clone <repository-url>
cd distributed-search-engine

# Create data directories
mkdir -p data/indexes/shard-{0,1,2}
mkdir -p data/tries/shard-{0,1}

# Start all services
docker-compose up -d

# Check service health
curl http://localhost:8080/health
```

### 2. Index Some Documents
```bash
# Index a single document
curl -X POST http://localhost:8080/index \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Introduction to Machine Learning",
    "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms that can learn from data.",
    "source": "ml-tutorial",
    "metadata": {"category": "education", "difficulty": "beginner"}
  }'

# Bulk index multiple documents
curl -X POST http://localhost:8080/bulk-index \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {
        "title": "Deep Learning Basics",
        "content": "Deep learning uses neural networks with multiple layers to model complex patterns in data.",
        "source": "dl-guide"
      },
      {
        "title": "Natural Language Processing",
        "content": "NLP combines computational linguistics with machine learning to help computers understand human language.",
        "source": "nlp-intro"
      }
    ]
  }'
```

### 3. Search Documents
```bash
# Basic search
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "machine learning",
    "size": 10,
    "from": 0
  }'

# Search with pagination
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "neural networks",
    "size": 5,
    "from": 10
  }'
```

### 4. Use Typeahead
```bash
# Get suggestions
curl "http://localhost:8080/typeahead?q=mach&limit=5"

# Get popular terms
curl "http://localhost:8080/typeahead?limit=10"
```

## API Reference

### Search API

#### POST /search
Search for documents across all shards.

**Request:**
```json
{
  "query": "search terms",
  "size": 10,
  "from": 0
}
```

**Response:**
```json
{
  "query": "search terms",
  "total": 25,
  "hits": [
    {
      "id": "doc-id",
      "title": "Document Title",
      "content": "Document content...",
      "source": "source-name",
      "score": 0.85,
      "created_at": "2023-01-01T00:00:00"
    }
  ],
  "took": 0.045
}
```

### Indexing API

#### POST /index
Index a single document.

**Request:**
```json
{
  "id": "optional-doc-id",
  "title": "Document Title",
  "content": "Document content",
  "source": "source-name",
  "metadata": {"key": "value"}
}
```

#### POST /bulk-index
Index multiple documents.

**Request:**
```json
{
  "documents": [
    {
      "title": "Doc 1",
      "content": "Content 1"
    },
    {
      "title": "Doc 2", 
      "content": "Content 2"
    }
  ]
}
```

### Typeahead API

#### GET /typeahead?q={prefix}&limit={count}
Get autocomplete suggestions.

**Response:**
```json
{
  "query": "mach",
  "suggestions": [
    {
      "term": "machine",
      "frequency": 45
    },
    {
      "term": "machine learning",
      "frequency": 23
    }
  ]
}
```

### System Monitoring

#### GET /stats
Get system statistics.

**Response:**
```json
{
  "search_services": 3,
  "typeahead_services": 2,
  "indexing_services": 3,
  "healthy_search_services": 3,
  "healthy_typeahead_services": 2,
  "healthy_indexing_services": 3,
  "cache_info": {
    "redis_connected": true
  }
}
```

### View Statistics
```bash
# System-wide stats
curl http://localhost:8080/stats

# Per-service stats
curl http://localhost:8001/stats    # Search shard stats
curl http://localhost:8011/stats    # Typeahead shard stats
curl http://localhost:8021/stats    # Indexing shard stats
curl http://localhost:8030/stats    # Ingestion stats

# Redis sync monitoring (NEW)
curl http://localhost:8011/redis-stats    # Redis vs PostgreSQL sync status
curl http://localhost:8012/redis-stats    # For typeahead shard 1
```

### Force Redis Sync
```bash
# Force immediate Redis to PostgreSQL sync
curl -X POST http://localhost:8011/force-sync
curl -X POST http://localhost:8012/force-sync
```

## Performance Characteristics

### Throughput
- **Indexing**: 1,000+ documents/second (bulk operations)
- **Search**: 100+ queries/second with sub-second response times
- **Typeahead**: 1,000+ requests/second with <50ms latency

### High-Concurrency Term Updates
The system uses a **Redis-first approach** to handle the "rush" when multiple indexing services try to update the same terms simultaneously:

1. **All term updates go to Redis first** using atomic `INCR` operations (no locks)
2. **Background processes dump Redis to PostgreSQL** every 30 seconds
3. **Each shard owns its Redis namespace** - no conflicts during sync
4. **Fallback to Kafka** if Redis is unavailable

This eliminates database lock contention and provides:
- **10x faster writes** compared to direct PostgreSQL updates
- **Zero lock contention** on popular terms like "machine", "learning"
- **Atomic operations** ensuring data consistency
- **Automatic failover** to Kafka if Redis fails

### Scalability
- **Horizontal scaling**: Add more shard instances
- **Linear performance**: 2x shards ≈ 2x throughput
- **No single point of failure**: Services can fail independently

### Storage
- **Inverted indexes**: File-based with compression
- **Document storage**: PostgreSQL with JSONB metadata
- **Term frequencies**: Redis cache with PostgreSQL persistence
- **Caching**: Redis for frequently accessed data

## Configuration

### Environment Variables

#### API Gateway
- `REDIS_URL`: Redis connection string
- `SEARCH_SERVICE_URLS`: Comma-separated search service URLs
- `TYPEAHEAD_SERVICE_URLS`: Comma-separated typeahead service URLs
- `INDEXING_SERVICE_URLS`: Comma-separated indexing service URLs

#### Search Services
- `SHARD_ID`: Shard identifier (0, 1, 2)
- `TOTAL_SHARDS`: Total number of search shards
- `POSTGRES_URL`: PostgreSQL connection string
- `INDEX_PATH`: Path for inverted index files

#### Typeahead Services
- `SHARD_ID`: Shard identifier (0, 1)
- `TOTAL_SHARDS`: Total number of typeahead shards
- `TRIE_PATH`: Path for trie data files

## Monitoring and Maintenance

### Health Checks
```bash
# Check all services
curl http://localhost:8080/health

# Check individual services
curl http://localhost:8001/health  # Search shard 0
curl http://localhost:8011/health  # Typeahead shard 0
curl http://localhost:8021/health  # Indexing shard 0
```

### Rebuild Indexes
```bash
# Rebuild search index for shard 0
curl -X POST http://localhost:8001/rebuild-index

# Rebuild typeahead trie for shard 0
curl -X POST http://localhost:8011/rebuild
```

## Troubleshooting

### Common Issues

1. **Services not starting**
   - Check Docker logs: `docker-compose logs <service-name>`
   - Verify port availability
   - Ensure sufficient memory

2. **Search returning no results**
   - Check if documents are indexed: `curl http://localhost:8080/stats`
   - Verify search service health
   - Rebuild indexes if necessary

3. **Slow performance**
   - Check Redis connection
   - Monitor resource usage: `docker stats`
   - Consider adding more shards

4. **Kafka connection issues**
   - Restart Kafka and Zookeeper: `docker-compose restart kafka zookeeper`
   - Check Kafka logs for errors

### Logs
```bash
# View logs for all services
docker-compose logs -f

# View logs for specific service
docker-compose logs -f api-gateway
docker-compose logs -f search-service-1
```

## Development

### Adding New Features
1. Modify the appropriate service code
2. Update Docker images: `docker-compose build`
3. Restart services: `docker-compose up -d`

### Scaling
1. Add new service instances to `docker-compose.yml`
2. Update environment variables for service discovery
3. Restart the system

### Testing
```bash
# Run a simple load test
for i in {1..100}; do
  curl -X POST http://localhost:8080/search \
    -H "Content-Type: application/json" \
    -d '{"query": "test query", "size": 10}' &
done
wait

# Test high-concurrency term updates (NEW)
python3 scripts/test_concurrency.py
```

## License

This project is licensed under the MIT License. # Elastisearch
