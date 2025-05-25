import os
import json
import hashlib
import requests
import redis
from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from typing import List, Dict, Any
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
SEARCH_SERVICE_URLS = os.getenv('SEARCH_SERVICE_URLS', '').split(',')
TYPEAHEAD_SERVICE_URLS = os.getenv('TYPEAHEAD_SERVICE_URLS', '').split(',')
INDEXING_SERVICE_URLS = os.getenv('INDEXING_SERVICE_URLS', '').split(',')

# Initialize Redis
redis_client = redis.from_url(REDIS_URL)

class ServiceRegistry:
    def __init__(self):
        self.search_services = [url.strip() for url in SEARCH_SERVICE_URLS if url.strip()]
        self.typeahead_services = [url.strip() for url in TYPEAHEAD_SERVICE_URLS if url.strip()]
        self.indexing_services = [url.strip() for url in INDEXING_SERVICE_URLS if url.strip()]
        
    def get_shard_for_query(self, query: str, total_shards: int) -> int:
        """Get shard ID for a query using consistent hashing"""
        return abs(hash(query.lower())) % total_shards
    
    def get_shard_for_document(self, doc_id: str, total_shards: int) -> int:
        """Get shard ID for a document using consistent hashing"""
        return abs(hash(doc_id)) % total_shards
    
    def get_healthy_services(self, service_urls: List[str]) -> List[str]:
        """Filter out unhealthy services"""
        healthy = []
        for url in service_urls:
            try:
                response = requests.get(f"{url}/health", timeout=2)
                if response.status_code == 200:
                    healthy.append(url)
            except:
                logger.warning(f"Service {url} is unhealthy")
        return healthy

service_registry = ServiceRegistry()

def cache_key(prefix: str, *args) -> str:
    """Generate cache key"""
    key_data = f"{prefix}:{':'.join(str(arg) for arg in args)}"
    return hashlib.md5(key_data.encode()).hexdigest()

def get_from_cache(key: str, ttl: int = 300):
    """Get data from Redis cache"""
    try:
        cached = redis_client.get(key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        logger.error(f"Cache get error: {e}")
    return None

def set_cache(key: str, data: Any, ttl: int = 300):
    """Set data in Redis cache"""
    try:
        redis_client.setex(key, ttl, json.dumps(data))
    except Exception as e:
        logger.error(f"Cache set error: {e}")

def make_request(url: str, method: str = 'GET', data: Dict = None, timeout: int = 10):
    """Make HTTP request with error handling"""
    try:
        if method == 'GET':
            response = requests.get(url, params=data, timeout=timeout)
        elif method == 'POST':
            response = requests.post(url, json=data, timeout=timeout)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed to {url}: {e}")
        raise

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": time.time()})

@app.route('/search', methods=['POST'])
def search():
    """Search endpoint that queries all search service shards"""
    data = request.get_json()
    query = data.get('query', '')
    size = data.get('size', 10)
    from_offset = data.get('from', 0)
    
    if not query:
        return jsonify({"error": "Query is required"}), 400
    
    # Check cache first
    cache_key_str = cache_key("search", query, size, from_offset)
    cached_result = get_from_cache(cache_key_str)
    if cached_result:
        logger.info(f"Cache hit for query: {query}")
        return jsonify(cached_result)
    
    # Get healthy search services
    healthy_services = service_registry.get_healthy_services(service_registry.search_services)
    if not healthy_services:
        return jsonify({"error": "No healthy search services available"}), 503
    
    # Query all shards in parallel
    results = []
    with ThreadPoolExecutor(max_workers=len(healthy_services)) as executor:
        future_to_service = {}
        
        for service_url in healthy_services:
            future = executor.submit(
                make_request,
                f"{service_url}/search",
                'POST',
                {"query": query, "size": size * 2, "from": 0}  # Get more results to merge
            )
            future_to_service[future] = service_url
        
        for future in as_completed(future_to_service):
            service_url = future_to_service[future]
            try:
                result = future.result()
                if result.get('hits'):
                    results.extend(result['hits'])
            except Exception as e:
                logger.error(f"Error querying {service_url}: {e}")
    
    # Merge and sort results by score
    results.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    # Apply pagination
    paginated_results = results[from_offset:from_offset + size]
    
    response = {
        "query": query,
        "total": len(results),
        "hits": paginated_results,
        "took": time.time()
    }
    
    # Cache the result
    set_cache(cache_key_str, response, ttl=300)
    
    return jsonify(response)

@app.route('/typeahead', methods=['GET'])
def typeahead():
    """Typeahead endpoint that queries typeahead service shards"""
    query = request.args.get('q', '')
    limit = int(request.args.get('limit', 10))
    
    if not query:
        return jsonify({"error": "Query parameter 'q' is required"}), 400
    
    # Check cache first
    cache_key_str = cache_key("typeahead", query, limit)
    cached_result = get_from_cache(cache_key_str, ttl=600)  # Longer cache for typeahead
    if cached_result:
        return jsonify(cached_result)
    
    # Get healthy typeahead services
    healthy_services = service_registry.get_healthy_services(service_registry.typeahead_services)
    if not healthy_services:
        return jsonify({"error": "No healthy typeahead services available"}), 503
    
    # Query all typeahead shards in parallel
    suggestions = []
    with ThreadPoolExecutor(max_workers=len(healthy_services)) as executor:
        future_to_service = {}
        
        for service_url in healthy_services:
            future = executor.submit(
                make_request,
                f"{service_url}/suggest",
                'GET',
                {"q": query, "limit": limit * 2}
            )
            future_to_service[future] = service_url
        
        for future in as_completed(future_to_service):
            service_url = future_to_service[future]
            try:
                result = future.result()
                if result.get('suggestions'):
                    suggestions.extend(result['suggestions'])
            except Exception as e:
                logger.error(f"Error querying typeahead {service_url}: {e}")
    
    # Remove duplicates and sort by frequency
    unique_suggestions = {}
    for suggestion in suggestions:
        term = suggestion['term']
        if term not in unique_suggestions:
            unique_suggestions[term] = suggestion
        else:
            # Keep the one with higher frequency
            if suggestion['frequency'] > unique_suggestions[term]['frequency']:
                unique_suggestions[term] = suggestion
    
    # Sort by frequency and limit results
    sorted_suggestions = sorted(
        unique_suggestions.values(),
        key=lambda x: x['frequency'],
        reverse=True
    )[:limit]
    
    response = {
        "query": query,
        "suggestions": sorted_suggestions
    }
    
    # Cache the result
    set_cache(cache_key_str, response, ttl=600)
    
    return jsonify(response)

@app.route('/index', methods=['POST'])
def index_document():
    """Index a document by routing to appropriate indexing service shard"""
    data = request.get_json()
    
    if not data or 'content' not in data:
        return jsonify({"error": "Document content is required"}), 400
    
    # Generate document ID if not provided
    doc_id = data.get('id', hashlib.md5(data['content'].encode()).hexdigest())
    
    # Determine which shard should handle this document
    shard_id = service_registry.get_shard_for_document(doc_id, len(service_registry.indexing_services))
    
    # Get the appropriate indexing service
    if shard_id >= len(service_registry.indexing_services):
        return jsonify({"error": "Invalid shard configuration"}), 500
    
    indexing_service_url = service_registry.indexing_services[shard_id]
    
    try:
        # Forward the request to the appropriate indexing service
        result = make_request(
            f"{indexing_service_url}/index",
            'POST',
            {**data, 'id': doc_id, 'shard_id': shard_id}
        )
        
        # Invalidate related caches
        # This is a simple approach - in production, you might want more sophisticated cache invalidation
        try:
            # Clear search caches that might be affected
            for key in redis_client.scan_iter(match="*search*"):
                redis_client.delete(key)
        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error indexing document: {e}")
        return jsonify({"error": "Failed to index document"}), 500

@app.route('/bulk-index', methods=['POST'])
def bulk_index():
    """Bulk index multiple documents"""
    data = request.get_json()
    documents = data.get('documents', [])
    
    if not documents:
        return jsonify({"error": "Documents array is required"}), 400
    
    # Group documents by shard
    shard_groups = {}
    for doc in documents:
        doc_id = doc.get('id', hashlib.md5(doc['content'].encode()).hexdigest())
        shard_id = service_registry.get_shard_for_document(doc_id, len(service_registry.indexing_services))
        
        if shard_id not in shard_groups:
            shard_groups[shard_id] = []
        shard_groups[shard_id].append({**doc, 'id': doc_id, 'shard_id': shard_id})
    
    # Send bulk requests to each shard in parallel
    results = []
    with ThreadPoolExecutor(max_workers=len(shard_groups)) as executor:
        future_to_shard = {}
        
        for shard_id, docs in shard_groups.items():
            if shard_id < len(service_registry.indexing_services):
                service_url = service_registry.indexing_services[shard_id]
                future = executor.submit(
                    make_request,
                    f"{service_url}/bulk-index",
                    'POST',
                    {"documents": docs}
                )
                future_to_shard[future] = shard_id
        
        for future in as_completed(future_to_shard):
            shard_id = future_to_shard[future]
            try:
                result = future.result()
                results.append({"shard_id": shard_id, "result": result})
            except Exception as e:
                logger.error(f"Error bulk indexing shard {shard_id}: {e}")
                results.append({"shard_id": shard_id, "error": str(e)})
    
    # Clear caches
    try:
        for key in redis_client.scan_iter(match="*search*"):
            redis_client.delete(key)
    except Exception as e:
        logger.error(f"Cache invalidation error: {e}")
    
    return jsonify({"results": results})

@app.route('/stats')
def stats():
    """Get system statistics"""
    stats = {
        "search_services": len(service_registry.search_services),
        "typeahead_services": len(service_registry.typeahead_services),
        "indexing_services": len(service_registry.indexing_services),
        "healthy_search_services": len(service_registry.get_healthy_services(service_registry.search_services)),
        "healthy_typeahead_services": len(service_registry.get_healthy_services(service_registry.typeahead_services)),
        "healthy_indexing_services": len(service_registry.get_healthy_services(service_registry.indexing_services)),
        "cache_info": {
            "redis_connected": redis_client.ping() if redis_client else False
        }
    }
    return jsonify(stats)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False) 