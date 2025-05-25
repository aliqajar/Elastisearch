import os
import json
import time
import logging
import hashlib
from typing import Dict, List, Any
import redis
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import threading
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
SERVICE_PORT = int(os.getenv('SERVICE_PORT', 8030))
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

# Initialize components
redis_client = redis.from_url(REDIS_URL)

class DataIngestionService:
    def __init__(self):
        self.kafka_producer = None
        self.stats = {
            'documents_received': 0,
            'documents_processed': 0,
            'documents_failed': 0,
            'bytes_processed': 0
        }
        self.stats_lock = threading.Lock()
        self._setup_kafka()
    
    def _setup_kafka(self):
        """Setup Kafka producer"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                batch_size=16384,  # Batch size for better throughput
                linger_ms=10,      # Wait up to 10ms to batch messages
                compression_type='gzip',  # Compress messages
                retries=3,
                acks='all'  # Wait for all replicas to acknowledge
            )
            logger.info("Connected to Kafka for data ingestion")
        except Exception as e:
            logger.error(f"Kafka connection error: {e}")
    
    def _update_stats(self, documents_received=0, documents_processed=0, documents_failed=0, bytes_processed=0):
        """Update service statistics"""
        with self.stats_lock:
            self.stats['documents_received'] += documents_received
            self.stats['documents_processed'] += documents_processed
            self.stats['documents_failed'] += documents_failed
            self.stats['bytes_processed'] += bytes_processed
    
    def _get_shard_for_document(self, doc_id: str, total_shards: int = 3) -> int:
        """Get shard ID for a document using consistent hashing"""
        return abs(hash(doc_id)) % total_shards
    
    def _validate_document(self, doc: Dict[str, Any]) -> tuple[bool, str]:
        """Validate document structure and content"""
        # Check required fields
        if 'content' not in doc:
            return False, "Missing required field: content"
        
        if not doc['content'] or not isinstance(doc['content'], str):
            return False, "Content must be a non-empty string"
        
        # Check content length (prevent extremely large documents)
        if len(doc['content']) > 10 * 1024 * 1024:  # 10MB limit
            return False, "Content exceeds maximum size limit (10MB)"
        
        # Validate optional fields
        if 'title' in doc and not isinstance(doc['title'], str):
            return False, "Title must be a string"
        
        if 'source' in doc and not isinstance(doc['source'], str):
            return False, "Source must be a string"
        
        if 'metadata' in doc and not isinstance(doc['metadata'], dict):
            return False, "Metadata must be a dictionary"
        
        return True, ""
    
    def _enrich_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich document with additional metadata"""
        # Generate ID if not provided
        if 'id' not in doc or not doc['id']:
            doc['id'] = hashlib.md5(doc['content'].encode()).hexdigest()
        
        # Add ingestion timestamp
        if 'metadata' not in doc:
            doc['metadata'] = {}
        
        doc['metadata']['ingested_at'] = time.time()
        doc['metadata']['ingestion_service'] = 'data-ingestion'
        
        # Add content statistics
        doc['metadata']['content_length'] = len(doc['content'])
        doc['metadata']['word_count'] = len(doc['content'].split())
        
        return doc
    
    def _send_to_kafka(self, topic: str, doc: Dict[str, Any], shard_id: int) -> bool:
        """Send document to Kafka topic"""
        try:
            if not self.kafka_producer:
                logger.error("Kafka producer not available")
                return False
            
            # Use document ID as key for partitioning
            key = f"shard-{shard_id}"
            
            # Send message
            future = self.kafka_producer.send(
                topic,
                value=doc,
                key=key
            )
            
            # Wait for confirmation (with timeout)
            future.get(timeout=10)
            return True
            
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
            return False
    
    def ingest_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest a single document"""
        start_time = time.time()
        
        try:
            # Update stats
            self._update_stats(documents_received=1)
            
            # Validate document
            is_valid, error_msg = self._validate_document(doc)
            if not is_valid:
                self._update_stats(documents_failed=1)
                return {
                    "status": "failed",
                    "error": error_msg,
                    "processing_time": time.time() - start_time
                }
            
            # Enrich document
            enriched_doc = self._enrich_document(doc.copy())
            
            # Determine shard
            shard_id = self._get_shard_for_document(enriched_doc['id'])
            enriched_doc['shard_id'] = shard_id
            
            # Send to Kafka
            success = self._send_to_kafka('document_indexing', enriched_doc, shard_id)
            
            if success:
                self._update_stats(
                    documents_processed=1,
                    bytes_processed=len(json.dumps(enriched_doc).encode())
                )
                
                return {
                    "status": "accepted",
                    "document_id": enriched_doc['id'],
                    "shard_id": shard_id,
                    "processing_time": time.time() - start_time
                }
            else:
                self._update_stats(documents_failed=1)
                return {
                    "status": "failed",
                    "error": "Failed to send to processing queue",
                    "processing_time": time.time() - start_time
                }
                
        except Exception as e:
            self._update_stats(documents_failed=1)
            logger.error(f"Error ingesting document: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "processing_time": time.time() - start_time
            }
    
    def bulk_ingest_documents(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Ingest multiple documents in parallel"""
        start_time = time.time()
        
        if not documents:
            return {
                "status": "failed",
                "error": "No documents provided",
                "processing_time": time.time() - start_time
            }
        
        # Limit batch size to prevent memory issues
        if len(documents) > 1000:
            return {
                "status": "failed",
                "error": "Batch size exceeds maximum limit (1000 documents)",
                "processing_time": time.time() - start_time
            }
        
        results = []
        successful = 0
        failed = 0
        
        # Process documents in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.ingest_document, doc) for doc in documents]
            
            for future in futures:
                try:
                    result = future.result(timeout=30)  # 30 second timeout per document
                    results.append(result)
                    
                    if result['status'] == 'accepted':
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    failed += 1
                    results.append({
                        "status": "failed",
                        "error": f"Processing timeout or error: {str(e)}"
                    })
        
        return {
            "status": "completed",
            "total_documents": len(documents),
            "successful": successful,
            "failed": failed,
            "results": results,
            "processing_time": time.time() - start_time
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get ingestion statistics"""
        with self.stats_lock:
            stats = self.stats.copy()
        
        # Add additional metrics
        stats['kafka_connected'] = self.kafka_producer is not None
        stats['redis_connected'] = redis_client.ping() if redis_client else False
        
        return stats

# Initialize ingestion service
ingestion_service = DataIngestionService()

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "kafka_connected": ingestion_service.kafka_producer is not None,
        "timestamp": time.time()
    })

@app.route('/ingest', methods=['POST'])
def ingest_document():
    """Ingest a single document"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        result = ingestion_service.ingest_document(data)
        
        if result['status'] == 'failed':
            return jsonify(result), 400
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in ingest endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/bulk-ingest', methods=['POST'])
def bulk_ingest():
    """Bulk ingest multiple documents"""
    try:
        data = request.get_json()
        
        if not data or 'documents' not in data:
            return jsonify({"error": "Documents array is required"}), 400
        
        documents = data['documents']
        
        if not isinstance(documents, list):
            return jsonify({"error": "Documents must be an array"}), 400
        
        result = ingestion_service.bulk_ingest_documents(documents)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in bulk ingest endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/stats')
def stats():
    """Get ingestion statistics"""
    try:
        stats = ingestion_service.get_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/validate', methods=['POST'])
def validate_document():
    """Validate a document without ingesting it"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        is_valid, error_msg = ingestion_service._validate_document(data)
        
        if is_valid:
            # Show what the enriched document would look like
            enriched = ingestion_service._enrich_document(data.copy())
            shard_id = ingestion_service._get_shard_for_document(enriched['id'])
            
            return jsonify({
                "valid": True,
                "document_id": enriched['id'],
                "shard_id": shard_id,
                "enriched_metadata": enriched['metadata']
            })
        else:
            return jsonify({
                "valid": False,
                "error": error_msg
            }), 400
            
    except Exception as e:
        logger.error(f"Error in validate endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/reset-stats', methods=['POST'])
def reset_stats():
    """Reset ingestion statistics"""
    try:
        with ingestion_service.stats_lock:
            ingestion_service.stats = {
                'documents_received': 0,
                'documents_processed': 0,
                'documents_failed': 0,
                'bytes_processed': 0
            }
        
        return jsonify({"message": "Statistics reset successfully"})
        
    except Exception as e:
        logger.error(f"Error resetting stats: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=SERVICE_PORT, debug=False) 