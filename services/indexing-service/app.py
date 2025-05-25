import os
import json
import pickle
import time
import logging
import hashlib
import threading
from collections import Counter
from typing import Dict, List, Any
import psycopg2
import redis
import requests
from flask import Flask, request, jsonify
from kafka import KafkaConsumer, KafkaProducer
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
SERVICE_PORT = int(os.getenv('SERVICE_PORT', 8021))
SHARD_ID = int(os.getenv('SHARD_ID', 0))
TOTAL_SHARDS = int(os.getenv('TOTAL_SHARDS', 3))
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/searchdb')
INDEX_PATH = os.getenv('INDEX_PATH', f'/data/indexes/shard-{SHARD_ID}')
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')

# Initialize components
redis_client = redis.from_url(REDIS_URL)

# Ensure index directory exists
os.makedirs(INDEX_PATH, exist_ok=True)

class TextProcessor:
    def __init__(self):
        self.stemmer = PorterStemmer()
        self.stop_words = set(stopwords.words('english'))
    
    def preprocess_text(self, text: str) -> List[str]:
        """Preprocess text: tokenize, lowercase, remove stopwords, stem"""
        if not text:
            return []
        
        # Tokenize and lowercase
        tokens = word_tokenize(text.lower())
        
        # Remove non-alphabetic tokens and stopwords
        tokens = [token for token in tokens if token.isalpha() and token not in self.stop_words]
        
        # Stem tokens
        tokens = [self.stemmer.stem(token) for token in tokens]
        
        return tokens
    
    def extract_terms_for_typeahead(self, text: str) -> List[str]:
        """Extract terms for typeahead (less aggressive processing)"""
        if not text:
            return []
        
        # Tokenize and lowercase
        tokens = word_tokenize(text.lower())
        
        # Remove non-alphabetic tokens and very short tokens
        tokens = [token for token in tokens if token.isalpha() and len(token) >= 2]
        
        # Remove stopwords but don't stem for typeahead
        tokens = [token for token in tokens if token not in self.stop_words]
        
        return tokens

class InvertedIndexUpdater:
    def __init__(self, index_path: str):
        self.index_path = index_path
        self.index_file = os.path.join(index_path, 'inverted_index.pkl')
        self.doc_freq_file = os.path.join(index_path, 'doc_frequencies.pkl')
        self.doc_lengths_file = os.path.join(index_path, 'doc_lengths.pkl')
        self.lock = threading.Lock()
    
    def update_document(self, doc_id: str, tokens: List[str], remove_first: bool = False):
        """Update inverted index with document"""
        with self.lock:
            # Load existing index
            inverted_index = self._load_index()
            doc_frequencies = self._load_doc_frequencies()
            doc_lengths = self._load_doc_lengths()
            
            # Remove existing document if updating
            if remove_first and doc_id in doc_lengths:
                self._remove_document_from_index(doc_id, inverted_index, doc_frequencies, doc_lengths)
            
            # Add new document
            if tokens:
                self._add_document_to_index(doc_id, tokens, inverted_index, doc_frequencies, doc_lengths)
            
            # Save updated index
            self._save_index(inverted_index, doc_frequencies, doc_lengths)
    
    def remove_document(self, doc_id: str):
        """Remove document from inverted index"""
        with self.lock:
            inverted_index = self._load_index()
            doc_frequencies = self._load_doc_frequencies()
            doc_lengths = self._load_doc_lengths()
            
            if doc_id in doc_lengths:
                self._remove_document_from_index(doc_id, inverted_index, doc_frequencies, doc_lengths)
                self._save_index(inverted_index, doc_frequencies, doc_lengths)
    
    def _load_index(self):
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading index: {e}")
        return {}
    
    def _load_doc_frequencies(self):
        try:
            if os.path.exists(self.doc_freq_file):
                with open(self.doc_freq_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading doc frequencies: {e}")
        return {}
    
    def _load_doc_lengths(self):
        try:
            if os.path.exists(self.doc_lengths_file):
                with open(self.doc_lengths_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading doc lengths: {e}")
        return {}
    
    def _save_index(self, inverted_index, doc_frequencies, doc_lengths):
        try:
            with open(self.index_file, 'wb') as f:
                pickle.dump(inverted_index, f)
            
            with open(self.doc_freq_file, 'wb') as f:
                pickle.dump(doc_frequencies, f)
            
            with open(self.doc_lengths_file, 'wb') as f:
                pickle.dump(doc_lengths, f)
                
        except Exception as e:
            logger.error(f"Error saving index: {e}")
    
    def _add_document_to_index(self, doc_id, tokens, inverted_index, doc_frequencies, doc_lengths):
        term_counts = Counter(tokens)
        doc_length = len(tokens)
        
        # Update inverted index
        for term, count in term_counts.items():
            if term not in inverted_index:
                inverted_index[term] = {}
            inverted_index[term][doc_id] = count
        
        # Update document frequencies
        for term in set(tokens):
            doc_frequencies[term] = doc_frequencies.get(term, 0) + 1
        
        # Store document length
        doc_lengths[doc_id] = doc_length
    
    def _remove_document_from_index(self, doc_id, inverted_index, doc_frequencies, doc_lengths):
        # Find all terms in the document and update frequencies
        terms_to_remove = []
        for term, doc_dict in inverted_index.items():
            if doc_id in doc_dict:
                del doc_dict[doc_id]
                doc_frequencies[term] -= 1
                
                # Remove term if no documents contain it
                if doc_frequencies[term] <= 0:
                    terms_to_remove.append(term)
        
        # Clean up empty terms
        for term in terms_to_remove:
            del inverted_index[term]
            del doc_frequencies[term]
        
        # Remove document length
        if doc_id in doc_lengths:
            del doc_lengths[doc_id]

class IndexingService:
    def __init__(self):
        self.text_processor = TextProcessor()
        self.index_updater = InvertedIndexUpdater(INDEX_PATH)
        self.db_connection = None
        self.kafka_producer = None
        self._connect_db()
        self._setup_kafka()
    
    def _connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_connection = psycopg2.connect(POSTGRES_URL)
            self.db_connection.autocommit = False
            logger.info(f"Connected to database for indexing shard {SHARD_ID}")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
    
    def _setup_kafka(self):
        """Setup Kafka producer"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Connected to Kafka for shard {SHARD_ID}")
        except Exception as e:
            logger.error(f"Kafka connection error: {e}")
    
    def _get_db_cursor(self):
        """Get database cursor with connection retry"""
        try:
            if self.db_connection.closed:
                self._connect_db()
            return self.db_connection.cursor()
        except Exception as e:
            logger.error(f"Database cursor error: {e}")
            self._connect_db()
            return self.db_connection.cursor()
    
    def _get_shard_for_term(self, term: str, total_typeahead_shards: int = 2) -> int:
        """Get shard ID for a term (for typeahead service)"""
        return abs(hash(term.lower())) % total_typeahead_shards
    
    def _update_typeahead_terms(self, terms: List[str]):
        """Update typeahead service with new terms (Redis-first approach)"""
        if not terms:
            return
        
        # Group terms by typeahead shard and count frequencies
        shard_terms = {}
        for term in terms:
            shard_id = self._get_shard_for_term(term)
            if shard_id not in shard_terms:
                shard_terms[shard_id] = Counter()
            shard_terms[shard_id][term] += 1
        
        # Update Redis directly for each shard (no locks, atomic operations)
        for shard_id, term_counts in shard_terms.items():
            try:
                # Use Redis atomic operations instead of database writes
                redis_key_prefix = f"terms:shard:{shard_id}"
                
                # Batch update Redis using pipeline for better performance
                pipe = redis_client.pipeline()
                for term, count in term_counts.items():
                    redis_key = f"{redis_key_prefix}:{term.lower().strip()}"
                    pipe.incr(redis_key, count)
                    pipe.expire(redis_key, 86400)  # 24 hour expiration
                
                # Execute all Redis operations atomically
                pipe.execute()
                
                logger.debug(f"Updated {len(term_counts)} terms in Redis for shard {shard_id}")
                
            except Exception as e:
                logger.error(f"Error updating Redis terms for shard {shard_id}: {e}")
                
                # Fallback to Kafka if Redis fails
                try:
                    if self.kafka_producer:
                        message = {
                            'action': 'update_terms',
                            'shard_id': shard_id,
                            'terms': dict(term_counts)
                        }
                        self.kafka_producer.send('typeahead_updates', value=message)
                        logger.info(f"Sent terms to Kafka as fallback for shard {shard_id}")
                except Exception as kafka_error:
                    logger.error(f"Both Redis and Kafka failed for shard {shard_id}: {kafka_error}")
    
    def index_document(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Index a single document"""
        try:
            doc_id = doc_data.get('id')
            content = doc_data.get('content', '')
            title = doc_data.get('title', '')
            source = doc_data.get('source', '')
            metadata = doc_data.get('metadata', {})
            shard_id = doc_data.get('shard_id', SHARD_ID)
            
            if not doc_id:
                doc_id = hashlib.md5(content.encode()).hexdigest()
            
            # Validate shard assignment
            if shard_id != SHARD_ID:
                return {"error": f"Document assigned to wrong shard. Expected {SHARD_ID}, got {shard_id}"}
            
            # Process text
            full_text = f"{title} {content}"
            search_tokens = self.text_processor.preprocess_text(full_text)
            typeahead_terms = self.text_processor.extract_terms_for_typeahead(full_text)
            
            # Start database transaction
            cursor = self._get_db_cursor()
            
            try:
                # Check if document already exists
                cursor.execute(
                    "SELECT id FROM documents WHERE id = %s AND shard_id = %s",
                    (doc_id, shard_id)
                )
                exists = cursor.fetchone() is not None
                
                if exists:
                    # Update existing document
                    cursor.execute("""
                        UPDATE documents 
                        SET content = %s, title = %s, source = %s, metadata = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND shard_id = %s
                    """, (content, title, source, json.dumps(metadata), doc_id, shard_id))
                    
                    # Update inverted index (remove old, add new)
                    self.index_updater.update_document(doc_id, search_tokens, remove_first=True)
                    
                else:
                    # Insert new document
                    cursor.execute("""
                        INSERT INTO documents (id, content, title, source, metadata, shard_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (doc_id, content, title, source, json.dumps(metadata), shard_id))
                    
                    # Update inverted index
                    self.index_updater.update_document(doc_id, search_tokens)
                
                # Commit database transaction
                self.db_connection.commit()
                
                # Update typeahead terms asynchronously
                if typeahead_terms:
                    threading.Thread(
                        target=self._update_typeahead_terms,
                        args=(typeahead_terms,)
                    ).start()
                
                return {
                    "id": doc_id,
                    "status": "updated" if exists else "indexed",
                    "shard_id": shard_id,
                    "tokens_processed": len(search_tokens),
                    "typeahead_terms": len(typeahead_terms)
                }
                
            except Exception as e:
                self.db_connection.rollback()
                raise e
                
        except Exception as e:
            logger.error(f"Error indexing document: {e}")
            return {"error": str(e)}
    
    def bulk_index_documents(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Index multiple documents in batch"""
        results = []
        successful = 0
        failed = 0
        
        for doc in documents:
            result = self.index_document(doc)
            results.append(result)
            
            if "error" in result:
                failed += 1
            else:
                successful += 1
        
        return {
            "total": len(documents),
            "successful": successful,
            "failed": failed,
            "results": results,
            "shard_id": SHARD_ID
        }
    
    def delete_document(self, doc_id: str) -> Dict[str, Any]:
        """Delete a document"""
        try:
            cursor = self._get_db_cursor()
            
            # Check if document exists
            cursor.execute(
                "SELECT id FROM documents WHERE id = %s AND shard_id = %s",
                (doc_id, SHARD_ID)
            )
            
            if not cursor.fetchone():
                return {"error": "Document not found"}
            
            # Delete from database
            cursor.execute(
                "DELETE FROM documents WHERE id = %s AND shard_id = %s",
                (doc_id, SHARD_ID)
            )
            
            # Remove from inverted index
            self.index_updater.remove_document(doc_id)
            
            self.db_connection.commit()
            
            return {
                "id": doc_id,
                "status": "deleted",
                "shard_id": SHARD_ID
            }
            
        except Exception as e:
            self.db_connection.rollback()
            logger.error(f"Error deleting document: {e}")
            return {"error": str(e)}

# Initialize indexing service
indexing_service = IndexingService()

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "shard_id": SHARD_ID,
        "total_shards": TOTAL_SHARDS,
        "timestamp": time.time()
    })

@app.route('/index', methods=['POST'])
def index_document():
    """Index a single document"""
    data = request.get_json()
    
    if not data or 'content' not in data:
        return jsonify({"error": "Document content is required"}), 400
    
    result = indexing_service.index_document(data)
    
    if "error" in result:
        return jsonify(result), 500
    
    return jsonify(result)

@app.route('/bulk-index', methods=['POST'])
def bulk_index():
    """Bulk index multiple documents"""
    data = request.get_json()
    documents = data.get('documents', [])
    
    if not documents:
        return jsonify({"error": "Documents array is required"}), 400
    
    result = indexing_service.bulk_index_documents(documents)
    return jsonify(result)

@app.route('/delete/<doc_id>', methods=['DELETE'])
def delete_document(doc_id):
    """Delete a document"""
    result = indexing_service.delete_document(doc_id)
    
    if "error" in result:
        return jsonify(result), 404 if "not found" in result["error"] else 500
    
    return jsonify(result)

@app.route('/stats')
def stats():
    """Get indexing statistics"""
    try:
        cursor = indexing_service._get_db_cursor()
        cursor.execute("SELECT COUNT(*) FROM documents WHERE shard_id = %s", (SHARD_ID,))
        doc_count = cursor.fetchone()[0]
        
        # Get index file sizes
        index_size = 0
        try:
            for filename in ['inverted_index.pkl', 'doc_frequencies.pkl', 'doc_lengths.pkl']:
                filepath = os.path.join(INDEX_PATH, filename)
                if os.path.exists(filepath):
                    index_size += os.path.getsize(filepath)
        except Exception as e:
            logger.error(f"Error calculating index size: {e}")
        
        return jsonify({
            "shard_id": SHARD_ID,
            "total_shards": TOTAL_SHARDS,
            "documents_indexed": doc_count,
            "index_size_bytes": index_size,
            "index_path": INDEX_PATH
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/reindex', methods=['POST'])
def reindex():
    """Reindex all documents in this shard"""
    try:
        cursor = indexing_service._get_db_cursor()
        cursor.execute("SELECT id, title, content FROM documents WHERE shard_id = %s", (SHARD_ID,))
        
        count = 0
        for row in cursor.fetchall():
            doc_id, title, content = str(row[0]), row[1] or "", row[2] or ""
            full_text = f"{title} {content}"
            tokens = indexing_service.text_processor.preprocess_text(full_text)
            indexing_service.index_updater.update_document(doc_id, tokens, remove_first=True)
            count += 1
        
        return jsonify({
            "message": "Reindexing completed",
            "documents_processed": count,
            "shard_id": SHARD_ID
        })
        
    except Exception as e:
        logger.error(f"Reindexing error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=SERVICE_PORT, debug=False) 