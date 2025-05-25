import os
import json
import pickle
import math
import time
import logging
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Any
import psycopg2
import redis
from flask import Flask, request, jsonify
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

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
SERVICE_PORT = int(os.getenv('SERVICE_PORT', 8001))
SHARD_ID = int(os.getenv('SHARD_ID', 0))
TOTAL_SHARDS = int(os.getenv('TOTAL_SHARDS', 3))
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/searchdb')
INDEX_PATH = os.getenv('INDEX_PATH', f'/data/indexes/shard-{SHARD_ID}')

# Initialize components
redis_client = redis.from_url(REDIS_URL)
stemmer = PorterStemmer()
stop_words = set(stopwords.words('english'))

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

class InvertedIndex:
    def __init__(self, index_path: str):
        self.index_path = index_path
        self.index_file = os.path.join(index_path, 'inverted_index.pkl')
        self.doc_freq_file = os.path.join(index_path, 'doc_frequencies.pkl')
        self.doc_lengths_file = os.path.join(index_path, 'doc_lengths.pkl')
        
        # Load existing index or create new one
        self.inverted_index = self._load_index()
        self.doc_frequencies = self._load_doc_frequencies()
        self.doc_lengths = self._load_doc_lengths()
        self.total_docs = len(self.doc_lengths)
    
    def _load_index(self) -> Dict[str, Dict[str, int]]:
        """Load inverted index from disk"""
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading index: {e}")
        return defaultdict(lambda: defaultdict(int))
    
    def _load_doc_frequencies(self) -> Dict[str, int]:
        """Load document frequencies from disk"""
        try:
            if os.path.exists(self.doc_freq_file):
                with open(self.doc_freq_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading doc frequencies: {e}")
        return defaultdict(int)
    
    def _load_doc_lengths(self) -> Dict[str, int]:
        """Load document lengths from disk"""
        try:
            if os.path.exists(self.doc_lengths_file):
                with open(self.doc_lengths_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading doc lengths: {e}")
        return {}
    
    def save_index(self):
        """Save index to disk"""
        try:
            with open(self.index_file, 'wb') as f:
                pickle.dump(dict(self.inverted_index), f)
            
            with open(self.doc_freq_file, 'wb') as f:
                pickle.dump(dict(self.doc_frequencies), f)
            
            with open(self.doc_lengths_file, 'wb') as f:
                pickle.dump(self.doc_lengths, f)
                
            logger.info(f"Index saved for shard {SHARD_ID}")
        except Exception as e:
            logger.error(f"Error saving index: {e}")
    
    def add_document(self, doc_id: str, tokens: List[str]):
        """Add document to inverted index"""
        # Count term frequencies in document
        term_counts = Counter(tokens)
        doc_length = len(tokens)
        
        # Update inverted index
        for term, count in term_counts.items():
            self.inverted_index[term][doc_id] = count
            
        # Update document frequencies (number of documents containing each term)
        for term in set(tokens):
            self.doc_frequencies[term] += 1
        
        # Store document length
        self.doc_lengths[doc_id] = doc_length
        self.total_docs = len(self.doc_lengths)
    
    def remove_document(self, doc_id: str):
        """Remove document from inverted index"""
        if doc_id not in self.doc_lengths:
            return
        
        # Find all terms in the document and update frequencies
        terms_to_remove = []
        for term, doc_dict in self.inverted_index.items():
            if doc_id in doc_dict:
                del doc_dict[doc_id]
                self.doc_frequencies[term] -= 1
                
                # Remove term if no documents contain it
                if self.doc_frequencies[term] <= 0:
                    terms_to_remove.append(term)
        
        # Clean up empty terms
        for term in terms_to_remove:
            del self.inverted_index[term]
            del self.doc_frequencies[term]
        
        # Remove document length
        del self.doc_lengths[doc_id]
        self.total_docs = len(self.doc_lengths)
    
    def search(self, query_tokens: List[str], limit: int = 10) -> List[Tuple[str, float]]:
        """Search using TF-IDF scoring"""
        if not query_tokens:
            return []
        
        # Get candidate documents (documents containing at least one query term)
        candidate_docs = set()
        for token in query_tokens:
            if token in self.inverted_index:
                candidate_docs.update(self.inverted_index[token].keys())
        
        if not candidate_docs:
            return []
        
        # Calculate TF-IDF scores
        scores = {}
        query_term_counts = Counter(query_tokens)
        
        for doc_id in candidate_docs:
            score = 0.0
            doc_length = self.doc_lengths.get(doc_id, 1)
            
            for term, query_tf in query_term_counts.items():
                if term in self.inverted_index and doc_id in self.inverted_index[term]:
                    # Term frequency in document
                    doc_tf = self.inverted_index[term][doc_id]
                    
                    # Document frequency (number of docs containing term)
                    df = self.doc_frequencies.get(term, 1)
                    
                    # TF-IDF calculation
                    tf = doc_tf / doc_length  # Normalized term frequency
                    idf = math.log(self.total_docs / df)  # Inverse document frequency
                    
                    score += tf * idf * query_tf
            
            scores[doc_id] = score
        
        # Sort by score and return top results
        sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_results[:limit]

class SearchService:
    def __init__(self):
        self.text_processor = TextProcessor()
        self.inverted_index = InvertedIndex(INDEX_PATH)
        self.db_connection = None
        self._connect_db()
    
    def _connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_connection = psycopg2.connect(POSTGRES_URL)
            logger.info(f"Connected to database for shard {SHARD_ID}")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
    
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
    
    def search_documents(self, query: str, size: int = 10, from_offset: int = 0) -> Dict[str, Any]:
        """Search documents using inverted index and PostgreSQL"""
        start_time = time.time()
        
        # Preprocess query
        query_tokens = self.text_processor.preprocess_text(query)
        if not query_tokens:
            return {"hits": [], "total": 0, "took": time.time() - start_time}
        
        # Search using inverted index
        search_results = self.inverted_index.search(query_tokens, limit=size + from_offset + 50)
        
        if not search_results:
            return {"hits": [], "total": 0, "took": time.time() - start_time}
        
        # Get document details from PostgreSQL
        doc_ids = [doc_id for doc_id, score in search_results]
        
        try:
            cursor = self._get_db_cursor()
            
            # Build query with placeholders
            placeholders = ','.join(['%s'] * len(doc_ids))
            query_sql = f"""
                SELECT id, title, content, source, metadata, created_at
                FROM documents 
                WHERE id = ANY(%s) AND shard_id = %s
                ORDER BY created_at DESC
            """
            
            cursor.execute(query_sql, (doc_ids, SHARD_ID))
            db_results = cursor.fetchall()
            
            # Create document lookup
            doc_lookup = {}
            for row in db_results:
                doc_lookup[str(row[0])] = {
                    "id": str(row[0]),
                    "title": row[1],
                    "content": row[2][:500] + "..." if len(row[2]) > 500 else row[2],  # Truncate content
                    "source": row[3],
                    "metadata": row[4],
                    "created_at": row[5].isoformat() if row[5] else None
                }
            
            # Combine search results with document data
            hits = []
            for doc_id, score in search_results:
                if doc_id in doc_lookup:
                    hit = doc_lookup[doc_id]
                    hit["score"] = score
                    hits.append(hit)
            
            # Apply pagination
            paginated_hits = hits[from_offset:from_offset + size]
            
            return {
                "hits": paginated_hits,
                "total": len(hits),
                "took": time.time() - start_time,
                "shard_id": SHARD_ID
            }
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return {"hits": [], "total": 0, "took": time.time() - start_time, "error": str(e)}
    
    def get_document_by_id(self, doc_id: str) -> Dict[str, Any]:
        """Get document by ID"""
        try:
            cursor = self._get_db_cursor()
            cursor.execute(
                "SELECT id, title, content, source, metadata, created_at FROM documents WHERE id = %s AND shard_id = %s",
                (doc_id, SHARD_ID)
            )
            result = cursor.fetchone()
            
            if result:
                return {
                    "id": str(result[0]),
                    "title": result[1],
                    "content": result[2],
                    "source": result[3],
                    "metadata": result[4],
                    "created_at": result[5].isoformat() if result[5] else None
                }
            return None
            
        except Exception as e:
            logger.error(f"Get document error: {e}")
            return None

# Initialize search service
search_service = SearchService()

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "shard_id": SHARD_ID,
        "total_shards": TOTAL_SHARDS,
        "documents_indexed": search_service.inverted_index.total_docs,
        "timestamp": time.time()
    })

@app.route('/search', methods=['POST'])
def search():
    """Search endpoint"""
    data = request.get_json()
    query = data.get('query', '')
    size = data.get('size', 10)
    from_offset = data.get('from', 0)
    
    if not query:
        return jsonify({"error": "Query is required"}), 400
    
    result = search_service.search_documents(query, size, from_offset)
    return jsonify(result)

@app.route('/document/<doc_id>')
def get_document(doc_id):
    """Get document by ID"""
    document = search_service.get_document_by_id(doc_id)
    if document:
        return jsonify(document)
    return jsonify({"error": "Document not found"}), 404

@app.route('/stats')
def stats():
    """Get shard statistics"""
    try:
        cursor = search_service._get_db_cursor()
        cursor.execute("SELECT COUNT(*) FROM documents WHERE shard_id = %s", (SHARD_ID,))
        db_count = cursor.fetchone()[0]
        
        return jsonify({
            "shard_id": SHARD_ID,
            "total_shards": TOTAL_SHARDS,
            "documents_in_db": db_count,
            "documents_in_index": search_service.inverted_index.total_docs,
            "unique_terms": len(search_service.inverted_index.inverted_index),
            "index_path": INDEX_PATH
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/rebuild-index', methods=['POST'])
def rebuild_index():
    """Rebuild inverted index from database"""
    try:
        cursor = search_service._get_db_cursor()
        cursor.execute("SELECT id, title, content FROM documents WHERE shard_id = %s", (SHARD_ID,))
        
        # Clear existing index
        search_service.inverted_index = InvertedIndex(INDEX_PATH)
        
        # Rebuild index
        count = 0
        for row in cursor.fetchall():
            doc_id, title, content = str(row[0]), row[1] or "", row[2] or ""
            full_text = f"{title} {content}"
            tokens = search_service.text_processor.preprocess_text(full_text)
            search_service.inverted_index.add_document(doc_id, tokens)
            count += 1
        
        # Save index
        search_service.inverted_index.save_index()
        
        return jsonify({
            "message": "Index rebuilt successfully",
            "documents_processed": count,
            "shard_id": SHARD_ID
        })
        
    except Exception as e:
        logger.error(f"Index rebuild error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=SERVICE_PORT, debug=False) 