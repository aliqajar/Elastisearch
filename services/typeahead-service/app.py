import os
import json
import pickle
import time
import logging
import threading
from typing import Dict, List, Tuple, Any
import psycopg2
import redis
from flask import Flask, request, jsonify
import pygtrie

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
SERVICE_PORT = int(os.getenv('SERVICE_PORT', 8011))
SHARD_ID = int(os.getenv('SHARD_ID', 0))
TOTAL_SHARDS = int(os.getenv('TOTAL_SHARDS', 2))
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/searchdb')
TRIE_PATH = os.getenv('TRIE_PATH', f'/data/tries/shard-{SHARD_ID}')

# Initialize components
redis_client = redis.from_url(REDIS_URL)

# Ensure trie directory exists
os.makedirs(TRIE_PATH, exist_ok=True)

class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.frequency = 0
        self.term = None

class PrefixTrie:
    def __init__(self, trie_path: str):
        self.trie_path = trie_path
        self.trie_file = os.path.join(trie_path, 'prefix_trie.pkl')
        self.root = TrieNode()
        self.term_frequencies = {}
        self._load_trie()
    
    def _load_trie(self):
        """Load trie from disk"""
        try:
            if os.path.exists(self.trie_file):
                with open(self.trie_file, 'rb') as f:
                    data = pickle.load(f)
                    self.root = data.get('root', TrieNode())
                    self.term_frequencies = data.get('frequencies', {})
                logger.info(f"Trie loaded for shard {SHARD_ID}")
        except Exception as e:
            logger.error(f"Error loading trie: {e}")
            self.root = TrieNode()
            self.term_frequencies = {}
    
    def save_trie(self):
        """Save trie to disk"""
        try:
            data = {
                'root': self.root,
                'frequencies': self.term_frequencies
            }
            with open(self.trie_file, 'wb') as f:
                pickle.dump(data, f)
            logger.info(f"Trie saved for shard {SHARD_ID}")
        except Exception as e:
            logger.error(f"Error saving trie: {e}")
    
    def insert(self, term: str, frequency: int = 1):
        """Insert a term into the trie with frequency"""
        if not term:
            return
        
        term = term.lower().strip()
        if not term:
            return
        
        # Update frequency tracking
        if term in self.term_frequencies:
            self.term_frequencies[term] += frequency
        else:
            self.term_frequencies[term] = frequency
        
        # Insert into trie
        node = self.root
        for char in term:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        
        node.is_end_of_word = True
        node.term = term
        node.frequency = self.term_frequencies[term]
    
    def remove(self, term: str):
        """Remove a term from the trie"""
        if not term:
            return
        
        term = term.lower().strip()
        if term not in self.term_frequencies:
            return
        
        # Remove from frequency tracking
        del self.term_frequencies[term]
        
        # Mark as not end of word in trie
        node = self.root
        for char in term:
            if char not in node.children:
                return
            node = node.children[char]
        
        node.is_end_of_word = False
        node.term = None
        node.frequency = 0
    
    def search_prefix(self, prefix: str, limit: int = 10) -> List[Tuple[str, int]]:
        """Search for terms with given prefix"""
        if not prefix:
            return []
        
        prefix = prefix.lower().strip()
        if not prefix:
            return []
        
        # Navigate to prefix node
        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]
        
        # Collect all terms with this prefix
        suggestions = []
        self._collect_terms(node, prefix, suggestions)
        
        # Sort by frequency (descending) and limit results
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return suggestions[:limit]
    
    def _collect_terms(self, node: TrieNode, current_prefix: str, suggestions: List[Tuple[str, int]]):
        """Recursively collect all terms from a node"""
        if node.is_end_of_word and node.term:
            suggestions.append((node.term, node.frequency))
        
        for char, child_node in node.children.items():
            self._collect_terms(child_node, current_prefix + char, suggestions)
    
    def get_popular_terms(self, limit: int = 20) -> List[Tuple[str, int]]:
        """Get most popular terms"""
        sorted_terms = sorted(
            self.term_frequencies.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_terms[:limit]

class RedisTermManager:
    """Manages term frequencies in Redis with periodic PostgreSQL dumps"""
    
    def __init__(self, redis_client, shard_id: int):
        self.redis_client = redis_client
        self.shard_id = shard_id
        self.redis_key_prefix = f"terms:shard:{shard_id}"
        self.dump_interval = 30  # seconds
        self.last_dump = time.time()
        self.dump_lock = threading.Lock()
        
        # Start background dump thread
        self.dump_thread = threading.Thread(target=self._periodic_dump, daemon=True)
        self.dump_thread.start()
    
    def get_redis_key(self, term: str) -> str:
        """Get Redis key for a term"""
        return f"{self.redis_key_prefix}:{term}"
    
    def increment_term(self, term: str, delta: int = 1) -> int:
        """Increment term frequency in Redis (atomic operation)"""
        if not term:
            return 0
        
        term = term.lower().strip()
        if not term:
            return 0
        
        try:
            key = self.get_redis_key(term)
            new_freq = self.redis_client.incr(key, delta)
            
            # Set expiration for cleanup (24 hours)
            if new_freq == delta:  # First time setting this key
                self.redis_client.expire(key, 86400)
            
            return max(0, new_freq)
        except Exception as e:
            logger.error(f"Error incrementing term in Redis: {e}")
            return 0
    
    def get_term_frequency(self, term: str) -> int:
        """Get term frequency from Redis"""
        if not term:
            return 0
        
        term = term.lower().strip()
        try:
            key = self.get_redis_key(term)
            freq = self.redis_client.get(key)
            return int(freq) if freq else 0
        except Exception as e:
            logger.error(f"Error getting term frequency from Redis: {e}")
            return 0
    
    def get_all_terms(self) -> Dict[str, int]:
        """Get all terms for this shard from Redis"""
        try:
            pattern = f"{self.redis_key_prefix}:*"
            terms = {}
            
            for key in self.redis_client.scan_iter(match=pattern):
                term = key.decode('utf-8').split(':', 3)[-1]  # Extract term from key
                freq = self.redis_client.get(key)
                if freq:
                    terms[term] = int(freq)
            
            return terms
        except Exception as e:
            logger.error(f"Error getting all terms from Redis: {e}")
            return {}
    
    def _periodic_dump(self):
        """Background thread that periodically dumps Redis data to PostgreSQL"""
        while True:
            try:
                time.sleep(self.dump_interval)
                self.dump_to_postgres()
            except Exception as e:
                logger.error(f"Error in periodic dump: {e}")
    
    def dump_to_postgres(self):
        """Dump Redis term frequencies to PostgreSQL"""
        with self.dump_lock:
            try:
                # Get all terms from Redis
                redis_terms = self.get_all_terms()
                if not redis_terms:
                    return
                
                # Connect to PostgreSQL
                conn = psycopg2.connect(POSTGRES_URL)
                cursor = conn.cursor()
                
                # Update PostgreSQL with Redis data
                updated_count = 0
                inserted_count = 0
                
                for term, frequency in redis_terms.items():
                    if frequency <= 0:
                        # Delete terms with zero or negative frequency
                        cursor.execute(
                            "DELETE FROM terms WHERE term = %s AND shard_id = %s",
                            (term, self.shard_id)
                        )
                    else:
                        # Upsert term frequency
                        cursor.execute("""
                            INSERT INTO terms (term, frequency, shard_id, updated_at)
                            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (term, shard_id)
                            DO UPDATE SET 
                                frequency = %s,
                                updated_at = CURRENT_TIMESTAMP
                        """, (term, frequency, self.shard_id, frequency))
                        
                        if cursor.rowcount == 1:
                            inserted_count += 1
                        else:
                            updated_count += 1
                
                conn.commit()
                conn.close()
                
                logger.info(f"Dumped to PostgreSQL: {inserted_count} inserted, {updated_count} updated")
                
            except Exception as e:
                logger.error(f"Error dumping to PostgreSQL: {e}")

class TypeaheadService:
    def __init__(self):
        self.trie = PrefixTrie(TRIE_PATH)
        self.redis_manager = RedisTermManager(redis_client, SHARD_ID)
        self.db_connection = None
        self._connect_db()
        self._load_terms_from_db()
        
        # Start background trie update thread
        self.trie_update_thread = threading.Thread(target=self._periodic_trie_update, daemon=True)
        self.trie_update_thread.start()
    
    def _connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_connection = psycopg2.connect(POSTGRES_URL)
            logger.info(f"Connected to database for typeahead shard {SHARD_ID}")
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
    
    def _load_terms_from_db(self):
        """Load terms from database into trie"""
        try:
            cursor = self._get_db_cursor()
            cursor.execute(
                "SELECT term, frequency FROM terms WHERE shard_id = %s ORDER BY frequency DESC",
                (SHARD_ID,)
            )
            
            count = 0
            for row in cursor.fetchall():
                term, frequency = row[0], row[1]
                self.trie.insert(term, frequency)
                count += 1
            
            logger.info(f"Loaded {count} terms from database for shard {SHARD_ID}")
            
        except Exception as e:
            logger.error(f"Error loading terms from database: {e}")
    
    def _periodic_trie_update(self):
        """Periodically update trie with Redis data"""
        while True:
            try:
                time.sleep(60)  # Update trie every minute
                self._update_trie_from_redis()
            except Exception as e:
                logger.error(f"Error in periodic trie update: {e}")
    
    def _update_trie_from_redis(self):
        """Update trie with latest data from Redis"""
        try:
            redis_terms = self.redis_manager.get_all_terms()
            
            # Update trie with Redis data
            for term, frequency in redis_terms.items():
                if frequency > 0:
                    self.trie.insert(term, frequency - self.trie.term_frequencies.get(term, 0))
                else:
                    self.trie.remove(term)
            
            # Save updated trie
            self.trie.save_trie()
            
        except Exception as e:
            logger.error(f"Error updating trie from Redis: {e}")
    
    def get_suggestions(self, prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get typeahead suggestions for a prefix"""
        if not prefix or len(prefix) < 1:
            # Return popular terms if no prefix
            popular_terms = self.trie.get_popular_terms(limit)
            return [{"term": term, "frequency": freq} for term, freq in popular_terms]
        
        # Get suggestions from trie
        suggestions = self.trie.search_prefix(prefix, limit)
        return [{"term": term, "frequency": freq} for term, freq in suggestions]
    
    def add_term(self, term: str, frequency: int = 1):
        """Add or update a term (Redis-first approach)"""
        if not term:
            return False
        
        term = term.lower().strip()
        if not term:
            return False
        
        try:
            # Update Redis immediately (atomic, no locks)
            new_freq = self.redis_manager.increment_term(term, frequency)
            
            # Update local trie for immediate search availability
            self.trie.insert(term, frequency)
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding term: {e}")
            return False
    
    def remove_term(self, term: str):
        """Remove a term (Redis-first approach)"""
        if not term:
            return False
        
        term = term.lower().strip()
        if not term:
            return False
        
        try:
            # Set frequency to 0 in Redis (will be deleted during next dump)
            self.redis_manager.redis_client.set(self.redis_manager.get_redis_key(term), 0)
            
            # Remove from local trie
            self.trie.remove(term)
            
            return True
            
        except Exception as e:
            logger.error(f"Error removing term: {e}")
            return False
    
    def update_term_frequency(self, term: str, frequency_delta: int):
        """Update term frequency (Redis-first approach)"""
        if not term:
            return False
        
        term = term.lower().strip()
        if not term:
            return False
        
        try:
            # Update Redis immediately
            new_freq = self.redis_manager.increment_term(term, frequency_delta)
            
            # Update local trie
            if new_freq <= 0:
                self.trie.remove(term)
            else:
                self.trie.insert(term, frequency_delta)
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating term frequency: {e}")
            return False
    
    def rebuild_trie(self):
        """Rebuild trie from database"""
        try:
            # Clear existing trie
            self.trie = PrefixTrie(TRIE_PATH)
            
            # Reload from database
            self._load_terms_from_db()
            
            # Update with Redis data
            self._update_trie_from_redis()
            
            # Save trie
            self.trie.save_trie()
            
            return True
            
        except Exception as e:
            logger.error(f"Error rebuilding trie: {e}")
            return False

# Initialize typeahead service
typeahead_service = TypeaheadService()

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "shard_id": SHARD_ID,
        "total_shards": TOTAL_SHARDS,
        "terms_count": len(typeahead_service.trie.term_frequencies),
        "timestamp": time.time()
    })

@app.route('/suggest', methods=['GET'])
def suggest():
    """Get typeahead suggestions"""
    prefix = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 10))
    
    # Limit the maximum number of suggestions
    limit = min(limit, 50)
    
    suggestions = typeahead_service.get_suggestions(prefix, limit)
    
    return jsonify({
        "query": prefix,
        "suggestions": suggestions,
        "shard_id": SHARD_ID
    })

@app.route('/add-term', methods=['POST'])
def add_term():
    """Add a new term"""
    data = request.get_json()
    term = data.get('term', '').strip()
    frequency = data.get('frequency', 1)
    
    if not term:
        return jsonify({"error": "Term is required"}), 400
    
    success = typeahead_service.add_term(term, frequency)
    
    if success:
        return jsonify({"message": "Term added successfully", "term": term})
    else:
        return jsonify({"error": "Failed to add term"}), 500

@app.route('/remove-term', methods=['DELETE'])
def remove_term():
    """Remove a term"""
    data = request.get_json()
    term = data.get('term', '').strip()
    
    if not term:
        return jsonify({"error": "Term is required"}), 400
    
    success = typeahead_service.remove_term(term)
    
    if success:
        return jsonify({"message": "Term removed successfully", "term": term})
    else:
        return jsonify({"error": "Failed to remove term"}), 500

@app.route('/update-frequency', methods=['PUT'])
def update_frequency():
    """Update term frequency"""
    data = request.get_json()
    term = data.get('term', '').strip()
    frequency_delta = data.get('frequency_delta', 1)
    
    if not term:
        return jsonify({"error": "Term is required"}), 400
    
    success = typeahead_service.update_term_frequency(term, frequency_delta)
    
    if success:
        return jsonify({"message": "Frequency updated successfully", "term": term})
    else:
        return jsonify({"error": "Failed to update frequency"}), 500

@app.route('/popular', methods=['GET'])
def popular_terms():
    """Get popular terms"""
    limit = int(request.args.get('limit', 20))
    limit = min(limit, 100)
    
    popular = typeahead_service.trie.get_popular_terms(limit)
    
    return jsonify({
        "popular_terms": [{"term": term, "frequency": freq} for term, freq in popular],
        "shard_id": SHARD_ID
    })

@app.route('/stats')
def stats():
    """Get typeahead statistics"""
    try:
        cursor = typeahead_service._get_db_cursor()
        cursor.execute("SELECT COUNT(*) FROM terms WHERE shard_id = %s", (SHARD_ID,))
        db_count = cursor.fetchone()[0]
        
        cursor.execute(
            "SELECT AVG(frequency), MAX(frequency), MIN(frequency) FROM terms WHERE shard_id = %s",
            (SHARD_ID,)
        )
        avg_freq, max_freq, min_freq = cursor.fetchone()
        
        return jsonify({
            "shard_id": SHARD_ID,
            "total_shards": TOTAL_SHARDS,
            "terms_in_db": db_count,
            "terms_in_trie": len(typeahead_service.trie.term_frequencies),
            "avg_frequency": float(avg_freq) if avg_freq else 0,
            "max_frequency": max_freq or 0,
            "min_frequency": min_freq or 0,
            "trie_path": TRIE_PATH
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/rebuild', methods=['POST'])
def rebuild():
    """Rebuild trie from database"""
    success = typeahead_service.rebuild_trie()
    
    if success:
        return jsonify({
            "message": "Trie rebuilt successfully",
            "terms_loaded": len(typeahead_service.trie.term_frequencies),
            "shard_id": SHARD_ID
        })
    else:
        return jsonify({"error": "Failed to rebuild trie"}), 500

@app.route('/save', methods=['POST'])
def save_trie():
    """Save trie to disk"""
    try:
        typeahead_service.trie.save_trie()
        return jsonify({"message": "Trie saved successfully", "shard_id": SHARD_ID})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/redis-stats')
def redis_stats():
    """Get Redis statistics and sync status"""
    try:
        redis_terms = typeahead_service.redis_manager.get_all_terms()
        
        # Get database term count for comparison
        cursor = typeahead_service._get_db_cursor()
        cursor.execute("SELECT COUNT(*) FROM terms WHERE shard_id = %s", (SHARD_ID,))
        db_count = cursor.fetchone()[0]
        
        return jsonify({
            "shard_id": SHARD_ID,
            "redis_terms_count": len(redis_terms),
            "database_terms_count": db_count,
            "sync_interval_seconds": typeahead_service.redis_manager.dump_interval,
            "top_redis_terms": sorted(redis_terms.items(), key=lambda x: x[1], reverse=True)[:10],
            "redis_connected": redis_client.ping()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/force-sync', methods=['POST'])
def force_sync():
    """Force immediate Redis to PostgreSQL sync"""
    try:
        typeahead_service.redis_manager.dump_to_postgres()
        return jsonify({
            "message": "Sync completed successfully",
            "shard_id": SHARD_ID,
            "timestamp": time.time()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Save trie on startup
    typeahead_service.trie.save_trie()
    app.run(host='0.0.0.0', port=SERVICE_PORT, debug=False) 