import os
import json
import pickle
import time
import logging
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

class TypeaheadService:
    def __init__(self):
        self.trie = PrefixTrie(TRIE_PATH)
        self.db_connection = None
        self._connect_db()
        self._load_terms_from_db()
    
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
        """Add or update a term"""
        if not term:
            return False
        
        term = term.lower().strip()
        if not term:
            return False
        
        try:
            # Update trie
            self.trie.insert(term, frequency)
            
            # Update database
            cursor = self._get_db_cursor()
            cursor.execute("""
                INSERT INTO terms (term, frequency, shard_id) 
                VALUES (%s, %s, %s)
                ON CONFLICT (term, shard_id) 
                DO UPDATE SET frequency = terms.frequency + %s
            """, (term, frequency, SHARD_ID, frequency))
            
            self.db_connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error adding term: {e}")
            return False
    
    def remove_term(self, term: str):
        """Remove a term"""
        if not term:
            return False
        
        term = term.lower().strip()
        if not term:
            return False
        
        try:
            # Remove from trie
            self.trie.remove(term)
            
            # Remove from database
            cursor = self._get_db_cursor()
            cursor.execute(
                "DELETE FROM terms WHERE term = %s AND shard_id = %s",
                (term, SHARD_ID)
            )
            
            self.db_connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error removing term: {e}")
            return False
    
    def update_term_frequency(self, term: str, frequency_delta: int):
        """Update term frequency"""
        if not term:
            return False
        
        term = term.lower().strip()
        if not term:
            return False
        
        try:
            # Update trie
            current_freq = self.trie.term_frequencies.get(term, 0)
            new_freq = max(0, current_freq + frequency_delta)
            
            if new_freq == 0:
                self.remove_term(term)
            else:
                self.trie.insert(term, new_freq - current_freq)
                
                # Update database
                cursor = self._get_db_cursor()
                cursor.execute("""
                    UPDATE terms 
                    SET frequency = %s 
                    WHERE term = %s AND shard_id = %s
                """, (new_freq, term, SHARD_ID))
                
                self.db_connection.commit()
            
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

if __name__ == '__main__':
    # Save trie on startup
    typeahead_service.trie.save_trie()
    app.run(host='0.0.0.0', port=SERVICE_PORT, debug=False) 