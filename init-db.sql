-- Initialize the search database schema

-- Documents table for storing original documents
CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content TEXT NOT NULL,
    title VARCHAR(500),
    source VARCHAR(255),
    metadata JSONB,
    shard_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for shard-based queries
CREATE INDEX IF NOT EXISTS idx_documents_shard_id ON documents(shard_id);
CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source);
CREATE INDEX IF NOT EXISTS idx_documents_created_at ON documents(created_at);

-- Full-text search index on content and title
CREATE INDEX IF NOT EXISTS idx_documents_fts ON documents USING gin(to_tsvector('english', coalesce(title, '') || ' ' || content));

-- Terms table for typeahead functionality
CREATE TABLE IF NOT EXISTS terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR(255) NOT NULL,
    frequency INTEGER DEFAULT 1,
    shard_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for typeahead queries
CREATE INDEX IF NOT EXISTS idx_terms_term ON terms(term);
CREATE INDEX IF NOT EXISTS idx_terms_shard_id ON terms(shard_id);
CREATE INDEX IF NOT EXISTS idx_terms_frequency ON terms(frequency DESC);

-- Unique constraint to prevent duplicate terms per shard
CREATE UNIQUE INDEX IF NOT EXISTS idx_terms_unique ON terms(term, shard_id);

-- NEW: Term updates buffer table for high-concurrency writes
CREATE TABLE IF NOT EXISTS term_updates_buffer (
    id SERIAL PRIMARY KEY,
    term VARCHAR(255) NOT NULL,
    frequency_delta INTEGER NOT NULL,
    shard_id INTEGER NOT NULL,
    batch_id UUID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Index for efficient batch processing
CREATE INDEX IF NOT EXISTS idx_term_updates_batch ON term_updates_buffer(batch_id, processed);
CREATE INDEX IF NOT EXISTS idx_term_updates_term_shard ON term_updates_buffer(term, shard_id, processed);

-- NEW: Term frequency cache in Redis-like structure (for very hot terms)
CREATE TABLE IF NOT EXISTS term_frequency_cache (
    term VARCHAR(255) NOT NULL,
    shard_id INTEGER NOT NULL,
    frequency INTEGER NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (term, shard_id)
);

-- Index metadata table
CREATE TABLE IF NOT EXISTS index_metadata (
    shard_id INTEGER PRIMARY KEY,
    document_count INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    index_version INTEGER DEFAULT 1,
    status VARCHAR(50) DEFAULT 'active'
);

-- Initialize shard metadata (assuming 3 shards for search, 2 for typeahead)
INSERT INTO index_metadata (shard_id, document_count, status) VALUES 
(0, 0, 'active'),
(1, 0, 'active'),
(2, 0, 'active')
ON CONFLICT (shard_id) DO NOTHING;

-- Function to update document count
CREATE OR REPLACE FUNCTION update_document_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE index_metadata 
        SET document_count = document_count + 1, 
            last_updated = CURRENT_TIMESTAMP 
        WHERE shard_id = NEW.shard_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE index_metadata 
        SET document_count = document_count - 1, 
            last_updated = CURRENT_TIMESTAMP 
        WHERE shard_id = OLD.shard_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update document counts
CREATE TRIGGER trigger_update_document_count
    AFTER INSERT OR DELETE ON documents
    FOR EACH ROW EXECUTE FUNCTION update_document_count();

-- NEW: High-performance batch term update function
CREATE OR REPLACE FUNCTION batch_update_term_frequencies(
    p_batch_id UUID,
    p_shard_id INTEGER
)
RETURNS TABLE(terms_processed INTEGER, terms_updated INTEGER, terms_inserted INTEGER) AS $$
DECLARE
    v_terms_processed INTEGER := 0;
    v_terms_updated INTEGER := 0;
    v_terms_inserted INTEGER := 0;
    v_term_record RECORD;
BEGIN
    -- Process all updates for this batch
    FOR v_term_record IN 
        SELECT 
            term,
            SUM(frequency_delta) as total_delta
        FROM term_updates_buffer 
        WHERE batch_id = p_batch_id 
        AND shard_id = p_shard_id 
        AND processed = FALSE
        GROUP BY term
    LOOP
        v_terms_processed := v_terms_processed + 1;
        
        -- Try to update existing term
        UPDATE terms 
        SET frequency = GREATEST(0, frequency + v_term_record.total_delta),
            updated_at = CURRENT_TIMESTAMP
        WHERE term = v_term_record.term 
        AND shard_id = p_shard_id;
        
        IF FOUND THEN
            v_terms_updated := v_terms_updated + 1;
            
            -- Remove terms with zero frequency
            DELETE FROM terms 
            WHERE term = v_term_record.term 
            AND shard_id = p_shard_id 
            AND frequency <= 0;
        ELSE
            -- Insert new term if delta is positive
            IF v_term_record.total_delta > 0 THEN
                INSERT INTO terms (term, frequency, shard_id)
                VALUES (v_term_record.term, v_term_record.total_delta, p_shard_id)
                ON CONFLICT (term, shard_id) 
                DO UPDATE SET frequency = terms.frequency + v_term_record.total_delta,
                             updated_at = CURRENT_TIMESTAMP;
                v_terms_inserted := v_terms_inserted + 1;
            END IF;
        END IF;
    END LOOP;
    
    -- Mark buffer entries as processed
    UPDATE term_updates_buffer 
    SET processed = TRUE 
    WHERE batch_id = p_batch_id 
    AND shard_id = p_shard_id;
    
    RETURN QUERY SELECT v_terms_processed, v_terms_updated, v_terms_inserted;
END;
$$ LANGUAGE plpgsql;

-- NEW: Function to queue term updates (lock-free)
CREATE OR REPLACE FUNCTION queue_term_update(
    p_term VARCHAR(255),
    p_frequency_delta INTEGER,
    p_shard_id INTEGER,
    p_batch_id UUID DEFAULT NULL
)
RETURNS BOOLEAN AS $$
BEGIN
    INSERT INTO term_updates_buffer (term, frequency_delta, shard_id, batch_id)
    VALUES (p_term, p_frequency_delta, p_shard_id, COALESCE(p_batch_id, gen_random_uuid()));
    
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- NEW: Cleanup old processed buffer entries
CREATE OR REPLACE FUNCTION cleanup_term_buffer()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM term_updates_buffer 
    WHERE processed = TRUE 
    AND created_at < NOW() - INTERVAL '1 hour';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get shard for document (consistent hashing)
CREATE OR REPLACE FUNCTION get_document_shard(doc_id UUID, total_shards INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN abs(hashtext(doc_id::text)) % total_shards;
END;
$$ LANGUAGE plpgsql;

-- Function to get shard for term (for typeahead)
CREATE OR REPLACE FUNCTION get_term_shard(term_text VARCHAR, total_shards INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN abs(hashtext(lower(term_text))) % total_shards;
END;
$$ LANGUAGE plpgsql; 