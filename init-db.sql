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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for typeahead queries
CREATE INDEX IF NOT EXISTS idx_terms_term ON terms(term);
CREATE INDEX IF NOT EXISTS idx_terms_shard_id ON terms(shard_id);
CREATE INDEX IF NOT EXISTS idx_terms_frequency ON terms(frequency DESC);

-- Unique constraint to prevent duplicate terms per shard
CREATE UNIQUE INDEX IF NOT EXISTS idx_terms_unique ON terms(term, shard_id);

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