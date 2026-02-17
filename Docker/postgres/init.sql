-- DocStream Schema

-- Documents table - tracks full lifecycle via status changes
CREATE TABLE documents (
    document_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'sent', 'viewed', 'signed', 'completed', 'declined')),
    template_category VARCHAR(100),  -- 'Sales', 'Legal', 'HR'
    
    -- Lifecycle timestamps (CDC will capture changes to these)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP,
    viewed_at TIMESTAMP,
    signed_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Updated whenever status changes
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure logical timestamp ordering
    CONSTRAINT valid_timestamps CHECK (
        (sent_at IS NULL OR sent_at >= created_at) AND
        (viewed_at IS NULL OR (sent_at IS NOT NULL AND viewed_at >= sent_at)) AND
        (signed_at IS NULL OR (viewed_at IS NOT NULL AND signed_at >= viewed_at)) AND
        (completed_at IS NULL OR (signed_at IS NOT NULL AND completed_at >= signed_at))
    )
);

-- Indexes for common queries
CREATE INDEX idx_documents_status ON documents(status);
CREATE INDEX idx_documents_created_at ON documents(created_at DESC);
CREATE INDEX idx_documents_template_category ON documents(template_category);

-- DCD setup for Debezium
ALTER TABLE documents REPLICA IDENTITY FULL;

-- Publication for Debezium (captures all changes)
CREATE PUBLICATION docstream_publication FOR TABLE documents;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO docuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO docuser;

-- Verification
DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'DocStream Database Initialized';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Table: documents (with status lifecycle)';
    RAISE NOTICE 'CDC: Enabled via publication';
    RAISE NOTICE 'Status flow: draft → sent → viewed → signed → completed';
    RAISE NOTICE '============================================';
END $$;

-- Create Marquez database
CREATE DATABASE marquez;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE marquez TO docuser;
