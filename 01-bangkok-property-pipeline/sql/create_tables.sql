-- Drop table if rebuilding from scratch
DROP TABLE IF EXISTS property_listings;

-- Main property listings table
CREATE TABLE IF NOT EXISTS property_listings (
    id                SERIAL PRIMARY KEY,
    district          VARCHAR(100),
    property_type     VARCHAR(100),
    price             NUMERIC(15, 2),
    area_sqm          NUMERIC(10, 2),
    price_per_sqm     NUMERIC(15, 2),
    bedrooms          INTEGER,
    bathrooms         INTEGER,
    price_tier        VARCHAR(50),
    listing_date      DATE,
    ingestion_date    TIMESTAMP,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster filtering in Power BI
CREATE INDEX IF NOT EXISTS idx_district ON property_listings(district);
CREATE INDEX IF NOT EXISTS idx_property_type ON property_listings(property_type);
CREATE INDEX IF NOT EXISTS idx_price_tier ON property_listings(price_tier);