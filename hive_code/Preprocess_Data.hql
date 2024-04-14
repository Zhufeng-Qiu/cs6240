-- Create external tables for business and review data
CREATE EXTERNAL TABLE business_data (
    business_id STRING,
    name STRING,
    address STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INT,
    is_open INT,
    attributes MAP<STRING, STRING>,
    categories ARRAY<STRING>,
    hours MAP<STRING, STRING>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '../../../data/business.json';

CREATE EXTERNAL TABLE review_data (
    review_id STRING,
    user_id STRING,
    business_id STRING,
    stars INT,
    date STRING,
    text STRING,
    useful INT,
    funny INT,
    cool INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '../../../data/review.json';

-- Join business and review tables, filter businesses from "WA" state, and export the results to a CSV file
INSERT OVERWRITE LOCAL DIRECTORY '../../../data/output_ca.csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT DISTINCT r.user_id, r.business_id
FROM review_data r
JOIN business_data b ON r.business_id = b.business_id
WHERE b.state = 'WA';