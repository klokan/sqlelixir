CREATE SCHEMA test;

CREATE TYPE test.test_states AS ENUM ('pending', 'completed');

CREATE TABLE test.test_parents (
    PRIMARY KEY (parent_id),
    parent_id uuid NOT NULL DEFAULT gen_random_uuid(),
    is_public boolean NOT NULL DEFAULT TRUE,
    state test.test_states NOT NULL DEFAULT 'pending',
    title text DEFAULT 'Something',
    created timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    availability daterange,
    numbers int[],
    geotransform double precision[6],
    matrix float[][],
    states test.test_states[],
    bbox box,
    metadata jsonb AS metadata_
);

CREATE TABLE test.test_children (
    PRIMARY KEY (parent_id, name),
    parent_id uuid NOT NULL REFERENCES test.test_parents (parent_id),
    name citext NOT NULL,
    full_name text NOT NULL UNIQUE
);

CREATE FUNCTION test.test_function(int) RETURNS int AS $$
    SELECT $1 * 2;
$$ LANGUAGE SQL IMMUTABLE STRICT;

PREPARE test.test_query AS
SELECT *
  FROM test.test_parents
 WHERE is_public;
