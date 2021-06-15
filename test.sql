CREATE SCHEMA test;

CREATE TYPE test.test_states AS ENUM ('pending', 'completed');

CREATE TABLE test.test_parents (
    PRIMARY KEY (parent_id),
    parent_id uuid NOT NULL DEFAULT gen_random_uuid(),
    is_public boolean NOT NULL DEFAULT TRUE,
    state test.test_states NOT NULL DEFAULT 'pending',
    title text DEFAULT 'Something',
    created timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    availability daterange CHECK (NOT is_empty(availability)),
    numbers int[],
    geotransform double precision[6],
    matrix float[][],
    states test.test_states[],
    bbox box,
    metadata jsonb AS metadata_,

    CONSTRAINT test_parents_state_valid_check
         CHECK (state = ANY(states)),

    CONSTRAINT test_parents_pending_availability_excl
       EXCLUDE USING GIST (availability WITH &&)
         WHERE (state = 'pending')
);

CREATE INDEX test_parents
    ON test.test_parents (created) INCLUDE (state)
 WHERE (is_public);

CREATE TABLE test.test_children (
    PRIMARY KEY (parent_id, name),
    parent_id uuid NOT NULL REFERENCES test.test_parents (parent_id) ON DELETE CASCADE,
    name citext NOT NULL,
    full_name text NOT NULL UNIQUE,
    full_name_length int NOT NULL GENERATED ALWAYS AS (length(full_name)) STORED
);

CREATE INDEX test_children_unaccented_full_name
    ON test.test_children (unaccent(full_name));

CREATE TABLE test.test_children_items (
    PRIMARY KEY (item_id),
    item_id int NOT NULL GENERATED ALWAYS AS IDENTITY,
    parent_id uuid,
    name citext,

    CONSTRAINT test_children_items_parent_id_name_key
        UNIQUE (parent_id, name),

    CONSTRAINT test_children_items_parent_id_name_fkey
       FOREIGN KEY (parent_id, name)
    REFERENCES test.test_children (parent_id, name)
     ON UPDATE CASCADE
     ON DELETE SET NULL
);

CREATE FUNCTION test.test_function(int) RETURNS int AS $$
    SELECT $1 * 2;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE PROCEDURE test.test_procedure(v_parent_id uuid) AS $$
    DELETE FROM test.test_parents
     WHERE parent_id = v_parent_id;
$$ LANGUAGE SQL;

PREPARE test.test_query AS
SELECT *
  FROM test.test_parents
 WHERE parent_id = :parent_id;
