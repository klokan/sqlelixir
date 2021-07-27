CREATE SCHEMA test;

CREATE TYPE test.products AS ENUM ('Cloud', 'Transmogrifier');
CREATE TYPE test.subscription_types AS ENUM ('service', 'license');

CREATE TABLE test.accounts (
    PRIMARY KEY (account_id),
    account_id uuid NOT NULL,
    service_subscription_id uuid REFERENCES test.subscriptions (subscription_id)
);

CREATE TABLE test.subscriptions (
    PRIMARY KEY (subscription_id),
    subscription_id uuid NOT NULL,
    subscription_type test.subscription_types NOT NULL,
    account_id uuid NOT NULL REFERENCES test.accounts (account_id),
    active tstzrange NOT NULL DEFAULT tstzrange(CURRENT_TIMESTAMP, NULL),
    product test.products NOT NULL,

    CONSTRAINT subscriptions_account_id_active_excl
       EXCLUDE (account_id WITH =, active WITH &&)
         WHERE (subscription_type = 'license')
);

CREATE INDEX subscriptions_product_active_idx
    ON test.subscriptions (product) INCLUDE (subscription_id)
 WHERE (upper_inf(active));
