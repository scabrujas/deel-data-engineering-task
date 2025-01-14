CREATE SCHEMA IF NOT EXISTS operations;

CREATE TABLE IF NOT EXISTS operations.customers(
    customer_id      BIGSERIAL NOT NULL PRIMARY KEY,
    customer_name    VARCHAR(500) NOT NULL,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    customer_address VARCHAR(500),
    updated_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    created_by       BIGINT
);

alter table operations.customers REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS operations.products(
    product_id       BIGSERIAL NOT NULL PRIMARY KEY,
    product_name     VARCHAR(500) NOT NULL,
    barcode          VARCHAR(26) NOT NULL,
    unity_price      DECIMAL NOT NULL,
    is_active        BOOLEAN,
    updated_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    created_by       BIGINT
);

alter table operations.products REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS operations.orders(
    order_id         BIGSERIAL NOT NULL PRIMARY KEY,
    order_date       DATE,
    delivery_date    DATE,
    customer_id      BIGINT,
    status           VARCHAR,
    updated_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    created_by       BIGINT
);

alter table operations.orders REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS operations.order_items(
    order_item_id    BIGSERIAL NOT NULL PRIMARY KEY,
    order_id         BIGINT,
    product_id       BIGINT,
    quanity          INTEGER,
    updated_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    updated_by       BIGINT,
    created_at       TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    created_by       BIGINT
);

alter table operations.order_items REPLICA IDENTITY FULL;

-- Create CDC User

CREATE ROLE cdc_replication REPLICATION LOGIN;
CREATE USER cdc_user WITH ROLE cdc_replication PASSWORD 'cdc_1234';

-- Create Replication Slot
SELECT pg_create_logical_replication_slot('cdc_pgoutput', 'pgoutput');

-- Create Publication
CREATE PUBLICATION cdc_publication FOR ALL TABLES;

-- Grant Permissions
GRANT CONNECT ON DATABASE finance_db TO cdc_user;
GRANT USAGE ON SCHEMA operations TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA operations TO cdc_user;

CREATE OR REPLACE PROCEDURE update_customers(p_customers_to_update_or_insert INTEGER DEFAULT 1)
LANGUAGE plpgsql AS $$
DECLARE
    v_customer_id      			    INTEGER      := NULL;
    v_customer_name    			    VARCHAR(500) := NULL;
    v_is_active        			    BOOLEAN      := NULL;
    v_customer_address 			    VARCHAR(500) := NULL;
    v_current_value    			    INTEGER      := 0;
	v_customers_to_update_or_insert INTEGER      := p_customers_to_update_or_insert;

    v_customer_names    VARCHAR(500)[] := NULL;
    v_active_statuses   BOOLEAN[]      := NULL;
    v_customer_adresses VARCHAR(500)[] := NULL;
BEGIN
    v_customer_names := ARRAY[
        'ACME INC', 'Amazing Customer INC', 'Amazing Partner LLC',
        'Green Forest INC', 'Blue Sky LLC', 'AI Customer INC', 'Creative Company',
        'Care Company', 'Cars Company', 'Best Sellers', 'Book Sellers', 'Car Sellers',
        'HR Company', 'Computer Company', 'Awesome Comapany 2.0', 'Very Good Company',
        'Door Company', 'Sun Company', 'Sky Company', 'Ventures Company'
    ];
    v_active_statuses := ARRAY[TRUE, FALSE];
    v_customer_adresses := ARRAY['Rain Forest Street, 1234, Fantasy City, USA', 'Blue Sky Street, 1234, Fantasy City, USA', 'Awesome Street, 1234, Fantasy City, USA'];

    WHILE v_current_value <= v_customers_to_update_or_insert LOOP

        SELECT v_customer_names[(FLOOR(RANDOM() * 20) + 1)]
        INTO v_customer_name;

        SELECT v_customer_adresses[(FLOOR(RANDOM() * 3) + 1)]
        INTO v_customer_address;

		SELECT v_active_statuses[(FLOOR(RANDOM() * 2) + 1)]
		INTO v_is_active;
		
        SELECT customer_id
        INTO v_customer_id
        FROM operations.customers
        WHERE customer_name = v_customer_name;

        IF v_customer_id IS NULL THEN
            INSERT INTO operations.customers(customer_name, is_active, customer_address, updated_at, updated_by, created_at, created_by)
            VALUES (v_customer_name, TRUE, v_customer_address, CURRENT_TIMESTAMP(3), -1, CURRENT_TIMESTAMP(3), -1);
		ELSE
			UPDATE operations.customers
			SET customer_address = v_customer_address,
				is_active = v_is_active,
				updated_by = -1,
				updated_at = CURRENT_TIMESTAMP(3)
			WHERE customer_id = v_customer_id;
        END IF;

		COMMIT;
        
		v_current_value := v_current_value + 1;

    END LOOP;
END; $$;


CREATE OR REPLACE PROCEDURE update_products(p_products_to_update_or_insert INTEGER DEFAULT 1)
LANGUAGE plpgsql AS $$
DECLARE
    v_product_id      			    INTEGER      := NULL;
    v_product_name    			    VARCHAR(500) := NULL;
    v_is_active                     BOOLEAN      := NULL;
    v_barcode        			    VARCHAR(26)  := NULL;
    v_unity_price                   DECIMAL      := NULL;

	v_current_value 			   INTEGER := 0;
	v_products_to_update_or_insert INTEGER := p_products_to_update_or_insert;

    v_product_names    VARCHAR(500)[]  := NULL;
    v_active_statuses   BOOLEAN[]      := NULL;
BEGIN

    v_product_names := ARRAY[
        'Pencil', 'Pen', 'Screen', 'Laptop', 'Car', 'Truck', 'Basketball Ball', 'Baseball Ball', 'Motorcycle',
        'Parrot', 'Goldfish', 'Rice', 'Beans', 'TV', 'Fridge', 'Oven', 'Cooker', 'Desk', 'Chair', 'Wheel', 'Tyre',
        'Break Pads', 'Spot Lights', 'T-Shirt', 'Jacket', 'Pants', 'Gasoline', 'Tickets'
    ];

    v_active_statuses := ARRAY[TRUE, FALSE];


    WHILE v_current_value <= v_products_to_update_or_insert LOOP
    
        SELECT v_product_names[(FLOOR(RANDOM() * 28) + 1)]
        INTO v_product_name;

        SELECT product_id
        INTO v_product_id
        FROM operations.products
        WHERE product_name = v_product_name;

        SELECT ROUND(CAST(RANDOM()*(RANDOM()*100) AS NUMERIC), 2)::FLOAT
        INTO v_unity_price;

        IF v_product_id IS NULL THEN

            SELECT CAST(100000000000 + floor(random() * 900000000000) AS bigint)
            INTO v_barcode;

            INSERT INTO operations.products(product_name, barcode, unity_price, is_active, updated_at, updated_by, created_at, created_by)
            VALUES (v_product_name, v_barcode, v_unity_price, TRUE, CURRENT_TIMESTAMP(3), -1, CURRENT_TIMESTAMP(3), -1);
        ELSE
            SELECT v_active_statuses[(FLOOR(RANDOM() * 2) + 1)]
            INTO v_is_active;

            UPDATE operations.products
            SET unity_price = v_unity_price,
                is_active = v_is_active,
                updated_at = CURRENT_TIMESTAMP(3),
                updated_by = -1
            WHERE product_id = v_product_id;
        END IF;

        COMMIT;
            
        v_current_value := v_current_value + 1;

    END LOOP;
END; $$;

CREATE OR REPLACE PROCEDURE generate_orders(p_orders_to_generate INTEGER DEFAULT 1, p_itens_per_order INTEGER DEFAULT 25)
LANGUAGE plpgsql AS $$
DECLARE
    v_order_id BIGINT := NULL;
    v_product_id BIGINT := NULL;
    v_customer_id BIGINT := NULL;
    v_order_status VARCHAR(20) := NULL;
    v_quantity INTEGER := NULL;

    v_current_value    INTEGER := 0;
    v_itens_per_order  INTEGER := p_itens_per_order;

    v_current_order_value INTEGER := 0;
    v_orders_to_generate INTEGER := p_orders_to_generate;

    v_product_ids  BIGINT[]        := NULL;
    v_customer_ids BIGINT[]        := NULL;
    v_order_statuses VARCHAR(20)[] := NULL;
BEGIN

    v_order_statuses := ARRAY['PROCESSING', 'COMPLETED', 'REPROCESSING'];

    SELECT ARRAY_AGG(customer_id)
      INTO v_customer_ids
      FROM finance_db.operations.customers;

    SELECT ARRAY_AGG(product_id)
      INTO v_product_ids
      FROM finance_db.operations.products;


    WHILE v_current_order_value <= v_orders_to_generate LOOP
        SELECT v_customer_ids[(FLOOR(RANDOM() * ARRAY_LENGTH(v_customer_ids, 1)) + 1)]
        INTO v_customer_id;

        SELECT order_id
        INTO v_order_id
        FROM operations.orders
        WHERE customer_id = v_customer_id
        AND order_date = CURRENT_DATE
        AND status != 'COMPLETED'
        LIMIT 1;


        IF v_order_id IS NULL THEN
            INSERT INTO operations.orders(order_date, delivery_date, customer_id, status, updated_at, updated_by, created_at, created_by)
                VALUES (CURRENT_DATE, CURRENT_DATE + 10, v_customer_id, 'PENDING', CURRENT_TIMESTAMP(3), -1, CURRENT_TIMESTAMP(3), -1); 

            WHILE v_current_value <= v_itens_per_order LOOP
                SELECT order_id
                INTO v_order_id
                FROM operations.orders
                WHERE customer_id = v_customer_id
                AND order_date = CURRENT_DATE
                LIMIT 1;

                SELECT v_product_ids[(FLOOR(RANDOM() * ARRAY_LENGTH(v_product_ids, 1)) + 1)]
                INTO v_product_id;

                SELECT FLOOR(RANDOM() * 1000)
                INTO v_quantity;

                INSERT INTO operations.order_items(order_id, product_id, quanity, updated_at, updated_by, created_at, created_by)
                VALUES (v_order_id, v_product_id, v_quantity, CURRENT_TIMESTAMP(3), -1, CURRENT_TIMESTAMP(3), -1);

				v_current_value := v_current_value + 1;

				COMMIT;
            END LOOP;

			v_current_value := 0;

        ELSE

            SELECT v_order_statuses[(FLOOR(RANDOM() * 3) + 1)]
            INTO v_order_status;

            UPDATE operations.orders SET 
                status = v_order_status,
                updated_at = CURRENT_TIMESTAMP(3),
                updated_by = -1
            WHERE order_id = v_order_id;

        END IF;
		
		v_current_order_value := v_current_order_value + 1;
		
		COMMIT;
    END LOOP;

END; $$;

-- initialize values

call update_customers(5);
call update_products(30);
call generate_orders(10);

-- Schedule DB Jobs
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
    'update_customers',
    '*/2 * * * *',
    $$ call update_customers(5); $$
);

SELECT cron.schedule(
    'update_products',
    '*/2 * * * *',
    $$ call update_products(10); $$
);

SELECT cron.schedule(
    'generate_orders',
    '*/1 * * * *',
    $$ call generate_orders(100); $$
);