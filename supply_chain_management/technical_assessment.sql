-- 1. Total quantity shipped for each product category
SELECT p.category, SUM(o.quantity) AS total_quantity_shipped
FROM supply_chain_schema.products p
JOIN supply_chain_schema.orders o ON p.product_id = o.product_id
JOIN supply_chain_schema.shipments s ON o.order_id = s.order_id
GROUP BY p.category;


-- 2. Warehouses with the most efficient shipping processes based on average shipping times:
SELECT w.warehouse_name, ROUND(AVG(t.shipping_time), 1) AS avg_shipping_time
FROM supply_chain_schema.warehouses w
JOIN supply_chain_schema.shipments s ON w.warehouse_id = s.warehouse_id
JOIN supply_chain_schema.time t ON s.shipment_date = t.shipment_date
GROUP BY w.warehouse_name
ORDER BY avg_shipping_time ASC;


-- 3. Total value of shipments for each supplier:
SELECT sup.supplier_name, SUM(p.unit_price * o.quantity) AS total_shipment_value
FROM supply_chain_schema.suppliers sup
JOIN supply_chain_schema.orders o ON sup.supplier_id = o.supplier_id
JOIN supply_chain_schema.products p ON o.product_id = p.product_id
GROUP BY sup.supplier_name;


-- 4. Top 5 products with the highest total shipment quantities:
SELECT p.product_name, SUM(o.quantity) AS total_shipment_quantity
FROM supply_chain_schema.products p
JOIN supply_chain_schema.orders o ON p.product_id = o.product_id
GROUP BY p.product_name
ORDER BY total_shipment_quantity DESC
LIMIT 5;


-- 5. Distribution of shipment values for each product category:
SELECT p.category, SUM(p.unit_price * o.quantity) AS total_shipment_value
FROM supply_chain_schema.products p
JOIN supply_chain_schema.orders o ON p.product_id = o.product_id
GROUP BY p.category;


-- Advanced
-- 6. Stored Procedure to Update Shipment Records
CREATE OR REPLACE PROCEDURE update_shipment(
    p_shipment_id INTEGER,
    p_shipment_date DATE,
    p_shipping_time INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE supply_chain_schema.shipments
    SET shipment_date = p_shipment_date
    WHERE shipment_id = p_shipment_id;

    UPDATE supply_chain_schema.time
    SET shipment_date = p_shipment_date, shipping_time = p_shipping_time
    WHERE date_id = p_shipment_id;
END;
$$;

-- 7. View for Consolidated Summary of Shipment and Product Performance
CREATE OR REPLACE VIEW supply_chain_schema.shipment_product_summary AS
SELECT
    p.product_id,
    p.product_name,
    COUNT(s.shipment_id) AS total_shipments,
    SUM(o.quantity) AS total_quantity,
    AVG(t.shipping_time) AS average_shipping_time
FROM
    supply_chain_schema.shipments s
JOIN
    supply_chain_schema.orders o ON s.order_id = o.order_id
JOIN
    supply_chain_schema.products p ON o.product_id = p.product_id
JOIN
    supply_chain_schema.time t ON s.shipment_id = t.date_id
GROUP BY
    p.product_id, p.product_name;

-- 8. Identify Suppliers with Significant Change in Shipment Values
WITH yearly_shipments AS (
    SELECT
        s.supplier_id,
        EXTRACT(YEAR FROM o.order_date) AS year,
        SUM(o.quantity * p.unit_price) AS total_value
    FROM
        supply_chain_schema.orders o
    JOIN
        supply_chain_schema.products p ON o.product_id = p.product_id
    JOIN
        supply_chain_schema.suppliers s ON o.supplier_id = s.supplier_id
    GROUP BY
        s.supplier_id, EXTRACT(YEAR FROM o.order_date)
)
SELECT
    current_year.supplier_id,
    current_year.total_value AS current_year_value,
    previous_year.total_value AS previous_year_value,
    (current_year.total_value - previous_year.total_value) AS value_change,
    (current_year.total_value - previous_year.total_value) / previous_year.total_value * 100 AS percent_change
FROM
    yearly_shipments current_year
JOIN
    yearly_shipments previous_year ON current_year.supplier_id = previous_year.supplier_id
    AND current_year.year = previous_year.year + 1
WHERE
    (current_year.total_value - previous_year.total_value) / previous_year.total_value * 100 > 20
    OR (current_year.total_value - previous_year.total_value) / previous_year.total_value * 100 < -20;


-- 9. Trigger to Update Historical Tracking Table

-- Create the historical tracking table:
CREATE TABLE supply_chain_schema.shipment_history (
    history_id SERIAL PRIMARY KEY,
    shipment_id INTEGER,
    operation_type VARCHAR(10),
    old_shipment_date DATE,
    new_shipment_date DATE,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- The trigger function
CREATE OR REPLACE FUNCTION update_shipment_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO supply_chain_schema.shipment_history (
            shipment_id,
            operation_type,
            old_shipment_date,
            new_shipment_date
        )
        VALUES (
            OLD.shipment_id,
            'UPDATE',
            OLD.shipment_date,
            NEW.shipment_date
        );
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO supply_chain_schema.shipment_history (
            shipment_id,
            operation_type,
            old_shipment_date
        )
        VALUES (
            OLD.shipment_id,
            'DELETE',
            OLD.shipment_date
        );
        RETURN OLD;
    END IF;
END;
$$;

-- The trigger
CREATE TRIGGER shipment_history_trigger
AFTER UPDATE OR DELETE
ON supply_chain_schema.shipments
FOR EACH ROW
EXECUTE FUNCTION update_shipment_history();
