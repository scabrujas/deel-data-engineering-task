#!/bin/bash
# Script to query the analytical views and display results

echo "======================================================================================"
echo "                         Deel Data Engineering Analytics                             "
echo "======================================================================================"

# Connect to analytics database and run queries
PGPASSWORD=1234 psql -h $TARGET_DB_HOST -U analytics_user -d analytics_db << EOF
\\echo '1. Open Orders by Delivery Date and Status'
\\echo '=========================================='
SELECT * FROM analytics.open_orders_by_delivery_date_status;

\\echo '2. Top 3 Delivery Dates with More Open Orders'
\\echo '============================================='
SELECT * FROM analytics.top_delivery_dates_open_orders;

\\echo '3. Open Pending Items by Product'
\\echo '================================'
SELECT * FROM analytics.open_pending_items_by_product;

\\echo '4. Top 3 Customers with More Pending Orders'
\\echo '=========================================='
SELECT * FROM analytics.top_customers_with_pending_orders;
EOF

echo "======================================================================================"
echo "                                 End of Report                                        "
echo "======================================================================================"
