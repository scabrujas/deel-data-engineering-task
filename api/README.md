### Analytics API

The project includes a RESTful API built with FastAPI that provides endpoints for accessing the analytical data. The API interacts directly with the target database views, so it always returns the most up-to-date data.

#### API Endpoints

1. **Orders by Delivery Date and Status**:
   - `GET /orders?status=open`: Retrieve information about the number of open orders by delivery date and status.
   - Query parameters:
     - `status`: Filter orders by status (open, pending, completed, cancelled, all)

2. **Top Delivery Dates with More Open Orders**:
   - `GET /orders/top?limit=3`: Retrieve information about the top delivery dates with more open orders.
   - Query parameters:
     - `limit`: Number of top delivery dates to return (default: 3)

3. **Open Pending Items by Product**:
   - `GET /orders/product`: Retrieve information about the number of open pending items by product ID.

4. **Top Customers with More Pending Orders**:
   - `GET /orders/customers?status=pending&limit=3`: Retrieve information about the top customers with more pending orders.
   - Query parameters:
     - `status`: Filter by order status (open, pending, completed, cancelled)
     - `limit`: Number of top customers to return (default: 3)

#### Using the API

You can access the API documentation at http://localhost:8000/docs after starting the services. The documentation provides interactive examples for each endpoint.

The following Makefile commands are available for working with the API:

```bash
# View API logs
make logs-api

# Open API documentation in browser
make api-docs
```

Example API requests:

```bash
# Get open orders by delivery date and status
curl http://localhost:8000/orders?status=open

# Get top 5 delivery dates with more open orders
curl http://localhost:8000/orders/top?limit=5

# Get open pending items by product
curl http://localhost:8000/orders/product

# Get top 3 customers with more pending orders
curl http://localhost:8000/orders/customers?status=pending&limit=3
```

#### API Integration

The API is automatically started as part of the docker-compose setup. It connects directly to the target database and uses the pre-defined views when appropriate, falling back to direct SQL queries when parameters are outside the default values.

Like the database views, the API endpoints always return real-time data as they query the database on each request. There is no caching at the API level, ensuring that you always get the most up-to-date information.
