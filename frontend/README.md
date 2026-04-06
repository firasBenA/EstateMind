
# EstateMind Dashboard

This is the new frontend dashboard for EstateMind.

## Setup

1.  Install dependencies:
    ```bash
    npm install
    ```

2.  Ensure the backend is running on port 8000:
    ```bash
    cd ../backend
    python manage.py runserver
    ```

3.  Start the frontend development server:
    ```bash
    npm run dev
    ```

The frontend will be available at http://localhost:8080 (or another port if 8080 is taken).
Requests to `/api/*` are proxied to the backend.

## Features

-   **Agent Dashboard**: View latest scraper run stats.
-   **Listings**: View distribution of listings by source.
-   **Scrapers**: View history of scraper runs.
-   **Metrics Grid**: Real-time stats on total listings, new records, and errors.
