#!/usr/bin/env python3
"""
Script to automatically set up a Metabase dashboard for flight analytics.

This script uses the Metabase API to:
1. Connect to the DuckDB database
2. Create saved questions (visualizations)
3. Create a dashboard with all the questions

Usage:
    python setup_dashboard.py --url http://localhost:3000 --email admin@example.com --password yourpassword

Prerequisites:
    - Metabase must be running and initialized
    - DuckDB database must be connected in Metabase
    - dbt models must have been run (fct_flights, fct_route_daily exist)
"""

import argparse
import json
import logging
import sys
import time
from typing import Any, Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


class MetabaseClient:
    """Client for interacting with Metabase API."""

    def __init__(self, base_url: str, email: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session_token: Optional[str] = None
        self._login(email, password)

    def _login(self, email: str, password: str) -> None:
        """Authenticate with Metabase."""
        response = self.session.post(
            f"{self.base_url}/api/session",
            json={"username": email, "password": password},
        )
        response.raise_for_status()
        self.session_token = response.json()["id"]
        self.session.headers.update({"X-Metabase-Session": self.session_token})
        logger.info("Successfully authenticated with Metabase")

    def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        """Make an API request."""
        url = f"{self.base_url}/api{endpoint}"
        response = self.session.request(method, url, **kwargs)
        if not response.ok:
            logger.error(f"API error: {response.status_code} - {response.text}")
        response.raise_for_status()
        return response.json() if response.content else {}

    def get_databases(self) -> list[dict]:
        """Get all databases."""
        return self._request("GET", "/database")

    def get_database_by_name(self, name: str) -> Optional[dict]:
        """Find a database by name."""
        databases = self.get_databases()
        for db in databases.get("data", databases):
            if db.get("name", "").lower() == name.lower():
                return db
        return None

    def add_duckdb_database(self, name: str, path: str) -> dict:
        """Add a DuckDB database connection.
        
        The DuckDB Metabase driver expects 'database_file' in details.
        See: https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver
        """
        payload = {
            "engine": "duckdb",
            "name": name,
            "details": {
                "database_file": path,
                "read_only": True,
                "old_implicit_casting": True,  # Required for datetime filtering
            },
            "is_full_sync": True,
            "is_on_demand": False,
        }
        logger.debug(f"Adding database with payload: {payload}")
        return self._request("POST", "/database", json=payload)

    def sync_database(self, database_id: int) -> None:
        """Trigger a database sync."""
        self._request("POST", f"/database/{database_id}/sync_schema")
        logger.info(f"Triggered sync for database {database_id}")

    def get_tables(self, database_id: int) -> list[dict]:
        """Get tables for a database."""
        metadata = self._request("GET", f"/database/{database_id}/metadata")
        tables = []
        for table in metadata.get("tables", []):
            tables.append(table)
        return tables

    def get_table_by_name(self, database_id: int, table_name: str) -> Optional[dict]:
        """Find a table by name."""
        tables = self.get_tables(database_id)
        for table in tables:
            if table.get("name", "").lower() == table_name.lower():
                return table
        return None

    def create_card(self, card_data: dict) -> dict:
        """Create a saved question (card)."""
        return self._request("POST", "/card", json=card_data)

    def create_dashboard(self, name: str, description: str = "") -> dict:
        """Create a new dashboard."""
        return self._request(
            "POST",
            "/dashboard",
            json={"name": name, "description": description},
        )

    def add_card_to_dashboard(
        self,
        dashboard_id: int,
        card_id: int,
        row: int,
        col: int,
        size_x: int = 6,
        size_y: int = 4,
    ) -> dict:
        """Add a card to a dashboard using the dashcards endpoint."""
        # Metabase API changed - use PUT to update dashboard with dashcards
        return self._request(
            "PUT",
            f"/dashboard/{dashboard_id}",
            json={
                "dashcards": [{
                    "id": -1,  # Negative ID for new cards
                    "card_id": card_id,
                    "row": row,
                    "col": col,
                    "size_x": size_x,
                    "size_y": size_y,
                }],
            },
        )

    def get_dashboard(self, dashboard_id: int) -> dict:
        """Get dashboard details."""
        return self._request("GET", f"/dashboard/{dashboard_id}")

    def get_all_dashboards(self) -> list[dict]:
        """Get all dashboards."""
        return self._request("GET", "/dashboard")

    def delete_dashboard(self, dashboard_id: int) -> None:
        """Delete a dashboard."""
        self._request("DELETE", f"/dashboard/{dashboard_id}")
        logger.info(f"Deleted dashboard ID {dashboard_id}")

    def get_all_cards(self) -> list[dict]:
        """Get all saved questions/cards."""
        return self._request("GET", "/card")

    def delete_card(self, card_id: int) -> None:
        """Delete a saved question/card."""
        self._request("DELETE", f"/card/{card_id}")
        logger.info(f"Deleted card ID {card_id}")

    def update_dashboard_cards(
        self,
        dashboard_id: int,
        dashcards: list[dict],
    ) -> dict:
        """Update all dashboard cards at once.
        
        For Metabase v0.48+, we need to:
        1. GET the current dashboard (to get all required fields)
        2. PUT with the updated dashcards list
        """
        # First, get the current dashboard state
        try:
            current_dashboard = self.get_dashboard(dashboard_id)
        except Exception as e:
            logger.error(f"Failed to get dashboard {dashboard_id}: {e}")
            raise

        # Merge existing dashcards with new ones
        existing_dashcards = current_dashboard.get("dashcards", [])
        
        # Build payload with all required dashboard fields
        payload = {
            "name": current_dashboard.get("name"),
            "description": current_dashboard.get("description"),
            "dashcards": existing_dashcards + dashcards,
        }
        
        try:
            result = self._request(
                "PUT",
                f"/dashboard/{dashboard_id}",
                json=payload,
            )
            logger.info(f"Successfully updated dashboard {dashboard_id} with {len(dashcards)} new cards")
            return result
        except Exception as e:
            logger.error(f"PUT /dashboard/{dashboard_id} failed: {e}")
            
            # Fallback: Try POST to /dashboard/:id/cards one by one
            logger.info("Trying fallback method: POST cards one by one...")
            for card in dashcards:
                try:
                    self._request(
                        "POST",
                        f"/dashboard/{dashboard_id}/cards",
                        json={
                            "cardId": card["card_id"],
                            "row": card["row"],
                            "col": card["col"],
                            "size_x": card["size_x"],
                            "size_y": card["size_y"],
                        },
                    )
                    logger.info(f"Added card {card['card_id']} via POST")
                except Exception as card_error:
                    logger.warning(f"Failed to add card {card['card_id']}: {card_error}")

            return {"id": dashboard_id}


def create_flight_questions(client: MetabaseClient, database_id: int) -> list[dict]:
    """Create all flight analytics questions."""
    questions = []

    # Question 1: Total Flights by Date
    q1 = client.create_card({
        "name": "Total Flights by Date",
        "display": "line",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        flight_date,
                        COUNT(*) as total_flights
                    FROM fct_flights
                    GROUP BY flight_date
                    ORDER BY flight_date
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Number of Flights",
        },
    })
    questions.append(q1)
    logger.info(f"Created question: {q1['name']}")

    # Question 2: Delay Category Distribution
    q2 = client.create_card({
        "name": "Delay Category Distribution",
        "display": "pie",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        delay_category,
                        COUNT(*) as count
                    FROM fct_flights
                    GROUP BY delay_category
                    ORDER BY count DESC
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "pie.show_legend": True,
            "pie.show_data_labels": True,
        },
    })
    questions.append(q2)
    logger.info(f"Created question: {q2['name']}")

    # Question 3: Average Delay by Time of Day
    q3 = client.create_card({
        "name": "Average Delay by Time of Day",
        "display": "bar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        time_of_day,
                        ROUND(AVG(dep_delay_min), 2) as avg_delay_min
                    FROM fct_flights
                    WHERE dep_delay_min IS NOT NULL
                    GROUP BY time_of_day
                    ORDER BY 
                        CASE time_of_day 
                            WHEN 'Morning' THEN 1 
                            WHEN 'Afternoon' THEN 2 
                            WHEN 'Evening' THEN 3 
                            WHEN 'Night' THEN 4 
                        END
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Time of Day",
            "graph.y_axis.title_text": "Average Delay (min)",
        },
    })
    questions.append(q3)
    logger.info(f"Created question: {q3['name']}")

    # Question 4: Top 10 Routes by Delay Percentage
    q4 = client.create_card({
        "name": "Top 10 Routes by Delay Percentage",
        "display": "row",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        route,
                        ROUND(AVG(delayed_pct), 2) as avg_delayed_pct,
                        SUM(total_flights) as total_flights
                    FROM fct_route_daily
                    GROUP BY route
                    HAVING SUM(total_flights) >= 5
                    ORDER BY avg_delayed_pct DESC
                    LIMIT 10
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Delay %",
        },
    })
    questions.append(q4)
    logger.info(f"Created question: {q4['name']}")

    # Question 5: Flights by Airline
    q5 = client.create_card({
        "name": "Flights by Airline",
        "display": "bar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        airline_iata,
                        COUNT(*) as total_flights
                    FROM fct_flights
                    WHERE airline_iata IS NOT NULL
                    GROUP BY airline_iata
                    ORDER BY total_flights DESC
                    LIMIT 15
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Airline",
            "graph.y_axis.title_text": "Number of Flights",
        },
    })
    questions.append(q5)
    logger.info(f"Created question: {q5['name']}")

    # Question 6: Daily Average Delay Trend
    q6 = client.create_card({
        "name": "Daily Average Delay Trend",
        "display": "line",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        flight_date,
                        ROUND(AVG(dep_delay_min), 2) as avg_delay_min,
                        ROUND(AVG(avg_delay_7d), 2) as rolling_avg_7d
                    FROM fct_flights
                    WHERE dep_delay_min IS NOT NULL
                    GROUP BY flight_date
                    ORDER BY flight_date
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Delay (min)",
            "graph.show_goal": False,
        },
    })
    questions.append(q6)
    logger.info(f"Created question: {q6['name']}")

    # Question 7: Weekend vs Weekday Performance
    q7 = client.create_card({
        "name": "Weekend vs Weekday Performance",
        "display": "bar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                        COUNT(*) as total_flights,
                        ROUND(AVG(dep_delay_min), 2) as avg_delay_min,
                        ROUND(SUM(CASE WHEN is_delayed_15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delayed_pct
                    FROM fct_flights
                    GROUP BY is_weekend
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {},
    })
    questions.append(q7)
    logger.info(f"Created question: {q7['name']}")

    # Question 8: KPI - Total Flights (Scalar)
    q8 = client.create_card({
        "name": "Total Flights",
        "display": "scalar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT COUNT(*) as total FROM fct_flights",
            },
            "database": database_id,
        },
        "visualization_settings": {
            "scalar.field": "total",
        },
    })
    questions.append(q8)
    logger.info(f"Created question: {q8['name']}")

    # Question 9: KPI - On-Time Rate
    q9 = client.create_card({
        "name": "On-Time Rate (%)",
        "display": "scalar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        ROUND(SUM(CASE WHEN delay_category IN ('ontime', 'early') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as ontime_pct
                    FROM fct_flights
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "scalar.field": "ontime_pct",
        },
    })
    questions.append(q9)
    logger.info(f"Created question: {q9['name']}")

    # Question 10: KPI - Average Delay
    q10 = client.create_card({
        "name": "Average Delay (min)",
        "display": "scalar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT ROUND(AVG(dep_delay_min), 1) as avg_delay
                    FROM fct_flights
                    WHERE dep_delay_min IS NOT NULL
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "scalar.field": "avg_delay",
        },
    })
    questions.append(q10)
    logger.info(f"Created question: {q10['name']}")

    # Question 11: Flights by Hour
    q11 = client.create_card({
        "name": "Flights by Departure Hour",
        "display": "bar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        dep_hour,
                        COUNT(*) as flights
                    FROM fct_flights
                    WHERE dep_hour IS NOT NULL
                    GROUP BY dep_hour
                    ORDER BY dep_hour
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Hour",
            "graph.y_axis.title_text": "Flights",
        },
    })
    questions.append(q11)
    logger.info(f"Created question: {q11['name']}")

    # Question 12: Top Departure Airports
    q12 = client.create_card({
        "name": "Top 10 Departure Airports",
        "display": "row",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        dep_iata,
                        COUNT(*) as flights
                    FROM fct_flights
                    GROUP BY dep_iata
                    ORDER BY flights DESC
                    LIMIT 10
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {},
    })
    questions.append(q12)
    logger.info(f"Created question: {q12['name']}")

    # Question 13: Severe Delay Rate (KPI)
    q13 = client.create_card({
        "name": "Severe Delay Rate (%)",
        "display": "scalar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        ROUND(SUM(CASE WHEN delay_category = 'severe_delay' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as severe_pct
                    FROM fct_flights
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "scalar.suffix": "%",
        },
    })
    questions.append(q13)
    logger.info(f"Created question: {q13['name']}")

    # Question 14: Delay Heatmap (Hour x Day)
    q14 = client.create_card({
        "name": "Delay Heatmap by Hour & Day",
        "display": "table",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        day_of_week_name as Day,
                        dep_hour as Hour,
                        avg_delay_min as "Avg Delay (min)",
                        total_flights as Flights,
                        delayed_pct as "Delayed %"
                    FROM delay_heatmap
                    ORDER BY day_of_week, dep_hour
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "table.pivot": True,
            "table.pivot_column": "Hour",
            "table.cell_column": "Avg Delay (min)",
        },
    })
    questions.append(q14)
    logger.info(f"Created question: {q14['name']}")

    # Question 15: Delay Category Trend (Stacked Area)
    q15 = client.create_card({
        "name": "Delay Categories Over Time",
        "display": "area",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        flight_date,
                        delay_category,
                        COUNT(*) as count
                    FROM fct_flights
                    GROUP BY flight_date, delay_category
                    ORDER BY flight_date, delay_category
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "stackable.stack_type": "stacked",
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Flights",
        },
    })
    questions.append(q15)
    logger.info(f"Created question: {q15['name']}")

    # Question 16: Top Routes Table with Metrics
    q16 = client.create_card({
        "name": "Top Routes Performance Table",
        "display": "table",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        route as Route,
                        SUM(total_flights) as "Total Flights",
                        ROUND(AVG(avg_dep_delay_min), 1) as "Avg Delay (min)",
                        ROUND(100 - AVG(delayed_pct), 1) as "On-Time %",
                        ROUND(AVG(delayed_pct), 1) as "Delayed %"
                    FROM fct_route_daily
                    GROUP BY route
                    HAVING SUM(total_flights) >= 3
                    ORDER BY SUM(total_flights) DESC
                    LIMIT 15
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "table.column_formatting": [
                {"columns": ["On-Time %"], "type": "range", "colors": ["#EF8C8C", "#F9D45C", "#84BB4C"]},
                {"columns": ["Delayed %"], "type": "range", "colors": ["#84BB4C", "#F9D45C", "#EF8C8C"]},
            ],
        },
    })
    questions.append(q16)
    logger.info(f"Created question: {q16['name']}")

    # Question 17: Weekly Comparison
    q17 = client.create_card({
        "name": "Weekly Performance Trend",
        "display": "combo",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT 
                        week_start as Week,
                        total_flights as Flights,
                        avg_delay_min as "Avg Delay",
                        ontime_pct as "On-Time %"
                    FROM weekly_comparison
                    ORDER BY week_start
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Week",
            "series_settings": {
                "Flights": {"display": "bar"},
                "Avg Delay": {"display": "line"},
                "On-Time %": {"display": "line"},
            },
        },
    })
    questions.append(q17)
    logger.info(f"Created question: {q17['name']}")

    # Question 18: Most Delayed Route (KPI)
    q18 = client.create_card({
        "name": "Most Delayed Route",
        "display": "scalar",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": """
                    SELECT route
                    FROM fct_route_daily
                    GROUP BY route
                    HAVING SUM(total_flights) >= 3
                    ORDER BY AVG(delayed_pct) DESC
                    LIMIT 1
                """,
            },
            "database": database_id,
        },
        "visualization_settings": {},
    })
    questions.append(q18)
    logger.info(f"Created question: {q18['name']}")

    return questions


def cleanup_flight_analytics(client: MetabaseClient) -> None:
    """Remove all Flight Analytics dashboards and related questions."""
    # Define the names we created
    dashboard_name = "Flight Analytics Dashboard"
    question_names = [
        "Total Flights by Date",
        "Delay Category Distribution",
        "Average Delay by Time of Day",
        "Top 10 Routes by Delay Percentage",
        "Flights by Airline",
        "Daily Average Delay Trend",
        "Weekend vs Weekday Performance",
        "Total Flights",
        "On-Time Rate (%)",
        "Average Delay (min)",
        "Flights by Departure Hour",
        "Top 10 Departure Airports",
        "Severe Delay Rate (%)",
        "Delay Heatmap by Hour & Day",
        "Delay Categories Over Time",
        "Top Routes Performance Table",
        "Weekly Performance Trend",
        "Most Delayed Route",
    ]

    # Delete dashboards with matching name
    dashboards = client.get_all_dashboards()
    deleted_dashboards = 0
    for dashboard in dashboards:
        if dashboard.get("name") == dashboard_name:
            try:
                client.delete_dashboard(dashboard["id"])
                deleted_dashboards += 1
            except Exception as e:
                logger.warning(f"Failed to delete dashboard {dashboard['id']}: {e}")

    logger.info(f"Deleted {deleted_dashboards} dashboard(s)")

    # Delete cards/questions with matching names
    cards = client.get_all_cards()
    deleted_cards = 0
    for card in cards:
        if card.get("name") in question_names:
            try:
                client.delete_card(card["id"])
                deleted_cards += 1
            except Exception as e:
                logger.warning(f"Failed to delete card {card['id']}: {e}")

    logger.info(f"Deleted {deleted_cards} question(s)")


def create_dashboard_with_cards(
    client: MetabaseClient,
    questions: list[dict],
) -> dict:
    """Create a dashboard and add all questions to it."""
    dashboard = client.create_dashboard(
        name="Flight Analytics Dashboard",
        description="Professional flight performance analytics dashboard. Features KPIs, temporal analysis, route performance, and week-over-week trends. Data sourced from AviationStack API, processed with PySpark, and transformed using dbt.",
    )
    dashboard_id = dashboard["id"]
    logger.info(f"Created dashboard: {dashboard['name']} (ID: {dashboard_id})")

    # Dashboard layout configuration
    # Row 0: KPI cards (small)
    # Row 1-2: Main charts
    # Row 3+: Additional charts
    
    layout = [
        # ROW 0: KPIs (5 cards across the top)
        {"name": "Total Flights", "row": 0, "col": 0, "size_x": 3, "size_y": 3},
        {"name": "On-Time Rate (%)", "row": 0, "col": 3, "size_x": 3, "size_y": 3},
        {"name": "Average Delay (min)", "row": 0, "col": 6, "size_x": 2, "size_y": 3},
        {"name": "Severe Delay Rate (%)", "row": 0, "col": 8, "size_x": 2, "size_y": 3},
        {"name": "Most Delayed Route", "row": 0, "col": 10, "size_x": 2, "size_y": 3},
        
        # ROW 3: Main trend chart (full width)
        {"name": "Total Flights by Date", "row": 3, "col": 0, "size_x": 12, "size_y": 5},
        
        # ROW 8: Delay analysis
        {"name": "Delay Categories Over Time", "row": 8, "col": 0, "size_x": 8, "size_y": 5},
        {"name": "Delay Category Distribution", "row": 8, "col": 8, "size_x": 4, "size_y": 5},
        
        # ROW 13: Temporal analysis
        {"name": "Delay Heatmap by Hour & Day", "row": 13, "col": 0, "size_x": 6, "size_y": 6},
        {"name": "Flights by Departure Hour", "row": 13, "col": 6, "size_x": 6, "size_y": 6},
        
        # ROW 19: Weekly trends
        {"name": "Weekly Performance Trend", "row": 19, "col": 0, "size_x": 12, "size_y": 5},
        
        # ROW 24: Routes and Airlines
        {"name": "Top Routes Performance Table", "row": 24, "col": 0, "size_x": 6, "size_y": 6},
        {"name": "Flights by Airline", "row": 24, "col": 6, "size_x": 6, "size_y": 6},
        
        # ROW 30: Additional metrics
        {"name": "Top 10 Departure Airports", "row": 30, "col": 0, "size_x": 4, "size_y": 5},
        {"name": "Average Delay by Time of Day", "row": 30, "col": 4, "size_x": 4, "size_y": 5},
        {"name": "Weekend vs Weekday Performance", "row": 30, "col": 8, "size_x": 4, "size_y": 5},
        
        # ROW 35: Deep dive
        {"name": "Daily Average Delay Trend", "row": 35, "col": 0, "size_x": 6, "size_y": 5},
        {"name": "Top 10 Routes by Delay Percentage", "row": 35, "col": 6, "size_x": 6, "size_y": 5},
    ]

    # Map question names to IDs
    question_map = {q["name"]: q["id"] for q in questions}

    # Build all dashcards at once
    dashcards = []
    for idx, item in enumerate(layout):
        card_id = question_map.get(item["name"])
        if card_id:
            dashcards.append({
                "id": -(idx + 1),  # Negative IDs for new cards
                "card_id": card_id,
                "row": item["row"],
                "col": item["col"],
                "size_x": item["size_x"],
                "size_y": item["size_y"],
            })
            logger.info(f"Prepared '{item['name']}' for dashboard")
        else:
            logger.warning(f"Question '{item['name']}' not found")

    # Update dashboard with all cards at once
    if dashcards:
        client.update_dashboard_cards(dashboard_id, dashcards)
        logger.info(f"Added {len(dashcards)} cards to dashboard")

    return dashboard


def main():
    parser = argparse.ArgumentParser(
        description="Set up Metabase dashboard for flight analytics"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:3000",
        help="Metabase base URL (default: http://localhost:3000)",
    )
    parser.add_argument(
        "--email",
        required=True,
        help="Metabase admin email",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="Metabase admin password",
    )
    parser.add_argument(
        "--database-name",
        default="DuckDB Analytics",
        help="Name of the DuckDB database in Metabase",
    )
    parser.add_argument(
        "--duckdb-path",
        default="/data/analytics.duckdb",
        help="Path to DuckDB file (inside container)",
    )
    parser.add_argument(
        "--skip-db-setup",
        action="store_true",
        help="Skip database setup (use if already configured)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean up duplicate dashboards and questions before creating new ones",
    )

    args = parser.parse_args()

    try:
        # Connect to Metabase
        client = MetabaseClient(args.url, args.email, args.password)

        # Clean up duplicates if requested
        if args.clean:
            logger.info("Cleaning up existing Flight Analytics dashboards and questions...")
            cleanup_flight_analytics(client)

        # Check/setup database connection
        db = client.get_database_by_name(args.database_name)
        
        if db is None and not args.skip_db_setup:
            logger.info(f"Adding DuckDB database: {args.database_name}")
            db = client.add_duckdb_database(args.database_name, args.duckdb_path)
            logger.info("Waiting for database sync...")
            time.sleep(10)  # Wait for initial sync
            client.sync_database(db["id"])
            time.sleep(5)
        elif db is None:
            logger.error(
                f"Database '{args.database_name}' not found. "
                "Run without --skip-db-setup to create it."
            )
            sys.exit(1)
        else:
            logger.info(f"Using existing database: {args.database_name} (ID: {db['id']})")

        database_id = db["id"]

        # Verify tables exist
        fct_flights = client.get_table_by_name(database_id, "fct_flights")
        fct_route_daily = client.get_table_by_name(database_id, "fct_route_daily")

        if not fct_flights or not fct_route_daily:
            logger.warning(
                "Required tables (fct_flights, fct_route_daily) not found. "
                "Make sure dbt models have been run. Syncing database..."
            )
            client.sync_database(database_id)
            time.sleep(10)

        # Create questions
        logger.info("Creating saved questions...")
        questions = create_flight_questions(client, database_id)

        # Create dashboard
        logger.info("Creating dashboard...")
        dashboard = create_dashboard_with_cards(client, questions)

        logger.info("=" * 60)
        logger.info("Dashboard setup complete!")
        logger.info(f"View your dashboard at: {args.url}/dashboard/{dashboard['id']}")
        logger.info("=" * 60)

    except requests.exceptions.ConnectionError:
        logger.error(
            f"Could not connect to Metabase at {args.url}. "
            "Make sure Metabase is running."
        )
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
        logger.error(f"Response: {e.response.text if e.response else 'N/A'}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
