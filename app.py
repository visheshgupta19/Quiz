from flask import Flask, render_template, request, jsonify
import sqlite3
import os
import tempfile
import re
from contextlib import contextmanager
import traceback

app = Flask(__name__)


# Create a temporary database from the SQL file
def create_database_from_sql():
    """Create an in-memory SQLite database from the SQL file"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row  # This enables column access by name

    # Read and execute the SQL file
    with open("all_codes.sql", "r") as f:
        sql_content = f.read()

    # Split by semicolon to get individual statements
    statements = sql_content.split(";")

    cursor = conn.cursor()
    for statement in statements:
        statement = statement.strip()
        if statement and not statement.startswith("--"):
            try:
                cursor.execute(statement)
            except sqlite3.Error as e:
                print(f"Error executing statement: {e}")
                print(f"Statement: {statement[:100]}...")

    conn.commit()
    return conn


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = create_database_from_sql()
    try:
        yield conn
    finally:
        conn.close()


def execute_user_query(query):
    """Execute user query and return results"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            print(f"Executing query: {query}")  # Debug log
            cursor.execute(query)

            # Check if it's a SELECT query (has results)
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                return {
                    "success": True,
                    "columns": columns,
                    "data": [dict(row) for row in results],
                    "row_count": len(
                        results
                    ),  # Use len(results) instead of cursor.rowcount
                }
            else:
                conn.commit()
                return {
                    "success": True,
                    "message": f"Query executed successfully. Rows affected: {cursor.rowcount}",
                    "row_count": cursor.rowcount,
                }
    except Exception as e:
        error_msg = f"SQL Error: {str(e)}"
        print(error_msg)  # Debug log
        print(traceback.format_exc())  # Full traceback
        return {"success": False, "error": error_msg}


def get_expected_result(question_id):
    """Get expected results for specific questions"""
    expected_queries = {
        "1": """
            SELECT sub.*
            FROM (
                SELECT 
                    t.*, 
                    ROW_NUMBER() OVER (PARTITION BY t.ProductID ORDER BY t.TransactionDate) AS rn
                FROM transactions t
            ) AS sub
            JOIN customers c ON sub.CustomerID = c.CustomerID
            JOIN products p ON sub.ProductID = p.ProductID
            WHERE rn = 3;
        """,
        "2": """
            SELECT 
                c.Region,
                SUM(t.TotalValue) as total_spending_region
            FROM transactions t 
            LEFT JOIN customers c ON c.CustomerID = t.CustomerID 
            GROUP BY c.Region
            HAVING SUM(t.TotalValue) > 300
            ORDER BY total_spending_region DESC
        """,
        "3": """
            SELECT p.ProductName FROM products p 
            WHERE NOT EXISTS (
                SELECT 1 FROM transactions t WHERE p.ProductID = t.ProductID
            )
        """,
        "4": """
            SELECT DISTINCT ProductID 
            FROM transactions 
            WHERE Price > (
                SELECT AVG(Price) FROM transactions
            )
        """,
    }

    if question_id in expected_queries:
        print(f"Getting expected result for question {question_id}")
        result = execute_user_query(expected_queries[question_id])
        print(f"Expected result: {result}")
        return result
    else:
        return {"success": False, "error": "Question not found"}


def compare_query_results(user_result, expected_result):
    """Compare user results with expected results"""
    if not user_result["success"]:
        return {
            "match": False,
            "message": f"User query failed: {user_result.get('error', 'Unknown error')}",
        }

    if not expected_result["success"]:
        return {
            "match": False,
            "message": f"Expected query failed: {expected_result.get('error', 'Unknown error')}",
        }

    # Check if both have data (for SELECT queries)
    if "data" in user_result and "data" in expected_result:
        user_data = user_result["data"]
        expected_data = expected_result["data"]

        # Check row count
        if len(user_data) != len(expected_data):
            return {
                "match": False,
                "message": f"Row count mismatch: User has {len(user_data)}, expected {len(expected_data)}",
            }

        # Check column count and names
        user_columns = set(user_result["columns"])
        expected_columns = set(expected_result["columns"])

        if user_columns != expected_columns:
            return {
                "match": False,
                "message": f"Column mismatch: User has {user_columns}, expected {expected_columns}",
            }

        # Check data content (simplified - for exact match)
        # Convert both datasets to comparable format
        try:
            user_data_sorted = sorted([tuple(sorted(row.items())) for row in user_data])
            expected_data_sorted = sorted(
                [tuple(sorted(row.items())) for row in expected_data]
            )

            if user_data_sorted != expected_data_sorted:
                return {
                    "match": False,
                    "message": "Data content does not match exactly",
                }

            return {"match": True, "message": "Perfect match!"}
        except Exception as e:
            return {"match": False, "message": f"Error comparing data: {str(e)}"}

    # For non-SELECT queries, check row counts
    elif user_result.get("row_count") == expected_result.get("row_count"):
        return {"match": True, "message": "Row counts match"}
    else:
        return {
            "match": False,
            "message": f'Row count mismatch: User affected {user_result.get("row_count")}, expected {expected_result.get("row_count")}',
        }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/execute_query", methods=["POST"])
def execute_query():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No JSON data received"})

        user_query = data.get("query", "").strip()
        print(f"Received query: {user_query}")  # Debug log

        if not user_query:
            return jsonify({"success": False, "error": "Query cannot be empty"})

        # Basic security check - prevent destructive operations in this demo
        destructive_keywords = [
            "DROP",
            "DELETE",
            "UPDATE",
            "INSERT",
            "ALTER",
            "CREATE",
            "TRUNCATE",
        ]
        if any(keyword in user_query.upper() for keyword in destructive_keywords):
            return jsonify(
                {
                    "success": False,
                    "error": "For security reasons, only SELECT queries are allowed in this demo",
                }
            )

        user_result = execute_user_query(user_query)
        print(f"User result: {user_result}")  # Debug log

        # If we have a current question, compare results
        comparison = None
        question_id = data.get("question_id")
        if question_id:
            print(f"Getting expected result for comparison with question {question_id}")
            expected_result = get_expected_result(question_id)
            comparison = compare_query_results(user_result, expected_result)
            print(f"Comparison result: {comparison}")  # Debug log

        return jsonify({"user_result": user_result, "comparison": comparison})

    except Exception as e:
        error_msg = f"Server error: {str(e)}"
        print(error_msg)
        print(traceback.format_exc())
        return jsonify({"success": False, "error": error_msg})


@app.route("/get_question/<question_id>")
def get_question(question_id):
    try:
        questions = {
            "1": {
                "question": "Write a SQL query to find the third transaction by date for each product.",
            },
            "2": {
                "question": "Find the total spending for each region and sort the results from highest to lowest, including only those regions with a total spending value greater than 300.",
            },
            "3": {"question": "Find the names of the products with zero transactions."},
            "4": {
                "question": "Find the unique product IDs that have an average price higher than the overall average price."
            },
        }

        if question_id in questions:
            print(f"Loading question {question_id}")
            expected_result = get_expected_result(question_id)
            return jsonify(
                {
                    "question": questions[question_id]["question"],
                    "expected_result": expected_result,
                }
            )
        else:
            return jsonify({"error": "Question not found"})
    except Exception as e:
        error_msg = f"Error loading question: {str(e)}"
        print(error_msg)
        print(traceback.format_exc())
        return jsonify({"error": error_msg})


import os

if __name__ == "__main__":
    # Use Render's assigned port or fallback to 5000
    # port = int(os.environ.get("PORT", 5000))
    # app.run(host="0.0.0.0", port=port, debug=True)
    app.run(debug=True, port=5003)
