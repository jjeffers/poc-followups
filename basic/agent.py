import os
from google.adk.agents import Agent
from google.adk.tools import FunctionTool
import sqlite3
import sqlglot
from sqlglot import exp
from datetime import datetime

DATABASE_FILE = "crm.db"



def _get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    return conn

def initialize_db():
    """Initializes the database with necessary tables if they don't exist."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            direction TEXT NOT NULL, -- e.g., 'inbound', 'outbound'
            content TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
    ''')
    conn.commit()
    conn.close()

def add_customer(name, email):
    """Adds a new customer to the database."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO customers (name, email) VALUES (?, ?)", (name, email))
        conn.commit()
        print(f"Customer '{name}' added successfully.")
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        print(f"Error: Customer with email '{email}' already exists.")
        return None
    finally:
        conn.close()

import os
import sqlite3
from datetime import datetime
import google.generativeai as genai
from google.generativeai.types import FunctionDeclaration

# --- Database Interaction Functions (MODIFIED) ---
DATABASE_FILE = "crm.db"

def _get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    return conn

def initialize_db():
    """Initializes the database with necessary tables if they don't exist."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            direction TEXT NOT NULL, -- e.g., 'inbound', 'outbound'
            content TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
    ''')
    conn.commit()
    conn.close()

def add_customer(name: str, email: str) -> int | None:
    """Adds a new customer to the CRM database. Returns the new customer_id or None if email exists."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO customers (name, email) VALUES (?, ?)", (name, email))
        conn.commit()
        # print(f"Customer '{name}' added successfully.") # Removed for cleaner output in tool call
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        # print(f"Error: Customer with email '{email}' already exists.") # Removed for cleaner output
        return None
    finally:
        conn.close()

# MODIFIED: Split get_customer_details into two distinct functions
def get_customer_details_by_id(customer_id: int) -> dict | None:
    """
    Retrieves customer details by customer ID.
    Returns a dictionary of customer details (customer_id, name, email) or None if not found.
    """
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id, name, email FROM customers WHERE customer_id = ?", (customer_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_customer_details_by_email(email: str) -> dict | None:
    """
    Retrieves customer details by email.
    Returns a dictionary of customer details (customer_id, name, email) or None if not found.
    """
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id, name, email FROM customers WHERE email = ?", (email,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def log_message(customer_id: int, direction: str, content: str):
    """
    Logs a message associated with a customer.
    Direction can be 'inbound' or 'outbound'.
    """
    conn = _get_db_connection()
    cursor = conn.cursor()
    timestamp = datetime.now().isoformat()
    try:
        cursor.execute(
            "INSERT INTO messages (customer_id, timestamp, direction, content) VALUES (?, ?, ?, ?)",
            (customer_id, timestamp, direction, content)
        )
        conn.commit()
        print(f"Message logged for customer_id {customer_id}.")
        return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"Error logging message: {e}")
        return None
    finally:
        conn.close()

def get_customer_messages(customer_id):
    """
    Retrieves all messages for a given customer_id, ordered by timestamp.
    Returns a list of dictionaries, each representing a message.
    """
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM messages WHERE customer_id = ? ORDER BY timestamp ASC",
        (customer_id,)
    )
    messages = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return messages

ALLOWED_COLUMNS = {"message_id","customer_id","direction", "timestamp", "content", "name", "email"}
ALLOWED_TABLES = {"messages","customers"}

def query_table(table: str, sql: str) -> list[dict]:
    """
    Execute a validated SELECT over the customers or messages tables in the local CRM.

    The argument `sql` must be a **complete SQL SELECT statement** string.
    Example:
        SELECT COUNT(*) AS total FROM customers
    or:
        SELECT name, phone FROM customers WHERE phone LIKE '%7'

    Only SELECT queries on the 'customers' or 'messages' table are allowed.
    """
    
    # Parse & validate
    try:
        expr = sqlglot.parse_one(sql, read="sqlite")
    except Exception:
        raise ValueError("Invalid SQL syntax.")

    # Must be a single SELECT statement
    if not isinstance(expr, exp.Select):
        raise ValueError("Only a single SELECT statement is allowed.")

    # Validate table(s)
    tables = {t.name for t in expr.find_all(exp.Table)}
    if not tables:
        raise ValueError("Query must reference the allowed table.")
    if tables - ALLOWED_TABLES:
        raise ValueError(f"Only tables {sorted(ALLOWED_TABLES)} are allowed.")

    # Validate selected columns (allow aggregates and *)
    # - If there are plain Column nodes, they must be from the allowed set.
    # - Star (*) is allowed.
    stars = list(expr.find_all(exp.Star))
    cols = {c.name for c in expr.find_all(exp.Column) if c.name}  # ignore None names
    if cols and not cols.issubset(ALLOWED_COLUMNS):
        raise ValueError(f"Only columns {sorted(ALLOWED_COLUMNS)} are allowed.")

    # Ensure a LIMIT
    if not list(expr.find_all(exp.Limit)):
        expr = expr.limit(100)
        
    # Re-render to safe SQL for SQLite
    safe_sql = expr.sql(dialect="sqlite")

    # Execute with clear error reporting
    try:
        conn = sqlite3.connect("crm.db")
        cur = conn.cursor()
        cur.execute(safe_sql)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
    except sqlite3.OperationalError as e:
        raise ValueError(f"SQLite error: {e}. SQL was: {safe_sql}") from e
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return [dict(zip(headers, r)) for r in rows]

def get_customers_with_no_messages():
    """
    Retrieves a list of customers who have no messages logged.
    Returns a dictionary of a status message ("success" or "error") and a list of dictionaries, each representing a customer if successful.
    """
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT c.*
        FROM customers c
        LEFT JOIN messages m ON c.customer_id = m.customer_id
        WHERE m.message_id IS NULL
    ''')
    customers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {'status': "success", 'results': customers}

def get_customers_with_last_message_before(datetime_threshold_str: str):
    """
    Retrieves customers whose last message was before the specified datetime_threshold.
    The datetime_threshold should be a string object that can be parsed as a datetime value.
    Returns a list of dictionaries, each representing a customer.
    """
    conn = _get_db_connection()
    cursor = conn.cursor()

    try:
        # Parse the string into a datetime object
        threshold = datetime.fromisoformat(datetime_threshold_str)
    except ValueError:
        raise ValueError(f"Invalid datetime format: {datetime_threshold_str}. Expected ISO 8601 format.")


    cursor.execute('''
        SELECT c.*
        FROM customers c
        JOIN (
            SELECT customer_id, MAX(timestamp) AS last_message_time
            FROM messages
            GROUP BY customer_id
        ) AS latest_messages ON c.customer_id = latest_messages.customer_id
        WHERE latest_messages.last_message_time < ?
    ''', (datetime_threshold_str,))
    customers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return customers



INSTRUCTION = """
You are an AI agent managing automated messaging sequences to ensure no lead is missed from a CMS database.
The business is a lawn care company in the southeast United States.
I want you to automatically manage and send follow-up sequences, so that no lead falls through the cracks and I can prevent lost revenue opportunities.
"""
initialize_db()


root_agent = Agent(
    name = "basic_agent",
    model = 'gemini-2.5-flash',
    description = "Agent that handles automated messaging sequences.",
    instruction = INSTRUCTION,
    tools=[
        query_table,
        log_message
    ]
)

print("Agent initialized with CRM tools!")