
import psycopg2.extras
import uuid
from faker import Faker
import psycopg2
from psycopg2 import sql
import random
from datetime import datetime, timedelta


DATABASE_NAME = 'healthcare'

fake = Faker()

DB_URL = "postgresql://shashank:z3L2HOT24J5yDJWdt3esqw@plain-koala-13452.5xj.cockroachlabs.cloud:26257/healthcare?sslmode=verify-full"

def connect_db():
    try:
        conn = psycopg2.connect(DB_URL, 
                                application_name="healthcare_app", 
                                cursor_factory=psycopg2.extras.RealDictCursor)
        print("Connected to the database.")
        return conn
    except Exception as e:
        print("Database connection failed.")
        print(e)
        return None


def optimize_queries(conn):
    """Optimizes queries for efficient data retrieval."""
    cursor = conn.cursor()

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patient_name ON patient_records(patient_name);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_doctor_specialization ON doctors_info(specialization);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_appointments_date ON appointments(appointment_date);")

    # Explain a query to see the execution plan
    cursor.execute("EXPLAIN SELECT * FROM patient_records WHERE patient_name = 'John Doe';")
    plan = cursor.fetchall()
    print("Execution plan for patient_records query:")
    for row in plan:
        print(row)

    # Optimize a query using JOIN and WHERE clauses
    cursor.execute("""
        EXPLAIN SELECT p.patient_name, a.appointment_date, d.name 
        FROM appointments a
        JOIN patient_records p ON a.patient_id = p.patient_id
        JOIN doctors_info d ON a.doctor_id = d.doctor_id
        WHERE p.patient_name = 'John Doe' AND a.appointment_date > NOW() - INTERVAL '1 year';
    """)
    plan = cursor.fetchall()
    print("\nExecution plan for appointments query:")
    for row in plan:
        print(row)

    # Ensure you commit the creation of indexes if outside of a transaction
    conn.commit()
    cursor.close()

    print("\nQuery optimization completed.\n")

def analyze_complex_queries(conn):
    with conn.cursor() as cursor:
        complex_query = """
        SELECT d.name, COUNT(a.appointment_id) AS appointment_count
        FROM doctors_info d
        JOIN appointments a ON d.doctor_id = a.doctor_id
        GROUP BY d.name
        ORDER BY appointment_count DESC;
        """

        # Execute and explain the complex query
        cursor.execute("EXPLAIN " + complex_query)
        explain_result = cursor.fetchall()
        print("Execution Plan for the Complex Query:")
        for row in explain_result:
            print(row)

        # Optionally, you can also use EXPLAIN ANALYZE for more detailed analysis
        cursor.execute("EXPLAIN ANALYZE " + complex_query)
        explain_analyze_result = cursor.fetchall()
        print("\nDetailed Execution Analysis for the Complex Query:")
        for row in explain_analyze_result:
            print(row)



def create_distributed_indexes(conn):
    """Creates distributed indexes on partitioned tables in a distributed environment."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_patient_records_male_gender ON patient_records_male(gender);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_patient_records_female_gender ON patient_records_female(gender);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_appointments_date ON appointments(appointment_date);
    """)


    query1 = "EXPLAIN SELECT * FROM patient_records_male WHERE gender = 'M';"
    cursor.execute(query1)
    plan1 = cursor.fetchall()
    for row in plan1:
        print("Example 1 :",row)

    query2 = "EXPLAIN SELECT pr.patient_name, pr.date_of_birth, pr.gender, a.appointment_date, a.purpose FROM appointments a JOIN patient_records_male pr ON a.patient_id = pr.patient_id WHERE pr.gender = 'M' AND a.appointment_date >= CURRENT_DATE - INTERVAL '1 year';"
    cursor.execute(query2)
    plan2 = cursor.fetchall()
    for row in plan2:
        print("Example 2 : ",row)

    conn.commit()
    cursor.close()

    print("Distributed indexes created.")

def main():
    conn = connect_db()
    if conn:
        # create_tables(conn)
        # insert_random_data(conn)
        # select_table_data(conn)
        # horizontal_fragmentation(conn)
        # vertical_fragmentation(conn)
        analyze_complex_queries(conn)
        optimize_queries(conn)
        create_distributed_indexes(conn)
        conn.close()


if __name__ == "__main__":
    main()
