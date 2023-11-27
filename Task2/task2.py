
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

def horizontal_fragmentation(conn):
    """Performs horizontal fragmentation on the patient_records table based on gender."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patient_records_male AS
        SELECT * FROM patient_records WHERE gender = 'M';
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patient_records_female AS
        SELECT * FROM patient_records WHERE gender = 'F';
    """)

    # Insert data into fragment tables
    cursor.execute("""
        INSERT INTO patient_records_male
        SELECT * FROM patient_records WHERE gender = 'M';
    """)
    cursor.execute("""
        INSERT INTO patient_records_female
        SELECT * FROM patient_records WHERE gender = 'F';
    """)
    tables = ['patient_records_male', 'patient_records_female']

    print("Horizontal fragmentation completed.\n")

    for table in tables:
        print(f"First five rows from table {table}:")

        cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 5").format(sql.Identifier(table)))

        records = cursor.fetchall()

        for row in records:
            print(row)
        print("\n") 
    conn.commit()
    cursor.close()

def vertical_fragmentation(conn):
    """Performs vertical fragmentation on the patient_records table."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patient_personal_info (
            patient_id INT PRIMARY KEY,
            patient_name VARCHAR(150) NOT NULL,
            date_of_birth DATE NOT NULL,
            gender VARCHAR(10) NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patient_contact_info (
            patient_id INT PRIMARY KEY,
            address VARCHAR(100) NOT NULL,
            contact_number VARCHAR(15) NOT NULL UNIQUE,
            email VARCHAR(50) NOT NULL UNIQUE
        );
    """)

    cursor.execute("""
        INSERT INTO patient_personal_info (patient_id, patient_name, date_of_birth, gender)
        SELECT patient_id, patient_name, date_of_birth, gender FROM patient_records
        ON CONFLICT (patient_id) DO NOTHING;
    """)
    cursor.execute("""
        INSERT INTO patient_contact_info (patient_id, address, contact_number, email)
        SELECT patient_id, address, contact_number, email FROM patient_records
        ON CONFLICT (patient_id) DO NOTHING;
    """)
    print("Vertical fragmentation completed.\n")
    
    tables = ['patient_personal_info', 'patient_contact_info']

    for table in tables:
        print(f"First five rows from table {table}:")

        cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 5").format(sql.Identifier(table)))

        records = cursor.fetchall()

        for row in records:
            print(row)
        print("\n")
    conn.commit()
    cursor.close()

def main():
    conn = connect_db()
    if conn:
        horizontal_fragmentation(conn)
        vertical_fragmentation(conn)
        conn.close()


if __name__ == "__main__":
    main()
