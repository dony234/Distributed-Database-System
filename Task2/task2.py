
import psycopg2.extras
import uuid
from faker import Faker
import psycopg2
from psycopg2 import sql
import random
import time
from datetime import datetime, timedelta


DATABASE_NAME = 'healthcare'
DATABASE_NAME2='postgres'

fake = Faker()

DB_URL = "postgresql://shashank:z3L2HOT24J5yDJWdt3esqw@plain-koala-13452.5xj.cockroachlabs.cloud:26257/healthcare?sslmode=verify-full"
DB_URL2 = "postgresql://shashank:z3L2HOT24J5yDJWdt3esqw@plain-koala-13452.5xj.cockroachlabs.cloud:26257/postgres?sslmode=verify-full"

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
def connect_db2():
    try:
        conn = psycopg2.connect(DB_URL2, 
                                application_name="postgres", 
                                cursor_factory=psycopg2.extras.RealDictCursor)
        print("Connected to the database.")
        return conn
    except Exception as e:
        print("Database connection failed.")
        print(e)
        return None
def create_tables(conn):
    with conn.cursor() as cur:
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS doctors_info (
                doctor_id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                specialization VARCHAR(50) NOT NULL,
                contact_number VARCHAR(10) NOT NULL,
                email VARCHAR(50) NOT NULL UNIQUE,
                availability VARCHAR(50) NOT NULL
                    
            );
            CREATE TABLE IF NOT EXISTS patient_records (
                patient_id SERIAL PRIMARY KEY,
                patient_name VARCHAR(50) NOT NULL,
                date_of_birth DATE NOT NULL,
                gender VARCHAR(10) NOT NULL,
                address VARCHAR(100) NOT NULL,
                contact_number VARCHAR(10) NOT NULL UNIQUE,
                email VARCHAR(50) NOT NULL UNIQUE,
                allergies VARCHAR(50)
            );
            CREATE TABLE IF NOT EXISTS appointments (
                appointment_id SERIAL PRIMARY KEY,
                patient_id INT NOT NULL,
                doctor_id INT NOT NULL,
                appointment_date TIMESTAMP NOT NULL,
                purpose VARCHAR(100) NOT NULL,
                FOREIGN KEY (patient_id) REFERENCES patient_records (patient_id),
                FOREIGN KEY (doctor_id) REFERENCES doctors_info (doctor_id)
            );
            CREATE TABLE IF NOT EXISTS medical_history (
                history_id SERIAL PRIMARY KEY,
                patient_id INT NOT NULL,
                diagnosis VARCHAR(200) NOT NULL,
                treatment VARCHAR(200) NOT NULL,
                date_of_visit TIMESTAMP NOT NULL,
                FOREIGN KEY (patient_id) REFERENCES patient_records (patient_id)
            );
            CREATE TABLE IF NOT EXISTS medications (
                medication_id SERIAL PRIMARY KEY,
                name VARCHAR(20) NOT NULL,
                dosage VARCHAR(20) NOT NULL,
                manufacturer VARCHAR(100) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS billing_insurance (
                billing_id SERIAL PRIMARY KEY,
                patient_id INT NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                date DATE NOT NULL,
                insurance_provider VARCHAR(100) NOT NULL,
                insurance_policy_number VARCHAR(50) NOT NULL,
                FOREIGN KEY (patient_id) REFERENCES patient_records (patient_id)
            );
            CREATE TABLE IF NOT EXISTS patient_medications (
                patient_id INT NOT NULL,
                medication_id INT NOT NULL,
                PRIMARY KEY (patient_id, medication_id),
                FOREIGN KEY (patient_id) REFERENCES patient_records (patient_id),
                FOREIGN KEY (medication_id) REFERENCES medications (medication_id)
            );        
        ''')
        
        
        # cur.execute('''
        #             DROP TABLE IF EXISTS appointments;
        #             DROP TABLE IF EXISTS medical_history;
        #             DROP TABLE IF EXISTS billing_insurance;
        #             DROP TABLE IF EXISTS patient_medications;
        #         ''')

        # cur.execute('''
        #             DROP TABLE IF EXISTS doctors_info;
        #             DROP TABLE IF EXISTS patient_records;
        #             DROP TABLE IF EXISTS medications;
        #             DROP TABLE IF EXISTS patient_contact_info;
        #             DROP TABLE IF EXISTS patient_personal_info;
        #             DROP TABLE IF EXISTS patient_records_female;
        #             DROP TABLE IF EXISTS patient_records_male;
        #             ''')

        print("Tables created successfully.")
        conn.commit()


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

def replicate_data(master_conn, slave_conn, table_name):
    master_cursor = master_conn.cursor()
    slave_cursor = slave_conn.cursor()

    last_replication_time = datetime.now() - timedelta(days=1)


    while True:
        master_cursor.execute(sql.SQL("SELECT * FROM {} WHERE last_modified > %s;").format(sql.Identifier(table_name)), (last_replication_time,))
        changes = master_cursor.fetchall()

        for change in changes:
            columns = list(change.keys())
            values = [change[col] for col in columns]

            placeholders = sql.SQL(', ').join(sql.Placeholder() * len(values))
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (patient_id) DO UPDATE SET ").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders
            )

            update_assignment = sql.SQL(', ').join([
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns if col != 'patient_id'
            ])
            
            final_query = insert_query + update_assignment

            slave_cursor.execute(final_query, values)

        slave_conn.commit()
        time.sleep(60)
        last_replication_time = datetime.now()

    master_cursor.close()
    slave_cursor.close()


def main():
    conn = connect_db()
    conn1 = connect_db2()
    if conn and conn1:
        create_tables(conn1)
        horizontal_fragmentation(conn)
        vertical_fragmentation(conn)
        # replicate_data(conn, conn1, 'patient_records')
        conn.close()


if __name__ == "__main__":
    main()
