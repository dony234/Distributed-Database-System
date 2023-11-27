
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
        

def insert_random_data(conn):
    """Insert random data into tables"""
    cursor = conn.cursor()

    for _ in range(20):
        cursor.execute(
            """
            INSERT INTO doctors_info(name, specialization, contact_number, email, availability)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (fake.name(),
             fake.job()[:150],  # Truncate to 150 characters
             fake.phone_number()[:10],  # Truncate to 10 characters
             fake.email(),
             fake.day_of_week())
        )
    # Insert data into patient_records
    for _ in range(20):
        cursor.execute(
            """
            INSERT INTO patient_records (patient_name, date_of_birth, gender, address, contact_number, email, allergies)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (
                fake.name(),
                fake.date_of_birth(minimum_age=0, maximum_age=115),
                random.choice(['M', 'F']),
                fake.address()[:100],  # Ensure the address fits within the VARCHAR(100) column
                fake.phone_number()[:10],  # Ensure the contact number fits within the VARCHAR(15) column
                fake.email()[:50],  # Ensure the email fits within the VARCHAR(50) column
                fake.sentence()[:50]  # Ensure the allergies fit within the VARCHAR(50) column
            )
        )

    # Insert data into medications
    for _ in range(20):
        cursor.execute(f"INSERT INTO medications(name, dosage, manufacturer) VALUES (%s, %s, %s);",(fake.word(), f"{random.randint(1, 500)} mg", fake.company()))

    # Need to commit after inserting into tables that have foreign key dependencies
    conn.commit()

    # Retrieve IDs for foreign key relationships
    cursor.execute(f"SELECT doctor_id FROM doctors_info;")
    doctor_ids = [row['doctor_id'] for row in cursor.fetchall()]

    cursor.execute(f"SELECT patient_id FROM patient_records;")
    patient_ids = [row['patient_id'] for row in cursor.fetchall()]

    cursor.execute(f"SELECT medication_id FROM medications;")
    medication_ids = [row['medication_id'] for row in cursor.fetchall()]

    # Insert data into appointments, medical_history, billing_insurance, patient_medications
    for _ in range(20):
        # appointments
        cursor.execute(f"INSERT INTO appointments(patient_id, doctor_id, appointment_date, purpose) VALUES (%s, %s, %s, %s);",(random.choice(patient_ids), random.choice(doctor_ids), fake.date_time_this_month(), fake.sentence()))

        # medical_history
        cursor.execute(f"INSERT INTO medical_history(patient_id, diagnosis, treatment, date_of_visit) VALUES (%s, %s, %s, %s);", (random.choice(patient_ids), fake.sentence(), fake.sentence(), fake.date_time_this_month()))

        # billing_insurance
        cursor.execute(f"INSERT INTO billing_insurance(patient_id, amount, date, insurance_provider, insurance_policy_number) VALUES (%s, %s, %s, %s, %s);",(random.choice(patient_ids), round(random.uniform(100, 10000), 2), fake.date_this_year(), fake.company(), fake.bothify(text='????-########')))

        # patient_medications
    used_combinations = set()
    while len(used_combinations) < 20:
        patient_id_choice = random.choice(patient_ids)
        medication_id_choice = random.choice(medication_ids)
        if (patient_id_choice, medication_id_choice) not in used_combinations:
            try:
                cursor.execute(
                    """
                    INSERT INTO patient_medications(patient_id, medication_id)
                    VALUES (%s, %s);
                    """,
                    (patient_id_choice, medication_id_choice)
                )
                used_combinations.add((patient_id_choice, medication_id_choice))
            except psycopg2.errors.UniqueViolation:
                continue
            except psycopg2.DatabaseError as error:
                print(f"An error occurred: {error}")
                conn.rollback()
                break
    conn.commit()

def select_table_data(conn):
    """Prints the first five rows of all tables in the database"""
    cursor = conn.cursor()

    # List of all tables to print the first five rows from
    tables = ['doctors_info', 'patient_records', 'appointments', 'medical_history', 'medications', 'billing_insurance', 'patient_medications']

    for table in tables:
        print(f"First five rows from table {table}:")

        cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 5").format(sql.Identifier(table)))

        # Fetch all records from the cursor
        records = cursor.fetchall()

        for row in records:
            print(row)
        print("\n")  # Print a newline for readability between tables

    cursor.close()

def main():
    conn = connect_db()
    if conn:
        create_tables(conn)
        insert_random_data(conn)
        select_table_data(conn)
        conn.close()


if __name__ == "__main__":
    main()
