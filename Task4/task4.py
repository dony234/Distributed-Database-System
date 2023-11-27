
import psycopg2.extras
import traceback
from decimal import Decimal
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


def perform_distributed_transaction(conn):
    """Performs an ACID-compliant distributed transaction and prints the results."""
    with conn.cursor() as cursor:
        try:
            print("Distributed Transaction: \n")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)


            cursor.execute("SELECT patient_id FROM billing_insurance;")
            fetched_ids = [row['patient_id'] for row in cursor.fetchall()]
            # print(fetched_ids)
            if len(fetched_ids) < 2:
                raise ValueError("Insufficient unique patient IDs available for transaction.")

            # Randomly choose two distinct patient IDs
            donor_patient_id, receiver_patient_id = random.sample(fetched_ids, 2)

            # Remaining transaction logic
            cursor.execute("SELECT amount FROM billing_insurance WHERE patient_id = %s;", (donor_patient_id,))
            donor_balance_before = cursor.fetchone()['amount'] if cursor.rowcount > 0 else None
            if donor_balance_before is None:
                raise ValueError(f"No record found for donor patient ID {donor_patient_id}")
            print(f"Donor balance before transaction: {donor_balance_before}")

            cursor.execute("SELECT amount FROM billing_insurance WHERE patient_id = %s;", (receiver_patient_id,))
            receiver_balance_before = cursor.fetchone()['amount'] if cursor.rowcount > 0 else None
            if receiver_balance_before is None:
                raise ValueError(f"No record found for receiver patient ID {receiver_patient_id}")
            print(f"Receiver balance before transaction: {receiver_balance_before}")

            cursor.execute("UPDATE billing_insurance SET amount = amount - 100 WHERE patient_id = %s;", (donor_patient_id,))
            cursor.execute("UPDATE billing_insurance SET amount = amount + 100 WHERE patient_id = %s;", (receiver_patient_id,))

            cursor.execute("SELECT amount FROM billing_insurance WHERE patient_id = %s;", (donor_patient_id,))
            donor_balance_after = cursor.fetchone()['amount']
            print(f"Donor balance after transaction: {donor_balance_after}")

            cursor.execute("SELECT amount FROM billing_insurance WHERE patient_id = %s;", (receiver_patient_id,))
            receiver_balance_after = cursor.fetchone()['amount']
            print(f"Receiver balance after transaction: {receiver_balance_after}")

            if donor_balance_after < 0:
                raise Exception('Insufficient funds for the transaction.')

            conn.commit()
            print("Transaction completed successfully.")
        except Exception as e:
            conn.rollback()
            print("Transaction failed.")
            traceback.print_exc()
        finally:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)




def perform_optimistic_concurrency_control(conn):
    """Performs an update with optimistic concurrency control."""
    with conn.cursor() as cursor:
        try:
            print("Optimistic Concurrency Control :\n")
            cursor.execute("SELECT patient_id FROM billing_insurance;")
            patient_ids = [row['patient_id'] for row in cursor.fetchall()]
            
            if not patient_ids:
                raise ValueError("No patient records found.")

            # Choose a random patient_id and update amount
            patient_id = random.choice(patient_ids)
            update_amount = random.randint(1, 500)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
            

            # Begin a transaction block
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='billing_insurance' AND column_name='version';
            """)
            if cursor.fetchone() is None:
                cursor.execute('ALTER TABLE billing_insurance ADD COLUMN version INT DEFAULT 1;')
                conn.commit()

            # Select the current amount and version for the patient
            cursor.execute("""
                SELECT amount, version FROM billing_insurance 
                WHERE patient_id = %s FOR UPDATE;
            """, (patient_id,))
            record = cursor.fetchone()
            
            # Check if a record was found
            if record is None:
                raise ValueError(f"No billing record found for patient_id" + patient_id)

            current_amount, current_version = record['amount'], record['version']

            current_amount = Decimal(current_amount)
            # Simulate some business logic and update calculations
            new_amount = current_amount + Decimal(update_amount)

            # Perform the update, including a check for the version
            cursor.execute("""
                UPDATE billing_insurance 
                SET amount = %s, version = version + 1 
                WHERE patient_id = %s AND version = %s;
            """, (new_amount, patient_id, current_version))

            # Check for row count to confirm the update happened
            if cursor.rowcount == 0:
                raise Exception('Concurrent update detected. Transaction aborted.')

            # Commit the transaction
            conn.commit()
            print(f"Amount for patient_id {patient_id} updated to: {new_amount}")
            print("Transaction completed successfully.")
        except Exception as e:
            conn.rollback()
            print("Transaction failed.")
            traceback.print_exc()
        finally:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)

def main():
    conn = connect_db()
    if conn:
        perform_distributed_transaction(conn)
        perform_optimistic_concurrency_control(conn)
        conn.close()


if __name__ == "__main__":
    main()
