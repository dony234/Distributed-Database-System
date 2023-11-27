from pymongo import MongoClient
from faker import Faker
import random


def connect_to_mongo():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.healthcare
    return db

def insert_random_data(db):
    faker = Faker()

    # Insert into patients collection
    patients = db.patients
    for _ in range(20):
        patient = {
            "name": faker.name(),
            "date_of_birth": faker.date_of_birth().isoformat(),
            "gender": random.choice(["M", "F"]),
            "address": faker.address(),
            "contact_number": faker.phone_number(),
            "email": faker.email()
        }
        patients.insert_one(patient)

    # Insert into doctors collection
    doctors = db.doctors
    for _ in range(20):
        doctor = {
            "name": faker.name(),
            "specialization": faker.job(),
            "contact_number": faker.phone_number(),
            "email": faker.email(),
            "availability": faker.day_of_week()
        }
        doctors.insert_one(doctor)

    # Insert into appointments collection
    appointments = db.appointments
    patient_ids = [p["_id"] for p in patients.find()]
    doctor_ids = [d["_id"] for d in doctors.find()]
    for _ in range(20):
        appointment = {
            "patient_id": random.choice(patient_ids),
            "doctor_id": random.choice(doctor_ids),
            "appointment_date": faker.date_time_this_year().isoformat(),
            "purpose": faker.sentence()
        }
        appointments.insert_one(appointment)

def display_data(db):
    print("Patients:")
    for patient in db.patients.find().limit(5):
        print(patient)

    print("\nDoctors:")
    for doctor in db.doctors.find().limit(5):
        print(doctor)

    print("\nAppointments:")
    for appointment in db.appointments.find().limit(5):
        print(appointment)

def update_query(db):
    # Fetching and randomly selecting a patient
    patient_ids = [p["_id"] for p in db.patients.find()]
    patient_id = random.choice(patient_ids)
    new_contact_number = "123-456-7890"
    print(f"Updating this ID's contact number with 123-456-7890:",patient_id)

    # Updating the patient's contact number
    result1 = db.patients.update_one({"_id": patient_id}, {"$set": {"contact_number": new_contact_number}})

    # Fetching and randomly selecting a doctor
    doctor_ids = [d["_id"] for d in db.doctors.find()]
    doctor_id = random.choice(doctor_ids)
    print(f"Updating this ID's availability with Saturday:",doctor_id)

    new_availability = "Saturday" 

    # Updating the doctor's availability
    result2 = db.doctors.update_one({"_id": doctor_id}, {"$set": {"availability": new_availability}})

    return result1.modified_count, result2.modified_count


def delete_query(db):
    # Fetching and randomly selecting a patient
    patient_ids = [p["_id"] for p in db.patients.find()]
    if patient_ids:
        some_patient_id = random.choice(patient_ids)
        print(f"Deleting patient_id :",some_patient_id)
        deleted_count = db.patients.delete_one({"_id": some_patient_id}).deleted_count
        print(f"Deleted {deleted_count} patient(s).")

    # Fetching and randomly selecting an appointment
    appointment_ids = [a["_id"] for a in db.appointments.find()]
    if appointment_ids:
        some_appointment_id = random.choice(appointment_ids)
        print(f"Deleting some_appointment_id :",some_appointment_id)
        deleted_count = db.appointments.delete_one({"_id": some_appointment_id}).deleted_count
        print(f"Deleted {deleted_count} appointment(s).")


def main():
    db = connect_to_mongo()
    insert_random_data(db)
    display_data(db)
    update_result = update_query(db)
    print(f"Updated {update_result[0]} patient(s) and {update_result[1]} doctor(s).")
    delete_query(db)
    display_data(db)

if __name__ == "__main__":
    main()
