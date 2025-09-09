'''
How to Run the Script
This is the most important step. In your Excel file, make sure your columns have the following headers (exactly) and save it as a file named tuition_logs.csv.

Required Columns:

    date (in YYYY-MM-DD format, e.g., 2025-09-08)

    start_time (in 24-hour HH:MM format, e.g., 17:00)

    end_time (in 24-hour HH:MM format, e.g., 18:30)

    subject (must exactly match one of your database ENUMs: 'Math', 'Physics', etc.)

    attendees (A comma-separated list of student first names, e.g., John or John,Jane)

    cost_per_hour (A number, e.g., 50 or 50.00)

    lesson_index (Optional: The lesson number, e.g., 1)

Example tuition_logs.csv file:
Code snippet

date,start_time,end_time,subject,attendees,cost_per_hour,lesson_index
2025-09-04,10:00,11:30,Math,"Mila",60.00,1
2025-09-01,19:00,20:00,Physics,"Mila,Omran",60.00,1
2025-09-02,17:00,18:00,Chemistry,"Abdullah",55.00,1

Place this tuition_logs.csv file in the root directory of your EfficientTutor-backend project.
'''
import os
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# --- CONFIGURATION ---
# Load the DATABASE_URL from your .env file
load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')
# The name of your CSV file
CSV_FILE_PATH = 'tuition_logs.csv'

def get_db_log_count(cur):
    """Gets the current number of rows in the tuition_logs table."""
    cur.execute("SELECT COUNT(*) AS count FROM tuition_logs;")
    return cur.fetchone()['count']

def get_student_parent_map(cur):
    """
    Fetches all students and creates a mapping of
    { 'FirstName': ('student_id', 'parent_user_id') }
    This is crucial for looking up the required IDs.
    """
    print("Fetching student and parent IDs from the database...")
    cur.execute("SELECT id, user_id, first_name FROM students;")
    student_map = {}
    for row in cur.fetchall():
        # This assumes first names are unique. If not, you may need to adjust.
        student_map[row['first_name']] = (str(row['id']), str(row['user_id']))
    print(f"Found {len(student_map)} students in the database.")
    return student_map

def main():
    """
    Main function to read the CSV and insert logs into the database.
    """
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found. Make sure it's in your .env file.")
        return

    try:
        # --- Confirmation Step ---
        print("!!! WARNING: This script will completely erase all data in the 'tuition_logs' table.")
        confirm = input("Are you sure you want to continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Operation cancelled by user.")
            return

        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # Deleting all tuition logs
            print("\nErasing all existing logs from the database...")
            cur.execute("DELETE FROM tuition_logs;")
            print("Table 'tuition_logs' cleared.")

            # reuploading the csv
            student_parent_map = get_student_parent_map(cur)
            
            print(f"\nReading data from '{CSV_FILE_PATH}'...")
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    try:
                        # THE FIX: Sanitize each name to remove whitespace AND curly quotes.
                        attendee_names = [name.strip().strip('“”') for name in row['attendees'].split(',')]
                        
                        print(attendee_names)
                        
                        # Find the parent ID. We assume all students in a group have the same parent.
                        first_student_name = attendee_names[0]
                        if first_student_name not in student_parent_map:
                            print(f"  - WARNING: Skipping row. Student '{first_student_name}' not found in the database.")
                            continue
                        
                        _, parent_user_id = student_parent_map[first_student_name]

                        # Format data for the database query
                        start_time = f"{row['date']} {row['start_time']}"
                        end_time = f"{row['date']} {row['end_time']}"
                        lesson_index = int(row['lesson_index']) if row.get('lesson_index') else None

                        print(f"  - Inserting log for {row['subject']} on {row['date']} for {attendee_names}...")
                        
                        cur.execute(
                            """
                            INSERT INTO tuition_logs 
                            (parent_user_id, subject, attendee_names, lesson_index, cost_per_hour, start_time, end_time)
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                parent_user_id,
                                row['subject'],
                                attendee_names,
                                lesson_index,
                                float(row['cost_per_hour']),
                                start_time,
                                end_time
                            )
                        )
                    except (KeyError, ValueError) as e:
                        print(f"  - WARNING: Skipping invalid row: {row}. Reason: {e}")
                        continue

            conn.commit()
            print("\nSUCCESS: All logs have been successfully inserted into the database.")

    except psycopg2.Error as e:
        print(f"\nDATABASE ERROR: {e}")
        if 'conn' in locals():
            conn.rollback()
    except FileNotFoundError:
        print(f"\nFILE ERROR: Could not find the file '{CSV_FILE_PATH}'. Make sure it's in the same directory.")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()

