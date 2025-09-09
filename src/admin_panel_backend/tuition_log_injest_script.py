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
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            student_parent_map = get_student_parent_map(cur)
            
            print(f"\nReading data from '{CSV_FILE_PATH}'...")
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    try:
                        attendee_names = [name.strip() for name in row['attendees'].split(',')]
                        
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
```

---
### ## Step 3: How to Run the Script

1.  **Save the Files:** Make sure your `tuition_logs.csv` and the new `ingest_logs.py` are both in the root directory of your `EfficientTutor-backend` project.
2.  **Install Dependencies:** Your `requirements.txt` should already contain `psycopg2-binary` and `python-dotenv`. If not, run `pip install psycopg2-binary python-dotenv`.
3.  **Run the Script:** Open your terminal in the project root and execute the script:
    ```bash
    python ingest_logs.py
    
