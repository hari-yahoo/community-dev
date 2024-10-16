```
        # Create the logging table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS message_logs (
                id SERIAL PRIMARY KEY,
                username INT,
                course_id VARCHAR(255),
                mode VARCHAR(50),
                total_number_of_lessons INT,
                lesson_number INT,
                lesson_name VARCHAR(255),
                lesson_grade INT,
                overall_grade_percentage INT,
                overall_progress_percentage INT,
                lesson_started_timestamp TIMESTAMP,
                lesson_completed_timestamp TIMESTAMP,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Prepare the insert query
        insert_query = sql.SQL("""
            INSERT INTO message_logs (username, course_id, mode, total_number_of_lessons, lesson_number, 
                                       lesson_name, lesson_grade, overall_grade_percentage, 
                                       overall_progress_percentage, lesson_started_timestamp, 
                                       lesson_completed_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        # Extract data from the message and insert it
        data = extract_data_from_message(message)  # Your function to extract data
        cursor.execute(insert_query, data)
```
