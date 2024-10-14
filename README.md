import os
import pymysql
import json
import logging
from datetime import datetime, timedelta

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# PostgreSQL connection settings
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")


def lambda_handler(event, context):
    conn = None
    try:
        # Connect to PostgreSQL database
        conn = pymysql.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
            
        )
        cursor = conn.cursor()

        # Process each record in the event
        for record in event['Records']:
            message_body = json.loads(record['body'])
            process_message(message_body, cursor)
        
        # Commit the transaction
        conn.commit()
        logger.info("Messages processed and committed successfully")

        return {
            'statusCode': 200,
            'body': json.dumps('Messages processed successfully')
        }

    except pymysql.Error as db_err:
        # Log database-related errors
        logger.error(f"Database error: {db_err}")
        if conn:
            conn.rollback()  # Rollback the transaction in case of error
        return {
            'statusCode': 500,
            'body': json.dumps(f"Database error: {db_err}")
        }

    except Exception as e:
        # Log any other errors
        logger.error(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"An error occurred: {e}")
        }

    finally:
        # Ensure the database connection is closed
        if conn:
            cursor.close()
            conn.close()
            logger.info("Database connection closed")

    logger.info("Done!")

def process_message(message, cursor):
    """Process the SQS message and run the necessary SQL operations."""

    # Extract message details
    username = message.get('username')
    course_id = message.get('course_id')
    mode = message.get('mode')
    total_lessons = message.get('total_number_of_lessons')
    lesson_number = message.get('lesson_number')
    lesson_name = message.get('lesson_name')
    lesson_grade = message.get('lesson_grade')
    overall_grade_percentage = message.get('overall_grade_percentage')
    overall_progress_percentage = message.get('overall_progress_percentage')
    lesson_started_timestamp = message.get('lesson_started_timestamp')
    lesson_completed_timestamp = message.get('lesson_completed_timestamp')

    logger.info(f"Course_id is {course_id}")

    # Fetch the course_student_mapping record from DB
    course_student_mapping = fetch_course_student_mapping(cursor, course_id, username)

    if course_student_mapping:
        # Update course_student_mapping based on the message
        logger.info("Student data found. Update is commented for now")
        logger.info(course_student_mapping)

        # update_course_student_mapping(
        #     cursor, course_student_mapping, mode, overall_grade_percentage,
        #     total_lessons, lesson_number, overall_progress_percentage,
        #     lesson_started_timestamp
        # )
        
        # Check if grade data exists and update or insert accordingly
        # manage_grade_data(
        #     cursor, course_student_mapping[0], lesson_name, lesson_grade, lesson_completed_timestamp
        # )
    else:
        logger.warning("Error: Student Data not found. *******")

def fetch_course_student_mapping(cursor, course_id, username):
    """Fetch course_student_mapping from the database based on course_id and username."""

    logger.info("Fetch course_student_mapping from the database based on course_id and username")

    select_query = f"""
    SELECT csm.* 
    FROM course_student_mapping csm
    LEFT JOIN xened_netsuite_course_mapping xncm ON xncm.id = csm.course_id
    LEFT JOIN users u ON u.id = csm.user_id
    WHERE xncm.xened_course_id = '{course_id}' AND u.sso_id = '{username}'
    """
    cursor.execute(select_query)

    return cursor.fetchone()

def update_course_student_mapping(cursor, course_student_mapping, mode, grade, total_lessons, lesson_number, progress, lesson_started_timestamp):
    """Update the course_student_mapping based on the message data."""

    update_query = f"""
    UPDATE course_student_mapping
    SET grade = {grade}, 
        total_lessons = {total_lessons}, 
        completed_till = {lesson_number}, 
        progress = {progress}
    """
    # Apply conditional logic based on 'mode'
    if mode == 'start':
        started_at = datetime.strptime(lesson_started_timestamp, "%Y-%m-%dT%H:%M")
        expiry_date = started_at + timedelta(days=365)
        update_query += f", status = 'In progress', started_at = '{started_at}', expiry_date = '{expiry_date}'"

    elif mode == 'complete':
        update_query += ", status = 'Completed'"

    update_query += f" WHERE id = {course_student_mapping[0]}"  # Assuming the first column is id
    
    cursor.execute(update_query)

def manage_grade_data(cursor, course_mapping_id, lesson_name, lesson_grade, lesson_completed_timestamp):
    """Check if grade data exists, and update or insert grade details."""

    check_grade_query = f"""
    SELECT * FROM course_grade_detail
    WHERE course_mapping_id = {course_mapping_id} AND lesson = '{lesson_name}'
    """
    cursor.execute(check_grade_query)

    grade_record = cursor.fetchone()

    # If grade exists, update if necessary; otherwise, insert a new record
    if grade_record:
        if grade_record['grade'] != lesson_grade:
            update_grade_query = f"""
            UPDATE course_grade_detail
            SET grade = {lesson_grade}
            WHERE course_mapping_id = {course_mapping_id} AND lesson = '{lesson_name}'
            """
            cursor.execute(update_grade_query)
    else:
        completed_at = datetime.strptime(lesson_completed_timestamp, "%Y-%m-%dT%H:%M")
        insert_grade_query = f"""
        INSERT INTO course_grade_detail (course_mapping_id, lesson, grade, completed_at)
        VALUES ({course_mapping_id}, '{lesson_name}', {lesson_grade}, '{completed_at}')
        """

        cursor.execute(insert_grade_query)
