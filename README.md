```
@import url(https://fonts.googleapis.com/css?family=Roboto:400,500,700,300,100);

body {
    /* background-color: #3e94ec; */
    font-family: "Roboto", helvetica, arial, sans-serif;

    text-rendering: optimizeLegibility;
}
article{
    background-color: #3e94ec;
}

.datatable {
    
    width: 95%;
    margin: 0 auto;
}

.datatable table {
    border-radius: 3px;
    border-collapse: collapse;

}
.datatable caption {
   
   
    padding: 10px;
}

.datatable th {
    color: #D5DDE5;
    background-color:#343a45;
    /* background: #1b1e24; */
    border-bottom: 4px solid #9ea7af;
    border-right: 1px solid #343a45;
    border-left: 1px solid #343a45;
    font-size: 18px;
    font-weight: 100;
    padding: 10px;
    text-align: left;
    text-shadow: 0 1px 1px rgba(0, 0, 0, 0.1);
    vertical-align: middle;
}

.datatable td {
    background: #FFFFFF;
    padding: 5px;
    text-align: left;
    vertical-align: middle;
    font-weight: 300;
    font-size: 16px;
    text-shadow: -1px -1px 1px rgba(0, 0, 0, 0.1);
    border-right: 1px solid #C1C3D1;
    border-left: 1px solid #C1C3D1;
    border-bottom: 1px solid #C1C3D1;;
    color: black;
}

.datatable tr {
    border-top: 1px solid #C1C3D1;
    border-bottom: 1px solid #C1C3D1;
}


.datatable tr:hover td {
    background: #4E5066;
    color: #FFFFFF;
    /* border-top: 1px solid #22262e; */
}

.datatable tr:first-child {
    border-top: none;
}

.datatable tr:last-child {
    border-bottom: none;
}

.datatable tr:nth-child(odd) td {
    background: #EBEBEB;
}

.datatable tr:nth-child(odd):hover td {
    background: #4E5066;
}

```

```
mysql://avnadmin:<password>@mysql-sqs-test-hari-gworks.h.aivencloud.com:28681/

defaultdb?ssl-mode=REQUIRED
required_fields = {
        'username': int,
        'course_id': str,
        'mode': str,
        'total_number_of_lessons': int,
        'lesson_number': int,
        'lesson_name': str,
        'lesson_grade': int,
        'overall_grade_percentage': int,
        'overall_progress_percentage': int,
        'lesson_started_timestamp': str,
        'lesson_completed_timestamp': str
    }
for field, field_type in required_fields.items():
            if field not in data:
                return f"Missing required field: {field}"
            if not isinstance(data[field], field_type):
                return f"Field {field} should be of type {field_type.__name__}"

def extract_data_from_message(message):
    try:
        # Parse the message (assuming it's a JSON string)
        data = json.loads(message)

        # Extract relevant fields from the parsed message
        username = data.get('username')
        course_id = data.get('course_id')
        mode = data.get('mode')
        total_number_of_lessons = data.get('total_number_of_lessons')
        lesson_number = data.get('lesson_number')
        lesson_name = data.get('lesson_name')
        lesson_grade = data.get('lesson_grade')
        overall_grade_percentage = data.get('overall_grade_percentage')
        overall_progress_percentage = data.get('overall_progress_percentage')
        lesson_started_timestamp = datetime.strptime(data.get('lesson_started_timestamp'), "%Y-%m-%dT%H:%M")
        lesson_completed_timestamp = datetime.strptime(data.get('lesson_completed_timestamp'), "%Y-%m-%dT%H:%M")

        # Prepare the data tuple for insertion
        extracted_data = (
            username,
            course_id,
            mode,
            total_number_of_lessons,
            lesson_number,
            lesson_name,
            lesson_grade,
            overall_grade_percentage,
            overall_progress_percentage,
            lesson_started_timestamp,
            lesson_completed_timestamp
        )

        return extracted_data

    except json.JSONDecodeError as json_error:
        print(f"Error decoding JSON: {json_error}")
    except Exception as e:
        print(f"Error extracting data from message: {e}")

    return None  # Return None if extraction fails
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

6f2A8dp-hE_SRFTH
        
        # Extract data from the message and insert it
        data = extract_data_from_message(message)  # Your function to extract data
        cursor.execute(insert_query, data)
```
