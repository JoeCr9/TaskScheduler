import os
import time
from datetime import datetime, timedelta
import pyodbc
import logging
from config import SQL_DIR
from xml_handler import load_jobs, save_jobs, get_tag_value, ensure_tags

def run_sql_file(file_name):
    file_path = os.path.join(SQL_DIR, file_name)
    with open(file_path, 'r') as file:
        sql = file.read()
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=...;'
        'DATABASE=...;'
        'UID=...;'
        'PWD=...'
        )
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

def run_sql_file_with_retry(file_name, retries=3, base_delay=5):
    delay=base_delay
    for attempt in range(retries):
        try:
            run_sql_file(file_name)
            logging.info(f"Successfully ran: {file_name}")
            return True
        except Exception as e:
            logging.error(f"Error running {file_name}: {e} | Attempt {attempt+1}/{retries}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                return False


def check_and_return_jobs():
    tree, root = load_jobs()
    now = datetime.now()
    for job in root.findall("Job"):
        defaults = {
            "ScheduleType": "Daily",
            "RunTime": "12:00",
            "IntervalMinutes": "1",
            "RunDays": "Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday",
            "LastRunTime": "Never",
            "Status": "Pending"
        }
        ensure_tags(job, defaults)

        schedule_type = get_tag_value(job, "ScheduleType")
        file_name = get_tag_value(job, "FileName")
        last_run_str = get_tag_value(job, "LastRunTime")
        last_run_time = datetime.min if last_run_str == "Never" else datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")

        should_run = False

        if schedule_type == "Interval":
            interval = int(get_tag_value(job, "IntervalMinutes"))
            next_run = last_run_time + timedelta(minutes=interval)
            should_run = now >= next_run
        elif schedule_type == "Daily":
            run_time = get_tag_value(job, "RunTime")
            run_hour, run_minute = map(int, run_time.split(':'))
            scheduled_time = now.replace(hour=run_hour, minute=run_minute, second=0, microsecond=0)
            should_run = now >= scheduled_time and last_run_time < scheduled_time
        elif schedule_type == "Weekly":
            run_days = get_tag_value(job, "RunDays").split(',')
            today = now.strftime("%A")
            run_time = get_tag_value(job, "RunTime")
            run_hour, run_minute = map(int, run_time.split(":"))
            scheduled_time = now.replace(hour=run_hour, minute=run_minute, second=0, microsecond=0)
            should_run = today in run_days and now >= scheduled_time and last_run_time < scheduled_time

            if should_run:
                success = run_sql_file_with_retry(file_name)
                if success:
                    job.find("LastRunTime").text = now.strftime("%Y-%m-%d %H:%M:%S")
                    job.find("Status").text = "Completed"
                else:
                    job.find("Status").text = "Failed"
    save_jobs(tree)