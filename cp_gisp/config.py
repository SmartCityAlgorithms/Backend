import json
from time import strftime


def check_api_key(request_json):
    # Check secret API key
    real_secret_key = 'ABC123'
    get_sectret_key = request_json.get('SECRET_API_KEY', None)
    if get_sectret_key == real_secret_key:
        return False
    return "Wrong secret API key or doesn't exist"


def load_mysql_data(mysql_file, logger):
    try:
        with open(mysql_file, 'r') as json_file:
            mysql = json.load(json_file)
    except FileNotFoundError as e:
        dt = strftime("[%Y-%b-%d %H:%M:%S]")
        logger.warning(f'{dt} Exception: {str(e)}')
        return None
    return mysql


#App port
app_port = 5000

# MySQL
MYSQL = '../credentials/mysql.json'
jobs_table = 'report_jobs'
admin = 'rustam.i@bstd.ru'