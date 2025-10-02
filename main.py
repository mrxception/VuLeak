import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import subprocess
import json
import datetime
import time
import re
import itertools
import threading

RESET = '\033[0m'
GRAY = '\033[90m'
CYAN = '\033[96m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
GREEN = '\033[92m'
AI_API_KEY = ''
AI_API_URL = ''

def log(level, message, secret_key=None):
    now = datetime.datetime.now().strftime('%H:%M:%S')
    if level == 'INFO':
        level_color = CYAN
    elif level == 'ERROR':
        level_color = RED
    elif level == 'WARNING':
        level_color = YELLOW
    elif level == 'INPUT':
        level_color = BLUE
    elif level == 'SUCCESS':
        level_color = GREEN
    else:
        level_color = RESET
    if secret_key and isinstance(message, str):
        message = message.replace(secret_key, '[HIDDEN]')
    print(f"{RESET}[{GRAY}{now}{RESET}] {RESET}[{level_color}{level}{RESET}] -> {message}")

def show_loading(stop_event, message):
    spinner = itertools.cycle(['-', '/', '|', '\\'])
    while not stop_event.is_set():
        sys.stdout.write(f'\r[{next(spinner)}] {message}')
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\r' + ' ' * 50 + '\r')
    sys.stdout.flush()

def validate_hub(hub_url, hub_key):
    cmd = [
        'curl',
        f"{hub_url}/rest/v1/",
        '-H', f"apikey: {hub_key}",
        '-H', f"Authorization: Bearer {hub_key}"
    ]
    stop_event = threading.Event()
    loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Validating credentials...'))
    loading_thread.start()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        stop_event.set()
        loading_thread.join()
        log('SUCCESS', f'{hub_url} is valid.', hub_key)
        return True
    except subprocess.CalledProcessError as e:
        stop_event.set()
        loading_thread.join()
        log('ERROR', f'Invalid {hub_url} or key. Error: {e.stderr}', hub_key)
        return False

def fetch_creds_from_ai(vault_url, code, is_db=False, retries=3, delay=5):
    if is_db:
        prompt = (
            f"Analyze the following code and extract PostgreSQL database connection details. "
            f"Return only a JSON object like: "
            f"{{\"user\": \"postgres.user\", \"password\": \"pass\", \"host\": \"host\", \"port\": \"port\", \"name\": \"db\", \"sslmode\": \"require\"}} "
            f"(use actual values from the code). If no credentials are found, return an empty object: {{}}. "
            f"Do not include any other text.\n\nCode:\n{code}"
        )
    else:
        prompt = (
            f"Analyze the following code and extract the URL and key. "
            f"Return only a JSON object like: {{\"url\": \"https://example.datahub\", \"key\": \"eyJhb...\"}} "
            f"(use actual values from the code). If no credentials are found, return an empty object: {{}}. "
            f"Do not include any other text.\n\nCode:\n{code}"
        )
    
    api_url = f"{AI_API_URL}"
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "model": "deepseek/deepseek-v3-0324",
        "stream": False
    }
    
    stop_event = threading.Event()
    loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Fetching credentials from AI...'))
    loading_thread.start()
    for attempt in range(retries):
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            raw_content = result['choices'][0]['message']['content']
            cleaned_content = re.sub(r'^```json\n|\n```$', '', raw_content).strip()
            credentials = json.loads(cleaned_content)
            stop_event.set()
            loading_thread.join()
            if is_db:
                required_keys = ['user', 'password', 'host', 'port', 'name']
                if not all(credentials.get(key) for key in required_keys):
                    log('WARNING', 'Incomplete database credentials found in AI response.')
                    return None
                log('SUCCESS', f'Extracted database credentials for {credentials["host"]}')
                return (f"postgresql://{credentials['user']}:{credentials['password']}@"
                        f"{credentials['host']}:{credentials['port']}/{credentials['name']}?sslmode={credentials.get('sslmode', 'require')}")
            else:
                if not credentials.get('url') or not credentials.get('key'):
                    log('WARNING', 'No credentials found in AI response.')
                    return None, None
                log('SUCCESS', f'Extracted URL: {credentials["url"]}')
                return credentials['url'], credentials['key']
        except Exception as e:
            log('ERROR', f'AI analysis attempt {attempt + 1}/{retries} failed: {str(e)}')
            if attempt < retries - 1:
                log('INFO', f'Retrying in {delay} seconds...')
                time.sleep(delay)
    stop_event.set()
    loading_thread.join()
    log('ERROR', 'All AI attempts failed to fetch credentials.')
    return None if is_db else (None, None)

def fetch_tables_from_ai(vault_url, code, retries=3, delay=5):
    prompt = (
        f"Analyze the following code and extract possible data table names mentioned or used in it. "
        f"Return only a JSON object like: {{\"tables\": [\"users\", \"profiles\", \"posts\"]}} "
        f"(use actual table names from the code). Do not include any other text.\n\nCode:\n{code}"
    )
    
    api_url = f"{AI_API_URL}"
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "model": "deepseek/deepseek-v3-0324",
        "stream": False
    }
    
    stop_event = threading.Event()
    loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Fetching table names from AI...'))
    loading_thread.start()
    for attempt in range(retries):
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            raw_content = result['choices'][0]['message']['content']
            cleaned_content = re.sub(r'^```json\n|\n```$', '', raw_content).strip()
            tables_data = json.loads(cleaned_content)
            tables = tables_data.get('tables', [])
            stop_event.set()
            loading_thread.join()
            if not tables:
                log('WARNING', 'No tables found in AI response. Using fallback: users, profiles, posts')
                return ['users', 'profiles', 'posts']
            log('SUCCESS', f'Possible tables extracted: {", ".join(tables)}')
            return tables
        except Exception as e:
            log('ERROR', f'AI table fetch attempt {attempt + 1}/{retries} failed: {str(e)}')
            if attempt < retries - 1:
                log('INFO', f'Retrying in {delay} seconds...')
                time.sleep(delay)
    stop_event.set()
    loading_thread.join()
    log('ERROR', 'All AI attempts failed. Using fallback table list: users, profiles, posts')
    return ['users', 'profiles', 'posts']

def get_table_schema(hub_url, hub_key, table, schema):
    table_ref = f"{schema}.{table}" if schema != 'public' else table
    cmd = [
        'curl',
        f"{hub_url}/rest/v1/{table_ref}?select=*&limit=1",
        '-H', f"apikey: {hub_key}",
        '-H', f"Authorization: Bearer {hub_key}"
    ]
    if schema != 'public':
        cmd.append('-H')
        cmd.append(f"Accept-Profile: {schema}")
    stop_event = threading.Event()
    loading_thread = threading.Thread(target=show_loading, args=(stop_event, f'Fetching schema for "{table}"...'))
    loading_thread.start()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        stop_event.set()
        loading_thread.join()
        try:
            data = json.loads(result.stdout)
            if data and isinstance(data, list) and len(data) > 0:
                columns = list(data[0].keys())
                log('SUCCESS', f'Inferred schema for "{table}": {", ".join(columns)}')
                return {col: {} for col in columns}
            log('WARNING', f'No data available to infer schema for "{table}". Raw response: {result.stdout}')
            return {}
        except json.JSONDecodeError:
            stop_event.set()
            loading_thread.join()
            log('WARNING', f'Invalid schema response for "{table}". Raw response: {result.stdout}')
            return {}
    except subprocess.CalledProcessError as e:
        stop_event.set()
        loading_thread.join()
        log('WARNING', f'Failed to fetch schema for "{table}": {e.stderr}', hub_key)
        return {}

def get_db_tables(conn):
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        cur.close()
        return [t["table_name"] for t in tables]
    except Exception as e:
        log('ERROR', f'Failed to fetch tables from database: {e}')
        return []

def get_db_schema(conn, table):
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        cols = cur.fetchall()
        cur.close()
        return {c['column_name']: {'data_type': c['data_type'], 'is_nullable': c['is_nullable']} for c in cols}
    except Exception as e:
        log('ERROR', f'Failed to fetch schema for "{table}": {e}')
        return {}

def execute_db_query(conn, table, query_str):
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if query_str:
            query = f"SELECT * FROM {table} WHERE {query_str};"
        else:
            query = f"SELECT * FROM {table};"
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        return [dict(row) for row in results]
    except Exception as e:
        log('ERROR', f'Query failed: {e}')
        return None

def display_db_all(conn):
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        log('INFO', 'Public tables:')
        for t in tables:
            print(f" - {t['table_name']}")
        
        log('INFO', '\nColumns for each table:')
        for t in tables:
            table = t['table_name']
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position;
            """, (table,))
            cols = cur.fetchall()
            print(f"\n{table}:")
            for c in cols:
                print(f"  {c['column_name']} ({c['data_type']}) nullable={c['is_nullable']}")
        
        log('INFO', '\nSample data from each table (first 5 rows):')
        for t in tables:
            table = t['table_name']
            print(f"\nTable: {table}")
            try:
                cur.execute(f"SELECT * FROM {table} LIMIT 5;")
                rows = cur.fetchall()
                for row in rows:
                    print(dict(row))
                if not rows:
                    print("  (no rows)")
            except Exception as e:
                print(f"  Error reading table {table}: {e}")
        cur.close()
    except Exception as e:
        log('ERROR', f'Failed to display all tables: {e}')

def display_rest_all(hub_url, hub_key, schema, tables):
    log('INFO', 'Public tables:')
    for table in tables:
        print(f" - {table}")
    
    log('INFO', '\nColumns for each table:')
    for table in tables:
        schema_data = get_table_schema(hub_url, hub_key, table, schema)
        print(f"\n{table}:")
        if schema_data:
            for col in schema_data.keys():
                print(f"  {col} (unknown type) nullable=unknown")
        else:
            print("  (no schema information available)")
    
    log('INFO', '\nSample data from each table (first 5 rows):')
    for table in tables:
        table_ref = f"{schema}.{table}" if schema != 'public' else table
        cmd = [
            'curl',
            f"{hub_url}/rest/v1/{table_ref}?select=*&limit=5",
            '-H', f"apikey: {hub_key}",
            '-H', f"Authorization: Bearer {hub_key}"
        ]
        if schema != 'public':
            cmd.append('-H')
            cmd.append(f"Accept-Profile: {schema}")
        print(f"\nTable: {table}")
        stop_event = threading.Event()
        loading_thread = threading.Thread(target=show_loading, args=(stop_event, f'Fetching data for "{table}"...'))
        loading_thread.start()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            stop_event.set()
            loading_thread.join()
            try:
                data = json.loads(result.stdout)
                if isinstance(data, list):
                    if not data:
                        print("  (no rows)")
                    for row in data:
                        print(row)
                else:
                    print(f"  Unexpected response: {result.stdout}")
            except json.JSONDecodeError:
                stop_event.set()
                loading_thread.join()
                print(f"  Invalid JSON response: {result.stdout}")
        except subprocess.CalledProcessError as e:
            stop_event.set()
            loading_thread.join()
            print(f"  Error reading table {table}: {e.stderr}")

def insert_db_data(conn, table, data):
    try:
        cur = conn.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s' for _ in data])
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
        cur.execute(query, list(data.values()))
        conn.commit()
        cur.close()
        log('SUCCESS', 'Data inserted successfully.')
        return True
    except Exception as e:
        log('ERROR', f'Insert failed: {e}')
        conn.rollback()
        return False

def delete_db_row(conn, table, condition):
    try:
        cur = conn.cursor()
        query = f"DELETE FROM {table} WHERE {condition};"
        cur.execute(query)
        conn.commit()
        cur.close()
        log('SUCCESS', 'Row removed successfully.')
        return True
    except Exception as e:
        log('ERROR', f'Row removal failed: {e}')
        conn.rollback()
        return False

def delete_all_db_rows(conn, table):
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table};")
        conn.commit()
        cur.close()
        log('SUCCESS', 'All rows deleted successfully.')
        return True
    except Exception as e:
        log('ERROR', f'Delete failed: {e}')
        conn.rollback()
        return False

def main():
    title = r"""
 __     __            __                           __       
|  \   |  \          |  \                         |  \      
| $$   | $$ __    __ | $$       ______    ______  | $$   __ 
| $$   | $$|  \  |  \| $$      /      \  |      \ | $$  /  \
 \$$\ /  $$| $$  | $$| $$     |  $$$$$$\  \$$$$$$\| $$_/  $$
  \$$\  $$ | $$  | $$| $$     | $$    $$ /      $$| $$   $$ 
   \$$ $$  | $$__/ $$| $$_____| $$$$$$$$|  $$$$$$$| $$$$$$\ 
    \$$$    \$$    $$| $$     \\$$     \ \$$    $$| $$  \$$\
     \$      \$$$$$$  \$$$$$$$$ \$$$$$$$  \$$$$$$$ \$$   \$$

     """
    
    print("\033[95m" + title + "\033[0m")

    log('INPUT', 'Choose connection method: "db" for direct database URL or "url" for GitHub raw link:')
    method = input().strip().lower()
    
    conn = None
    hub_url = None
    hub_key = None
    code = None
    schema = 'public'
    tables = []
    existing_tables = []

    if method == 'db':
        log('INPUT', "Choose method for database credentials: 'ai' to analyze code or 'manual' to enter directly:")
        creds_method = input().strip().lower()
        
        if creds_method == 'ai':
            log('INPUT', 'Enter raw URL containing database configuration:')
            creds_url = input().strip()
            stop_event = threading.Event()
            loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Fetching database configuration...'))
            loading_thread.start()
            try:
                response = requests.get(creds_url, timeout=10)
                response.raise_for_status()
                creds_code = response.text
                stop_event.set()
                loading_thread.join()
                log('SUCCESS', 'Successfully fetched database configuration.')
            except Exception as e:
                stop_event.set()
                loading_thread.join()
                log('ERROR', f'Failed to fetch configuration code: {str(e)}')
                input("Press any key to exit...")
                sys.exit(1)
            
            db_url = fetch_creds_from_ai(creds_url, creds_code, is_db=True)
            if not db_url:
                log('WARNING', 'AI failed to extract valid database credentials. Falling back to manual input.')
                creds_method = 'manual'
        
        if creds_method != 'ai':
            log('INPUT', 'Enter PostgreSQL database URL (e.g., postgresql://user:pass@host:port/db):')
            db_url = input().strip()
        
        try:
            conn = psycopg2.connect(db_url)
            log('SUCCESS', 'Successfully connected to database.')
            tables = get_db_tables(conn)
            if not tables:
                log('ERROR', 'No tables found in database. Exiting.')
                conn.close()
                input("Press any key to exit...")
                sys.exit(1)
            existing_tables = tables
            log('INFO', f'Existing tables: {", ".join(tables)}')
        except Exception as e:
            log('ERROR', f'Failed to connect to database: {e}')
            input("Press any key to exit...")
            sys.exit(1)
    else:
        log('INPUT', 'Enter valid raw URL:')
        vault_url = input().strip()
        
        stop_event = threading.Event()
        loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Fetching code from raw URL...'))
        loading_thread.start()
        try:
            response = requests.get(vault_url, timeout=10)
            response.raise_for_status()
            code = response.text
            stop_event.set()
            loading_thread.join()
            log('SUCCESS', 'Successfully fetched code from raw URL.')
        except Exception as e:
            stop_event.set()
            loading_thread.join()
            log('ERROR', f'Failed to fetch code: {str(e)}')
            input("Press any key to exit...")
            sys.exit(1)
        
        log('INPUT', "Choose method for data credentials: 'ai' to analyze code or 'manual' to enter directly:")
        creds_method = input().strip().lower()
        
        if creds_method == 'ai':
            while True:
                log('INPUT', 'Enter a different raw URL containing data:')
                creds_url = input().strip()
                if creds_url == vault_url:
                    log('ERROR', 'The credentials URL must be different from the code first URL.')
                    continue
                break
            stop_event = threading.Event()
            loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Fetching credentials...'))
            loading_thread.start()
            try:
                response = requests.get(creds_url, timeout=10)
                response.raise_for_status()
                creds_code = response.text
                stop_event.set()
                loading_thread.join()
                log('SUCCESS', 'Successfully fetched credentials.')
            except Exception as e:
                stop_event.set()
                loading_thread.join()
                log('ERROR', f'Failed to fetch credentials code: {str(e)}')
                input("Press any key to exit...")
                sys.exit(1)
            
            hub_url, hub_key = fetch_creds_from_ai(creds_url, creds_code)
            if not hub_url or not hub_key:
                log('WARNING', 'AI failed to extract valid data credentials. Falling back to manual input.')
                creds_method = 'manual'
            else:
                if not validate_hub(hub_url, hub_key):
                    log('WARNING', f'AI-extracted credentials for {hub_url} are invalid. Falling back to manual input.')
                    creds_method = 'manual'
        
        if creds_method != 'ai':
            log('INPUT', 'Enter data supabase URL:')
            hub_url = input().strip()
            log('INPUT', 'Enter data supabase key:')
            hub_key = input().strip()
        
        log('INPUT', 'Enter schema name (default: public):')
        schema = input().strip() or 'public'
        
        if not validate_hub(hub_url, hub_key):
            input("Press any key to exit...")
            sys.exit(1)
        
        tables = fetch_tables_from_ai(vault_url, code)
        if not tables:
            log('ERROR', 'No tables available to proceed. Exiting.')
            input("Press any key to exit...")
            sys.exit(1)
        
        for table in tables:
            table_ref = f"{schema}.{table}" if schema != 'public' else table
            cmd = [
                'curl',
                f"{hub_url}/rest/v1/{table_ref}?select=*",
                '-H', f"apikey: {hub_key}",
                '-H', f"Authorization: Bearer {hub_key}"
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                try:
                    data = json.loads(result.stdout)
                    if isinstance(data, list):
                        existing_tables.append(table)
                        log('SUCCESS', f'Table "{table}" exists.')
                    else:
                        log('WARNING', f'Table "{table}" does not exist or access denied. Response: {result.stdout}')
                except json.JSONDecodeError:
                    log('WARNING', f'Table "{table}" invalid response. Raw: {result.stdout}')
            except subprocess.CalledProcessError as e:
                log('WARNING', f'Table "{table}" check failed. Error: {e.stderr}', hub_key)
        
        if not existing_tables:
            log('WARNING', 'No existing tables found. Using fallback table list: users, profiles, posts')
            existing_tables = ['users', 'profiles', 'posts']
        
        log('INFO', f'Existing tables: {", ".join(existing_tables)}')
    
    log('INPUT', 'Enter master key for debugging (leave blank to use anon key):')
    master_key = input().strip() or hub_key
    
    while True:
        log('INPUT', 'Enter table name to query, "insert" to add data, "remove" to delete a row, "delete_all" to delete all rows, "display_all" to show all tables, or "q" to quit:')
        action = input().strip().lower()
        if action == 'q':
            if conn:
                conn.close()
            input("Press any key to exit...")
            break
        
        if action == 'display_all':
            if method == 'db':
                display_db_all(conn)
            else:
                display_rest_all(hub_url, hub_key, schema, existing_tables)
            continue
        
        if action == 'insert':
            log('INPUT', 'Enter table name to insert data into:')
            table = input().strip()
            if method == 'db':
                if table not in tables:
                    log('ERROR', 'Invalid table name.')
                    continue
                schema_data = get_db_schema(conn, table)
            else:
                if table not in existing_tables:
                    log('ERROR', 'Invalid table name.')
                    continue
                schema_data = get_table_schema(hub_url, hub_key, table, schema)
            if not schema_data:
                log('ERROR', 'Cannot insert data without schema information.')
                continue
            log('INFO', f'Required fields for "{table}": {", ".join(schema_data.keys())}')
            data_input = {}
            for field in schema_data.keys():
                log('INPUT', f'Enter value for "{field}" (leave blank to skip optional fields):')
                value = input().strip()
                if value:
                    if value.lower() in ('true', 'false'):
                        data_input[field] = value.lower() == 'true'
                    elif value.isdigit():
                        data_input[field] = int(value)
                    elif value.replace('.', '', 1).isdigit():
                        data_input[field] = float(value)
                    else:
                        data_input[field] = value
            if not data_input:
                log('ERROR', 'No data provided for insertion.')
                continue
            if method == 'db':
                if insert_db_data(conn, table, data_input):
                    continue
            else:
                table_ref = f"{schema}.{table}" if schema != 'public' else table
                cmd = [
                    'curl',
                    '-X', 'POST',
                    f"{hub_url}/rest/v1/{table_ref}",
                    '-H', f"apikey: {hub_key}",
                    '-H', f"Authorization: Bearer {hub_key}",
                    '-H', 'Content-Type: application/json',
                    '-d', json.dumps(data_input)
                ]
                stop_event = threading.Event()
                loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Inserting data...'))
                loading_thread.start()
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    stop_event.set()
                    loading_thread.join()
                    log('SUCCESS', 'Data inserted successfully.')
                except subprocess.CalledProcessError as e:
                    stop_event.set()
                    loading_thread.join()
                    log('ERROR', f'Insert failed: {e.stderr}', hub_key)
            continue
        
        if action == 'remove':
            log('INPUT', 'Enter table name to remove a row from:')
            table = input().strip()
            if method == 'db':
                if table not in tables:
                    log('ERROR', 'Invalid table name.')
                    continue
            else:
                if table not in existing_tables:
                    log('ERROR', 'Invalid table name.')
                    continue
            log('INPUT', 'Enter condition to identify the row (e.g., "id=eq.123" for REST, "id=123" for DB):')
            condition = input().strip().replace(' ', '')
            if not condition:
                log('ERROR', 'Condition is required.')
                continue
            if method == 'db':
                if delete_db_row(conn, table, condition):
                    continue
            else:
                table_ref = f"{schema}.{table}" if schema != 'public' else table
                cmd = [
                    'curl',
                    '-X', 'DELETE',
                    f"{hub_url}/rest/v1/{table_ref}?{condition}",
                    '-H', f"apikey: {hub_key}",
                    '-H', f"Authorization: Bearer {hub_key}"
                ]
                stop_event = threading.Event()
                loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Removing row...'))
                loading_thread.start()
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    stop_event.set()
                    loading_thread.join()
                    log('SUCCESS', 'Row removed successfully.')
                except subprocess.CalledProcessError as e:
                    stop_event.set()
                    loading_thread.join()
                    log('ERROR', f'Row removal failed: {e.stderr}', hub_key)
            continue
        
        if action == 'delete_all':
            log('INPUT', 'Enter table name to delete all rows from:')
            table = input().strip()
            if method == 'db':
                if table not in tables:
                    log('ERROR', 'Invalid table name.')
                    continue
                if delete_all_db_rows(conn, table):
                    continue
            else:
                if table not in existing_tables:
                    log('ERROR', 'Invalid table name.')
                    continue
                table_ref = f"{schema}.{table}" if schema != 'public' else table
                del_cmd = [
                    'curl',
                    '-X', 'DELETE',
                    f"{hub_url}/rest/v1/{table_ref}",
                    '-H', f"apikey: {hub_key}",
                    '-H', f"Authorization: Bearer {hub_key}"
                ]
                if schema != 'public':
                    del_cmd.append('-H')
                    del_cmd.append(f"Accept-Profile: {schema}")
                stop_event = threading.Event()
                loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Deleting all rows...'))
                loading_thread.start()
                try:
                    del_result = subprocess.run(del_cmd, capture_output=True, text=True, check=True)
                    stop_event.set()
                    loading_thread.join()
                    log('SUCCESS', 'All rows deleted successfully.')
                except subprocess.CalledProcessError as e:
                    stop_event.set()
                    loading_thread.join()
                    log('ERROR', f'Delete failed: {e.stderr}', hub_key)
            continue
        
        if method == 'db':
            if action not in tables:
                log('ERROR', 'Invalid table name.')
                continue
            log('INPUT', 'Enter query condition (e.g., "id=1") or empty for all rows:')
            query_str = input().strip()
            results = execute_db_query(conn, action, query_str)
            if results is not None:
                if not results:
                    log('INFO', 'Query returned no rows.')
                else:
                    log('SUCCESS', 'Query successful. Results:')
                    print(json.dumps(results, indent=4))
            continue
        
        if action not in existing_tables:
            log('ERROR', 'Invalid table name.')
            continue
        
        log('INPUT', 'Enter query string (e.g., "id=eq.1") or empty for all rows (no spaces in query):')
        query_str = input().strip().replace(' ', '')
        if query_str:
            query_param = f'?{query_str}'
        else:
            query_param = '?select=*'
        
        table_ref = f"{schema}.{action}" if schema != 'public' else action
        cmd = [
            'curl',
            f"{hub_url}/rest/v1/{table_ref}{query_param}",
            '-H', f"apikey: {hub_key}",
            '-H', f"Authorization: Bearer {hub_key}"
        ]
        if schema != 'public':
            cmd.append('-H')
            cmd.append(f"Accept-Profile: {schema}")
        stop_event = threading.Event()
        loading_thread = threading.Thread(target=show_loading, args=(stop_event, 'Querying table...'))
        loading_thread.start()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            stop_event.set()
            loading_thread.join()
            try:
                data = json.loads(result.stdout)
                if isinstance(data, list):
                    if not data:
                        log('WARNING', f'Query returned empty result for "{action}". Raw response: {result.stdout}')
                        if master_key != hub_key:
                            log('INFO', 'Retrying query with master key to bypass RLS...')
                            cmd_master = [
                                'curl',
                                f"{hub_url}/rest/v1/{table_ref}{query_param}",
                                '-H', f"apikey: {master_key}",
                                '-H', f"Authorization: Bearer {master_key}"
                            ]
                            if schema != 'public':
                                cmd_master.append('-H')
                                cmd_master.append(f"Accept-Profile: {schema}")
                            try:
                                result_master = subprocess.run(cmd_master, capture_output=True, text=True, check=True)
                                data_master = json.loads(result_master.stdout)
                                if data_master:
                                    log('WARNING', 'Data found with master key, indicating an RLS permission issue with anon key.')
                                    print(json.dumps(data_master, indent=4))
                                else:
                                    log('INFO', 'No data found even with master key. Table is likely empty.')
                            except subprocess.CalledProcessError as e:
                                log('ERROR', f'Master key query failed: {e.stderr}', master_key)
                        else:
                            log('INFO', 'Table may be empty or RLS restricts access. Provide a master key to verify.')
                    else:
                        log('SUCCESS', 'Query successful. Results:')
                        print(json.dumps(data, indent=4))
                else:
                    log('WARNING', f'Query returned non-list data: {result.stdout}')
            except json.JSONDecodeError:
                log('ERROR', f'Invalid JSON response from query: {result.stdout}')
        except subprocess.CalledProcessError as e:
            stop_event.set()
            loading_thread.join()
            log('ERROR', f'Query failed: {e.stderr}', hub_key)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n")
        log('INFO', 'Program interrupted by user.')
        input("Press any key to exit...")
    except Exception as e:
        log('ERROR', f'Unexpected error: {str(e)}')
        input("Press any key to exit...")