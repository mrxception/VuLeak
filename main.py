import requests
import subprocess
import json
import datetime
import sys
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

def fetch_creds_from_ai(vault_url, code, retries=3, delay=5):
    prompt = (
        f"Analyze the following code and extract the URL and key. "
        f"Return only a JSON object like: {{\"url\": \"https://example.datahub\", \"key\": \"eyJhb...\"}} "
        f"(use actual values from the code). If no credentials are found, return an empty object: {{}}. "
        f"Do not include any other text.\n\nCode:\n{code}"
    )
    
    api_url = "https://router.huggingface.co/novita/v3/openai/chat/completions"
    headers = {
        "Authorization": "Bearer ---",
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
    return None, None

def fetch_tables_from_ai(vault_url, code, retries=3, delay=5):
    prompt = (
        f"Analyze the following code and extract possible data table names mentioned or used in it. "
        f"Return only a JSON object like: {{\"tables\": [\"users\", \"profiles\", \"posts\"]}} "
        f"(use actual table names from the code). Do not include any other text.\n\nCode:\n{code}"
    )
    
    api_url = "https://router.huggingface.co/novita/v3/openai/chat/completions"
    headers = {
        "Authorization": "Bearer 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",
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
    method = input().strip().lower()
    
    if method == 'ai':
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
            method = 'manual'
        else:
            if not validate_hub(hub_url, hub_key):
                log('WARNING', f'AI-extracted credentials for {hub_url} are invalid. Falling back to manual input.')
                method = 'manual'
    
    if method != 'ai':
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
    
    existing_tables = []
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
        log('INPUT', 'Enter table name to query, "insert" to add data, "remove" to delete a row, or "q" to quit:')
        action = input().strip()
        if action.lower() == 'q':
            input("Press any key to exit...")
            break
        if action.lower() == 'insert':
            log('INPUT', 'Enter table name to insert data into:')
            table = input().strip()
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
        if action.lower() == 'remove':
            log('INPUT', 'Enter table name to remove a row from:')
            table = input().strip()
            if table not in existing_tables:
                log('ERROR', 'Invalid table name.')
                continue
            log('INPUT', 'Enter condition to identify the row (e.g., "id=eq.123"):')
            condition = input().strip().replace(' ', '')
            if not condition:
                log('ERROR', 'Condition is required.')
                continue
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
        
        log('INPUT', 'Delete all rows in this table? (y/n):')
        delete_choice = input().strip().lower()
        if delete_choice == 'y':
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