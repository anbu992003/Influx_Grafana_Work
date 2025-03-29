import subprocess
import os

def read_config_from_shell(script_path):
    # Source the shell script and capture its environment variables
    result = subprocess.run(['bash', '-c', f'source {script_path} && env'],
                            capture_output=True, text=True)
    # Parse the environment variables
    env_vars = {}
    for line in result.stdout.splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            env_vars[key] = value
    return env_vars

# Path to the shell script
script_path = 'config.sh'
config = read_config_from_shell(script_path)

# Accessing configuration variables
print("Database Host:", config.get('DB_HOST'))
print("Database Port:", config.get('DB_PORT'))
print("Database User:", config.get('DB_USER'))
print("Database Password:", config.get('DB_PASSWORD'))