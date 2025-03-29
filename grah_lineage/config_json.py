import os
environment = os.getenv("ENV", "development")
config_file = f"config_{environment}.json"

with open(config_file) as f:
    config = json.load(f)

    