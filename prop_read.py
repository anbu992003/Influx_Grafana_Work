import yaml
import os

def load_yaml_config(env):
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config.get(env, {})

def main():
    env = os.getenv('ENVIRONMENT', 'dev')  # Default to 'dev' if ENVIRONMENT is not set
    config = load_yaml_config(env)
    print(f"Configuration for {env}:")
    print(config)

if __name__ == '__main__':
    main()





dev.database.host=localhost
dev.database.port=5432
dev.api.key=dev-api-key

sit.database.host=sit-db.example.com
sit.database.port=5432
sit.api.key=sit-api-key

uat.database.host=uat-db.example.com
uat.database.port=5432
uat.api.key=uat-api-key

prod.database.host=prod-db.example.com
prod.database.port=5432
prod.api.key=prod-api-key
