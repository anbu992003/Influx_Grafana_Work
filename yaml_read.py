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





dev:
  database:
    host: "localhost"
    port: 5432
  api:
    key: "dev-api-key"

sit:
  database:
    host: "sit-db.example.com"
    port: 5432
  api:
    key: "sit-api-key"

uat:
  database:
    host: "uat-db.example.com"
    port: 5432
  api:
    key: "uat-api-key"

prod:
  database:
    host: "prod-db.example.com"
    port: 5432
  api:
    key: "prod-api-key"
