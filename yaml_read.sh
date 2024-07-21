sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq
sudo chmod +x /usr/bin/yq




dev:
  db_host: dev-db.example.com
  db_user: devuser
  db_pass: devpass
sit:
  db_host: sit-db.example.com
  db_user: situser
  db_pass: sitpass
uat:
  db_host: uat-db.example.com
  db_user: uatuser
  db_pass: uatpass
prod:
  db_host: prod-db.example.com
  db_user: produser
  db_pass: prodpass



#!/bin/bash

# Check if environment is passed as an argument
if [ -z "$1" ]; then
  echo "Usage: $0 <environment>"
  echo "Example: $0 dev"
  exit 1
fi

ENVIRONMENT=$1
CONFIG_FILE="config.yaml"

# Check if yq is installed
if ! command -v yq &> /dev/null; then
  echo "yq could not be found, please install yq."
  exit 1
fi

# Read configuration from YAML file
DB_HOST=$(yq e ".$ENVIRONMENT.db_host" $CONFIG_FILE)
DB_USER=$(yq e ".$ENVIRONMENT.db_user" $CONFIG_FILE)
DB_PASS=$(yq e ".$ENVIRONMENT.db_pass" $CONFIG_FILE)

# Check if the configuration values are empty
if [ -z "$DB_HOST" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASS" ]; then
  echo "Invalid environment or configuration not found for '$ENVIRONMENT'"
  exit 1
fi

# Print configuration values (you can use them in your script as needed)
echo "DB Host: $DB_HOST"
echo "DB User: $DB_USER"
echo "DB Pass: $DB_PASS"

