#!/bin/bash

environment=$1

# Validate environment
if [[ ! "$environment" =~ ^(dev|sit|uat|prod)$ ]]; then
  echo "Invalid environment: $environment"
  exit 1
fi

# Use Python and PyYAML to parse YAML
python -c "import yaml, sys; print(yaml.safe_load(open('$environment.yaml'))['your_key'])"
