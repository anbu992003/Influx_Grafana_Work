def read_properties_file(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config

# Path to the properties file
properties_file = 'config.properties'
config = read_properties_file(properties_file)

# Accessing configuration values
print("Database Host:", config.get('db.host'))
print("Database Port:", config.get('db.port'))
print("Database User:", config.get('db.user'))
print("Database Password:", config.get('db.password'))