from configparser import ConfigParser


config = ConfigParser()


config['settings'] = {
    'env_path': '/Users/username/Env',
}

config['db'] = {
    'db_host': '127.0.0.1',
    'db_port': 27017,
    'username': 'hydrogen',
    'password': '1e1p0n'
}


with open('.config', 'w') as f:
    config.write(f)
