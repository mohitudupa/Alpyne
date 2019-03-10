from configparser import ConfigParser
import os


def main():
    print('Setting up .config ...')
    project_path = os.path.abspath('.')
    env_path = os.path.join(project_path, 'Env/')
    config = ConfigParser()

    config['settings'] = {
        'project_path': project_path,
        'env_path': env_path,
    }

    config['db'] = {
        'db_host': '127.0.0.1',
        'db_port': 27017,
        'username': 'hydrogen',
        'password': '1e1p0n',
    }

    with open('.config', 'w') as f:
        config.write(f)

    print('Added config file...')
    print('Setting up .env ...')

    with open('.env', 'w') as f:
        f.write(f'export PYTHONPATH="{project_path}"\n')
        f.write(f'PROJECTPATH="{project_path}"\n')

    print('Added .env file...')
    print('Restart your pipenv environment to load the new python-path')


if __name__ == '__main__':
    main()
