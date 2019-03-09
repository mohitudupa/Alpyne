import os
from subprocess import Popen
from Containers import containers as fs
from configparser import ConfigParser


parser = ConfigParser()
parser.read('.config')

db_host = parser.get('db', 'db_host')
db_port = int(parser.get('db', 'db_port'))
db_username = parser.get('db', 'username')
db_password = parser.get('db', 'password')

path = parser.get('settings', 'env_path')


def val_env(data: dict):
    """
    This function validates the input-container and the output-container sent for job creation
    :param data: data posted during job creation
    :return: File handlers for input-file, code-file and support files
    "rtype: dict
    """
    containers = fs.Containers(db_host, db_port, db_username, db_password, data['client_id'])

    file_data = {}

    try:
        # Getting the file pointers for code and files
        containers.open("code")
        file_data['code_handler'] = containers.get_one(data['code_file_name'])
        containers.open("files")
        file_data['file_names'] = containers.list_files()
        file_data['files_handlers'] = containers.get(file_data['file_names'])
        containers.open(data['input_container_name'])
        file_data['input_handler'] = containers.get_one(data['input_file_name'])

        return file_data
    except Exception:
        raise AssertionError('Invalid container resources')
    finally:
        containers.close()


def init_env(data: dict, file_data: dict):
    """
    This function creates the environment for execution with code, support files and the input file
    :param data: File names and container names of the job
    :param file_data: File handlers for the env
    :return:
    :rtype: None
    """
    job_id = data['job_id']

    os.mkdir(f"{path}/{job_id}")

    with open(f"{path}/{job_id}/{data['code_file_name']}", "wb") as f:
        f.write(file_data['code_handler'].read())
    file_data['code_handler'].close()
    
    for file_name, file_pointer in zip(file_data['file_names'], file_data['files_handlers']):
        with open(f"{path}/{job_id}/{file_name}", "wb") as f:
            f.write(file_pointer.read())
        file_pointer.close()
    
    with open(f"{path}/{job_id}/inp", "wb") as f:
        f.write(file_data['input_handler'].read())
    file_data['input_handler'].close()


def rm_env(data: dict):
    """
    This function removes the environment and uploads the result of the job to the container
    :param data: File names and container names of the job
    :return:
    :rtype: None
    """
    with open(f'{path}/{data["job_id"]}/out', 'rb') as out:
        containers = fs.Containers(db_host, db_port, db_username, db_password, data['client_id'])
        containers.open(data['output_container_name'])
        containers.put_one(out, data['input_file_name'])
        containers.close()

    os.system(f"rm -rf {path}/{data['job_id']}")


def run(data: dict, file_data: dict, update_job_status, update_job_pid):
    """
    This function executes the setup of the environment, runs the environment and tears down the environment
    :param data: File names and container names of the job
    :param file_data: File handlers for the env
    :param update_job_status: Callback function to update job status
    :param update_job_pid: Callback function to update job pid
    :return:
    :rtype: str
    """
    init_env(data, file_data)

    update_job_status(data['job_id'], 'running')

    process = Popen(['python', f'Compute/engine/run.py',
                     f'{path}/{data["job_id"]}',
                     data['code_file_name'].split('.')[0]])

    update_job_pid(data['job_id'], process.pid)

    process.wait()
    
    rm_env(data)

    update_job_status(data['job_id'], 'finished')
