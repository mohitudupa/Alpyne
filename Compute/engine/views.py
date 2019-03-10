from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from django.http import Http404

from pymongo import MongoClient

import configparser
import os
import uuid
import threading

from . import serializers, env

project_path = os.environ.get('PROJECTPATH')

parser = configparser.ConfigParser()
parser.read(os.path.join(project_path, '.config'))

db_host = parser.get('db', 'db_host')
db_port = int(parser.get('db', 'db_port'))

client = MongoClient(db_host, db_port)
db = client['alpyne']
users_collection = db['users']

jobs = {}


def update_job_status(job_id: str, status: str):
    """
    This function is used to update the job status of a job from init to running and from running to finished
    :param job_id: job id of the job (a uuid in hex format)
    :param status: new status of the job (either running | finished)
    :return:
    :rtype: None
    """
    global jobs
    jobs[job_id]['status'] = status


def update_job_pid(job_id, pid):
    """
    This function is used to add the pid of the job to the jobs dictionary
    :param job_id: job id of the job (a uuid in hex format)
    :param pid: pid of the job subprocess
    :return:
    :rtype: None
    """
    global jobs
    jobs[job_id]['pid'] = pid


class Echo(APIView):
    """
    This class based view is an echo response for GET, POST, PUT and DELETE requests
    Does not require user authentication for access
    """
    permission_classes = (permissions.AllowAny,)

    @staticmethod
    def get(request, format=None):
        """
        This method applies get call to the `echo` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :return: Json response of str: `'Echo'` for the api call
        """
        return Response('Echo')

    @staticmethod
    def post(request, format=None):
        """
        This method applies post call to the `echo` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :return: Json response dict: `data posted` for the api call
        """
        return Response(request.data)

    @staticmethod
    def put(request, format=None):
        """
        This method applies get call to the `echo` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :return: Json response dict: `data put` for the api call
        """
        return Response(request.data)

    @staticmethod
    def delete(request, format=None):
        """
        This method applies get call to the `echo` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :return: Json response str: `'Echo'` for the api call
        """
        return Response('Echo')


class Jobs(APIView):
    """
    This class based view is the Jobs listing response for GET and POST requests
    Requires user authentication for access
    """
    permission_classes = (permissions.IsAuthenticated,)

    @staticmethod
    def get(request, format=None):
        """
        This method applies get call to the `jobs` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :return: Json response dict: `job id of the created job` for the api call
        """
        return Response(jobs)

    @staticmethod
    def post(request, format=None):
        """
        This method applies post call to the `jobs` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :return: Json response dict: `the list of running and stopped jobs` for the api call
                 Json response dict: `error message on invalid job post request` for the api call
        """
        try:
            serialized_post_jobs = serializers.PostJobsSerializer(data=request.data)
            if not serialized_post_jobs.is_valid():
                raise AssertionError('Invalid data')
            
            data = serialized_post_jobs.validated_data

            mongo_user = users_collection.find_one({'username': request.user.username})

            data['client_id'] = mongo_user['client_id']
            data['job_id'] = uuid.uuid1().hex
            data['status'] = 'init'
            data['pid'] = ''

            jobs[data['job_id']] = data

            file_data = env.val_env(data)
            td = threading.Thread(target=env.run, args=(data, file_data, update_job_status, update_job_pid))
            td.start()

        except AssertionError as ae:
            return Response({'error': str(ae)})
        
        return Response({'job created': data['job_id']})


class Job(APIView):
    @staticmethod
    def get(request, job_id, format=None):
        """
        This method applies get call to the `jobs` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :param job_id: Job ID of the job whose details has to be fetched
        :return: Json response dict: `job data of the job with id job_id` for the api call
        """
        data = jobs.get(job_id, {})
        if not data:
            raise Http404
        return Response(data)

    @staticmethod
    def delete(request, job_id, format=None):
        """
        This method applies delete call to the `jobs` url
        :param request: Request object from django
        :param format: Return format for the DFR ui
        :param job_id: Job ID of the job to be killed
        :return: Json response dict: `job id of the killed job` for the api call
        """
        data = jobs.get(job_id, {})
        if not data:
            raise Http404

        if data['status'] != 'finished':
            os.kill(data['pid'], 2)

        jobs.pop(job_id)

        return Response({'killed': job_id})


"""
{
        "input_file_name": "blk1",
        "input_container_name": "inp",
        "output_container_name": "out",
        "code_file_name": "code.py"
}
"""
