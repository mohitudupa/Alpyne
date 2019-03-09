from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from django.contrib.auth.models import User

from pymongo import MongoClient

import configparser

from . import serializers

parser = configparser.ConfigParser()
parser.read('.config')

db_host = parser.get('db', 'db_host')
db_port = int(parser.get('db', 'db_port'))


client = MongoClient(db_host, db_port)
db = client['alpyne']


class Echo(APIView):
    permission_classes = (permissions.IsAuthenticated,)

    @staticmethod
    def get(request, format=None):
        return Response('Echo')

    @staticmethod
    def post(request, format=None):
        return Response(request.data)

    @staticmethod
    def put(request, format=None):
        return Response(request.data)

    @staticmethod
    def delete(request, format=None):
        return Response(request.data)


class Register(APIView):
    @staticmethod
    def post(request, format=None):
        try:
            serialized_post_register = serializers.PostRegisterSerializer(data=request.data)
            if not serialized_post_register.is_valid():
                raise AssertionError('Invalid data')
            
            data = serialized_post_register.validated_data

            mongo_user = db['users'].find_one({'username': data['username']})
            if not mongo_user:
                raise AssertionError('No registration found in master DB')

            user = User.objects.create_user(username=data['username'],
                                            email=data['email'],
                                            password=data['password'])
            if not user:
                raise AssertionError('User not created')

        except AssertionError as ae:
            return Response({'error': str(ae)})
        
        return Response({'user created': data['username']})
