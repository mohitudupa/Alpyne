from rest_framework import serializers


class PostRegisterSerializer(serializers.Serializer):
    username = serializers.RegexField(r'^.+$')
    email = serializers.EmailField()
    password = serializers.RegexField(r'^.{8,}$')
