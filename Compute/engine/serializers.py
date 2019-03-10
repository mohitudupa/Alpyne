from rest_framework import serializers


class PostJobsSerializer(serializers.Serializer):
    input_container_name = serializers.CharField(max_length=50)
    output_container_name = serializers.CharField(max_length=50)
    input_file_name = serializers.CharField(max_length=50)
    code_file_name = serializers.CharField(max_length=50)
