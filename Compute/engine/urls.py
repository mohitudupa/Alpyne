from django.urls import path
from . import views

app_name = 'engine'


urlpatterns = [
    path('echo/', views.Echo.as_view(), name='echo'),
    path('jobs/', views.Jobs.as_view(), name='jobs'),
    path('job/<str:job_id>/', views.Job.as_view(), name='job'),
]
