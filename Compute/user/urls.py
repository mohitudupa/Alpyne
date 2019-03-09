from django.urls import path
from . import views


app_name = 'user'


urlpatterns = [
    path('echo/', views.Echo.as_view(), name='echo'),
    path('register/', views.Register.as_view(), name='register'),
]
