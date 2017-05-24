from django.conf.urls import url, include
from django.contrib import admin

from db.wb_django.weibo2 import views

urlpatterns = [
    url(r'^$', views.index),
    url(r'^tendency$',view=views.tendency)
]
