# Create your models here.
from django.db import models

class Facts_table(models.Model):
    fact_id = models.IntegerField()
    id_ip = models.IntegerField()
    id_type = models.IntegerField()
    id_API = models.IntegerField()
    id_protocol = models.IntegerField()
    id_result = models.IntegerField()
    id_referer = models.IntegerField()
    id_data = models.IntegerField()
    def __str__(self):
        return self.title

class Clients (models.Model):
     ip_client = models.CharField(max_length=20)
     user_id = models.CharField(max_length=10000)
     journal_name = models.CharField(max_length=10000)
     call = models.IntegerField()
     def __str__(self):
         return self.title

class User_agent (models.Model):
     id_agent = models.IntegerField()
     id_ip = models.IntegerField()
     os = models.CharField(max_length=1000)
     krnl = models.CharField(max_length=1000)
     ren_eng = models.CharField(max_length=1000)
     eng_ver = models.CharField(max_length=100)
     html_cmpbl = models.CharField(max_length=10000)
     browser = models.CharField(max_length=10000)
     browser_ver = models.CharField(max_length=100)
     def __str__(self):
         return self.title

class Protocol_version (models.Model):
     protocol_id = models.IntegerField()
     p_name = models.CharField(max_length=10000)
     p_version = models.CharField(max_length=100)
     def __str__(self):
         return self.title

class Request_type (models.Model):
     type_id = models.IntegerField()
     type_name = models.CharField(max_length=40)
     def __str__(self):
         return self.title

class Api (models.Model):
     api_id = models.IntegerField()
     api_name = models.CharField(max_length=400)
     def __str__(self):
         return self.title

class Result(models.Model):
    result_id = models.IntegerField()
    id_code = models.IntegerField()
    result_time = models.TimeField()
    result_byte = models.IntegerField()
    def __str__(self):
        return self.title

class Code_type(models.Model):
    code_id = models.IntegerField()
    code_name = models.CharField(max_length=40)
    def __str__(self):
        return self.title

class Referer(models.Model):
    ref_id = models.IntegerField()
    ref_name = models.CharField(max_length=10000)
    def __str__(self):
        return self.title
