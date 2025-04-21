# Create your models here.
from django.db import models

class Facts_table(models.Model):
    fact_id = models.IntegerField(primary_key = True, default = 0)
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
     class Meta:
         unique_together = (('ip_client', 'user_id', 'journal_name'),)
     def __str__(self):
         return self.title

class User_agent (models.Model):
     id_agent = models.IntegerField(primary_key = True, default = 0)
     id_ip = models.IntegerField()
     os = models.CharField(max_length=1000)
     krnl = models.CharField(max_length=1000)
     ren_eng = models.CharField(max_length=1000)
     eng_ver = models.CharField(max_length=100)
     html_cmpbl = models.CharField(max_length=10000)
     browser = models.CharField(max_length=10000)
     browser_ver = models.CharField(max_length=100)
     class Meta:
         unique_together = (('id_ip', 'os', 'krnl', 'ren_eng','eng_ver', 'html_cmpbl', 'browser', 'browser_ver'),)
     def __str__(self):
         return self.title

class Protocol_version (models.Model):
     protocol_id = models.IntegerField(primary_key = True, default = 0)
     p_name = models.CharField(max_length=10000)
     class Meta:
         unique_together = (('p_name'),)
     def __str__(self):
         return self.title

class Request_type (models.Model):
     type_id = models.IntegerField(primary_key = True, default = 0)
     type_name = models.CharField(max_length=40)
     class Meta:
         unique_together = (('type_name'),)
     def __str__(self):
         return self.title

class Api (models.Model):
     api_id = models.IntegerField(primary_key = True, default = 0)
     api_name = models.CharField(max_length=400)
     class Meta:
         unique_together = (('api_name'),)
     def __str__(self):
         return self.title

class Result(models.Model):
    result_id = models.IntegerField(primary_key = True, default = 0)
    id_code = models.IntegerField()
    result_time = models.TimeField()
    result_byte = models.IntegerField()
    def __str__(self):
        return self.title

class Code_type(models.Model):
    code_id = models.IntegerField(primary_key = True, default = 0)
    code_name = models.CharField(max_length=40)
    class Meta:
        unique_together = (('code_name'),)
    def __str__(self):
        return self.title

class Referer(models.Model):
    ref_id = models.IntegerField(primary_key = True, default = 0)
    ref_name = models.CharField(max_length=10000)
    class Meta:
        unique_together = (('ref_name'),)
    def __str__(self):
        return self.title

