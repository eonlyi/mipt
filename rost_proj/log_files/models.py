# Create your models here.
from django.db import models


class User_agent(models.Model):
    agent_id = models.AutoField(primary_key=True)
    os = models.CharField(max_length=1000)
    krnl = models.CharField(max_length=1000)
    ren_eng = models.CharField(max_length=1000)
    eng_ver = models.CharField(max_length=100)
    html_cmpbl = models.CharField(max_length=10000)
    browser = models.CharField(max_length=10000)
    browser_ver = models.CharField(max_length=100)

    class Meta:
        unique_together = (('os', 'krnl', 'ren_eng', 'eng_ver', 'html_cmpbl', 'browser', 'browser_ver'),)

    def __str__(self):
        return self.title


class Clients (models.Model):
     ip_client = models.CharField(max_length=20)
     journal_name = models.CharField(max_length=10000)
     #call = models.IntegerField()
     user_id = models.CharField(max_length=20)
     class Meta:
         unique_together = (('ip_client', 'journal_name'),)
     def __str__(self):
         return self.title

class Protocol_version (models.Model):
     id = models.AutoField(primary_key=True)
     p_name = models.CharField(max_length=10000)
     class Meta:
         unique_together = (('p_name'),)
     def __str__(self):
         return self.title

class Request_type (models.Model):
     id = models.AutoField(primary_key=True)
     type_name = models.CharField(max_length=40)
     class Meta:
         unique_together = (('type_name'),)
     def __str__(self):
         return self.title

class Api (models.Model):
     id = models.AutoField(primary_key=True)
     api_name = models.CharField(max_length=400)
     class Meta:
         unique_together = (('api_name'),)
     def __str__(self):
         return self.title

class Code_type(models.Model):
    code_id = models.AutoField(primary_key=True)
    code_name = models.CharField(max_length=40, unique=True)  # Added unique

    class Meta:
        db_table = 'log_files_code_type'  # Explicit table name

    def __str__(self):  # Fixed double underscore
        return self.code_name  # Return actual field

class Result(models.Model):
    id = models.AutoField(primary_key=True)
    code = models.ForeignKey(
        Code_type,  # Fixed reference
        on_delete=models.SET_NULL,  # Changed from SET_DEFAULT
        null=True,
        blank=True,
        db_column='id_code',  # Must match your DB column
        to_field='code_id'  # Must match referenced field
    )
    result_time = models.TimeField()
    result_byte = models.IntegerField()

    class Meta:
        db_table = 'log_files_result'  # Explicit table name

    def __str__(self):
        return f"Result {self.id}"

class Referer(models.Model):
    id = models.AutoField(primary_key=True)
    ref_name = models.CharField(max_length=10000)
    class Meta:
        unique_together = (('ref_name'),)
    def __str__(self):
        return self.title

class Facts_table(models.Model):
    id_client = models.ForeignKey(Clients, on_delete=models.SET_NULL, null=True, blank=True, db_column='id_client', to_field='id')
    id_type = models.ForeignKey(Request_type, on_delete=models.SET_NULL, null=True, blank=True, db_column='id_type', to_field='id')
    id_API = models.ForeignKey(Api, on_delete=models.SET_NULL, null=True, blank=True, db_column='id_API', to_field='id')
    id_protocol = models.ForeignKey(Protocol_version, on_delete=models.SET_NULL, null=True, blank=True, db_column='id_protocol', to_field='id')
    id_result = models.ForeignKey(Result,on_delete=models.SET_NULL, null=True, blank=True, db_column='id_result', to_field='id')  # Must match referenced field)
    id_referer = models.ForeignKey(Referer, on_delete=models.SET_NULL, null=True, blank=True, db_column='id_referer', to_field='id')
    id_agent = models.ForeignKey(User_agent, on_delete=models.SET_NULL, null=True, blank=True, db_column='id_agent', to_field='agent_id')
    date = models.DateField(max_length=400, blank=True, null=True, default=None)
    def __str__(self):
        return self.title

