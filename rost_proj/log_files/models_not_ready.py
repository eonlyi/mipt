# Create your models here.
from django.db import models

class Code_type(models.Model):
    #code_id = models.IntegerField(primary_key = True, default = 0)
    code_name = models.CharField(max_length=40)
    class Meta:
        unique_together = (('code_name'),)
    def __str__(self):
        return self.title

class Result(models.Model):
    code = models.ForeignKey(Code_type, on_delete=models.SET_DEFAULT, blank=True, null=True, default=None)
    result_time = models.TimeField()
    result_byte = models.IntegerField()
    def __str__(self):
        return self.title