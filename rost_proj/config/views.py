from django.shortcuts import render
from django.core.cache import cache
# Create your views here.

def main_page(request):
    return render(request, "main_page.html")

def upload_page(request):
    return render(request, "upload_page.html")

def dashboard_page(request):
    return render(request, "dashboard_page.html")

def report_page(request):
    return render(request, "report_page.html")