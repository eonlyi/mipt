from django.shortcuts import render
from django.core.cache import cache
from .forms import UploadFileForm, Analysis
import os
import fill_db_via_temp_tables
from django.conf import settings
from . import test_analysis
import csv
from django.http import HttpResponse
from log_files.models import Facts_table

# Create your views here.
def upload_page(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            upload_dir = os.path.join(settings.BASE_DIR, 'uploads')
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir)
            file_path = os.path.join(upload_dir, file.name)
            with open(file_path, 'wb+') as destination:
                for chunk in file.chunks():
                    destination.write(chunk)
            fill_db_via_temp_tables.fill_db(file_path)
            return render(request, 'upload_page.html', {'form': form, 'success': True})
    else:
        form = UploadFileForm()
    return render(request, 'upload_page.html', {'form': form, 'success': False})

def main_page(request):
    return render(request, "main_page.html")

def dashboard_page(request):
    return render(request, 'dashboard_page.html')

def analysis(request):
    analysis = test_analysis.get_total_requests_by_type()
    context = {
        'analysis': analysis,
        }
    return render(request, 'analysis.html', context)

def export_facts_view(request):
    if request.method == 'POST':
        # Если запрос POST, выгружаем данные в CSV
        return export_facts_to_csv(request)
    else:
        # Если запрос GET, отображаем страницу с кнопкой для скачивания
        return render(request, 'report_page.html')

def export_facts_to_csv(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="facts.csv"'

    writer = csv.writer(response)
    writer.writerow([
        'Date','Client IP', 'Journal Name', 'User ID', 'Request Type', 'API Name',
        'Protocol Version', 'Result Code', 'Result Time', 'Result Byte',
        'Referer Name', 'OS', 'Kernel', 'Rendering Engine', 'Engine Version',
        'HTML Compatibility', 'Browser', 'Browser Version'
    ])

    facts = Facts_table.objects.all().select_related(
        'id_client', 'id_type', 'id_API', 'id_protocol', 'id_result',
        'id_referer', 'id_agent'
    )

    for fact in facts:
        writer.writerow([
            fact.date if fact.date else '-',
            fact.id_client.ip_client if fact.id_client else '',
            fact.id_client.journal_name if fact.id_client else '',
            fact.id_client.user_id if fact.id_client else '',
            fact.id_type.type_name if fact.id_type else '',
            fact.id_API.api_name if fact.id_API else '',
            fact.id_protocol.p_name if fact.id_protocol else '',
            fact.id_result.code.code_name if fact.id_result and fact.id_result.code else '',
            fact.id_result.result_time if fact.id_result else '',
            fact.id_result.result_byte if fact.id_result else '',
            fact.id_referer.ref_name if fact.id_referer else '',
            fact.id_agent.os if fact.id_agent else '',
            fact.id_agent.krnl if fact.id_agent else '',
            fact.id_agent.ren_eng if fact.id_agent else '',
            fact.id_agent.eng_ver if fact.id_agent else '',
            fact.id_agent.html_cmpbl if fact.id_agent else '',
            fact.id_agent.browser if fact.id_agent else '',
            fact.id_agent.browser_ver if fact.id_agent else '',
        ])

    return response
