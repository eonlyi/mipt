from django.db.models import Count, Avg, F, Q, Case, When, Value, IntegerField, CharField
from django.db.models.functions import ExtractWeekDay
from datetime import datetime, timedelta
from log_files.models import Facts_table as FT, Request_type as RT, Result as R, Api as API, Code_type as CT, Clients as C, Referer as Ref
import pandas as pd

#ЗАПРОСЫ ПО АНАЛИТИКЕ
def get_total_requests_by_type():
    try:
        # Используем аннотации для подсчета записей и группировки по типу
        result = (
            FT.objects
            .values('id_type')  # Группировка по id_type
            .annotate(TotalRequests=Count('id_type'))  # Подсчет записей
            .order_by('-TotalRequests')  # Сортировка по убыванию
        )

        # Добавляем имена типов из RequestType
        request_types = RT.objects.in_bulk([item['id_type'] for item in result])
        result_with_names = [
            {
                'type_name': request_types[item['id_type']].type_name,
                'TotalRequests': item['TotalRequests']
            }
            for item in result
        ]

        return result_with_names

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None


def ips_with_most_404_errors():
    return(
    C.objects.filter(
        facts_table__id_result__code__code_name='404'
    ).annotate(
        total_404_errors=Count('facts_table__id_result__code')
    ).values(
        'ip_client', 'total_404_errors'
    ).order_by(
        '-total_404_errors'
    )[:10])


