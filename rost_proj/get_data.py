import sqlite3

DATABASE_NAME = 'db.git logsqlite3'

def execute_query(query, params=()):
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()

        return result

    except sqlite3.Error as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None

    finally:
        if conn:
            cursor.close()
            conn.close()

# Запросы

# 1) Общее количество запросов по типам (GET, POST...)
query_total_requests_by_type = """
SELECT 
    TYPE_NAME,
    COUNT(*) AS TotalRequests
FROM Facts_table ft
JOIN Request_type rt ON ft.TYPE_ID = rt.TYPE_ID
GROUP BY TYPE_NAME
ORDER BY TotalRequests DESC;
"""

# 2) Топ 10 самых популярных API
query_top_10_popular_apis = """
SELECT 
    API_NAME,
    COUNT(*) AS TotalRequests
FROM Facts_table ft
JOIN API a ON ft.API_ID = a.API_ID
GROUP BY API_NAME
ORDER BY TotalRequests DESC
LIMIT 10;
"""

# 3) Количество запросов по коду статуса (200, 404...)
query_requests_count_by_status_code = """
SELECT 
    CODE_NAME,
    COUNT(*) AS TotalRequests
FROM Facts_table ft
JOIN Result r ON ft.RES_ID = r.RES_ID
JOIN Code_type ct ON r.CODE_ID = ct.CODE_ID
GROUP BY CODE_NAME
ORDER BY TotalRequests DESC;
"""

# 4) Среднее время ответа сервера
query_average_response_time = """
SELECT 
    AVG(RES_TIME) AS AverageResponseTime
FROM Result;
"""

# 5) Количество запросов с каждого IP-адреса
query_requests_count_by_ip = """
SELECT 
    c.C_IP,
    COUNT(f.F_ID) AS TotalRequests
FROM Clients c
LEFT JOIN Facts_table f ON c.C_ID = f.C_ID
GROUP BY c.C_IP
ORDER BY TotalRequests DESC;
"""

# 6) Топ 5 рефереров, которые привели больше всего трафика
query_top_5_referrers = """
SELECT 
    r.R_NAME,
    COUNT(f.F_ID) AS TotalRequests
FROM Referer r
JOIN Facts_table f ON r.REF_ID = f.REF_ID
GROUP BY r.R_NAME
ORDER BY TotalRequests DESC
LIMIT 5;
"""

# 7) Распределение запросов по дням недели
query_requests_distribution_by_day_of_week = """
SELECT
    strftime('%w', F_DATA) AS DayOfWeek,
    COUNT(*) AS TotalRequests
FROM
    Facts_table
GROUP BY
    DayOfWeek
ORDER BY
    DayOfWeek;
"""

# 8) Соотношение успешных и неуспешных запросов
query_success_vs_failure_ratio = """
SELECT
    CASE
        WHEN ct.CODE_NAME LIKE '2%' THEN 'Success'
        ELSE 'Failure'
    END AS RequestStatus,
    COUNT(*) AS TotalRequests
FROM Facts_table ft
JOIN Result r ON ft.RES_ID = r.RES_ID
JOIN Code_type ct ON r.CODE_ID = ct.CODE_ID
GROUP BY RequestStatus;
"""

# 9) Общее количество байт, отправленных сервером
query_total_bytes_sent = """
SELECT
    SUM(RES_BAIT) AS TotalBytesSent
FROM Result;
"""

# 10) IP-адреса с наибольшим количеством ошибок 404:
query_ips_with_most_404_errors = """
SELECT 
    c.C_IP,
    COUNT(*) AS Total404Errors
FROM Clients c
JOIN Facts_table ft ON c.C_ID = ft.C_ID
JOIN Result r ON ft.RES_ID = r.RES_ID
JOIN Code_type ct ON r.CODE_ID = ct.CODE_ID
WHERE ct.CODE_NAME = '404'
GROUP BY c.C_IP
ORDER BY Total404Errors DESC
LIMIT 10;
"""

# 12) API с самым высоким средним временем ответа:
query_apis_with_highest_avg_response_time = """
SELECT 
    a.API_NAME,
    AVG(r.RES_TIME) AS AverageResponseTime
FROM API a
JOIN Facts_table ft ON a.API_ID = ft.API_ID
JOIN Result r ON ft.RES_ID = r.RES_ID
GROUP BY a.API_NAME
ORDER BY AverageResponseTime DESC
LIMIT 10;
"""

# 13) Типы запросов, занимающие больше всего времени на сервере:
query_request_types_with_highest_avg_response_time = """
SELECT 
    rt.TYPE_NAME,
    AVG(r.RES_TIME) AS AverageResponseTime
FROM Request_type rt
JOIN Facts_table ft ON rt.TYPE_ID = ft.TYPE_ID
JOIN Result r ON ft.RES_ID = r.RES_ID
GROUP BY rt.TYPE_NAME
ORDER BY AverageResponseTime DESC;
"""

# 14) IP-адреса, потребляющие больше всего байт:
query_ips_consuming_most_traffic = """
SELECT 
    c.C_IP,
    SUM(r.RES_BAIT) AS TotalBytes
FROM Clients c
JOIN Facts_table ft ON c.C_ID = ft.C_ID
JOIN Result r ON ft.RES_ID = r.RES_ID
GROUP BY c.C_IP
ORDER BY TotalBytes DESC
LIMIT 10;
"""
