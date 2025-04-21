import sqlite3
from django.db import connection
from log_files.parsing import parse_log
import pandas as pd

def last_id (db_table, column):
    #Получение максимального id из столбца таблицы бд
    with connection.cursor() as cursor:
            cursor.execute('SELECT MAX(%s) FROM %s', (column, db_table))
            max_id = cursor.fetchone()
            if max_id[0] is None:
                last_id = 0
            else:
                last_id = max_id[0]
    return(last_id)

#? path = 'logs/logfile1.log'

def fill_db(logfile_path):
    conn = sqlite3.connect('db.sqlite3')
    logs_df = parse_log(logfile_path)

    # Таблица log_files_facts_table
    # Таблица log_files_result



#Таблица log_files_clients
    df_to_fill = logs_df[['IP',"UserID", 'Journal']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'IP': 'ip_client', 'UserID': 'user_id', 'Journal': 'journal_name', })
    df_to_fill['call'] = "-" #ЗАГЛУШКА
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t LEFT JOIN log_files_clients l 
            ON t.ip_client = l.ip_client AND t.user_id = l.user_id AND t.journal_name = l.journal_name
            WHERE l.ip_client IS NULL AND l.user_id IS NULL AND l.journal_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_clients", conn, if_exists='append', index=False)

# Таблица log_files_api
    df_to_fill = logs_df[['Api']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Api': 'api_name'})
    # Проверка на наличие в бд
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t
        LEFT JOIN log_files_api l ON t.api_name = l.api_name
        WHERE l.api_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_api", conn, if_exists='append', index=False)


 # Таблица log_files_code_type
    df_to_fill = logs_df[['Status']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Status': 'code_name'})
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t
            LEFT JOIN log_files_code_type l ON t.code_name = l.code_name
            WHERE l.code_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_code_type", conn, if_exists='append', index=False)

    # Таблица log_files_protocol_version
    df_to_fill = logs_df[['Protocol']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Protocol': 'p_name'})
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t LEFT JOIN log_files_protocol_version l 
                ON t.p_name = l.p_name 
                WHERE l.p_name IS NULL  '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_protocol_version", conn, if_exists='append', index=False)

    # Таблица log_files_referer
    df_to_fill = logs_df[['Referer']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Referer': 'ref_name'})
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t
              LEFT JOIN log_files_referer l ON t.ref_name = l.ref_name
              WHERE l.ref_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_referer", conn, if_exists='append', index=False)

    # Таблица log_files_request_type
    df_to_fill = logs_df[['Request']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Request': 'type_name'})
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t
              LEFT JOIN log_files_request_type l ON t.type_name = l.type_name
              WHERE l.type_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_request_type", conn, if_exists='append', index=False)

# Таблица log_files_user_agent
    df_to_fill = logs_df[['OS', "OS_kernel",'Rendering_engine', "Rendering_engine_version",'Compatability','Browser', "Version_of_browser" ]].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'OS': 'os', 'OS_kernel': 'krnl', 'Rendering_engine': 'ren_eng', 'Rendering_engine_version': 'eng_ver', 'Compatability': 'html_cmpbl','Browser': 'browser', 'Version_of_browser': 'browser_ver'})
    df_to_fill['id_ip'] = '-' #ЗАГЛУШКА
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t LEFT JOIN log_files_user_agent l 
                ON t.os = l.os AND t.krnl = l.krnl AND t.ren_eng = l.ren_eng AND t.eng_ver = l.eng_ver AND t.html_cmpbl = l.html_cmpbl AND t.browser = l.browser AND t.browser_ver = l.browser_ver
                WHERE l.os IS NULL AND l.krnl IS NULL AND l.ren_eng IS NULL AND l.eng_ver IS NULL AND l.html_cmpbl IS NULL AND l.browser IS NULL AND l.browser_ver IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_user_agent", conn, if_exists='append', index=False)

    conn.close()

fill_db('log_files/logs/test.txt')
