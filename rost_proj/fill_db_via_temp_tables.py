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

    # Таблица log_files_user_agent
    df_to_fill = logs_df[['OS', "OS_kernel", 'Rendering_engine', "Rendering_engine_version", 'Compatability', 'Browser',
                          "Version_of_browser"]].drop_duplicates()
    df_to_fill = df_to_fill.rename(
        columns={'OS': 'os', 'OS_kernel': 'krnl', 'Rendering_engine': 'ren_eng', 'Rendering_engine_version': 'eng_ver',
                 'Compatability': 'html_cmpbl', 'Browser': 'browser', 'Version_of_browser': 'browser_ver'})
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = '''SELECT t.* FROM temp_table t LEFT JOIN log_files_user_agent l
                ON  t.os = l.os AND t.krnl = l.krnl AND t.ren_eng = l.ren_eng AND t.eng_ver = l.eng_ver AND t.html_cmpbl = l.html_cmpbl AND t.browser = l.browser AND t.browser_ver = l.browser_ver
                WHERE l.os IS NULL AND l.krnl IS NULL AND l.ren_eng IS NULL AND l.eng_ver IS NULL AND l.html_cmpbl IS NULL AND l.browser IS NULL AND l.browser_ver IS NULL'''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_user_agent", conn, if_exists='append', index=False)

#Таблица log_files_clients
    df_to_fill = logs_df[['IP',"UserID", 'Journal']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'IP': 'ip_client', 'UserID': 'user_id', 'Journal': 'journal_name' })
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

    # Таблица log_files_code_type
    df_to_fill = logs_df[['Status']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Status': 'code_name'})
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t
               LEFT JOIN log_files_code_type l ON t.code_name = l.code_name
               WHERE l.code_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_code_type", conn, if_exists='append', index=False)

    #  Таблица log_files_result:
    code_map = pd.read_sql(
        "SELECT code_id, code_name FROM log_files_code_type",
        conn
    ).set_index('code_name')['code_id'].to_dict()

    df_to_fill = logs_df[['Status','Duration', 'Byte']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'Status': 'id_code','Duration': 'result_time', 'Byte': 'result_byte'})
    df_to_fill['id_code'] = df_to_fill['id_code'].astype(str).map(code_map)
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)


    query = ''' SELECT t.* FROM temp_table t LEFT JOIN log_files_result l
                    ON t.result_time = l.result_time AND t.result_byte = l.result_byte
                    WHERE l.result_time IS NULL AND l.result_byte IS NULL'''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_result", conn, if_exists='append', index=False)

    # Таблица log_files_facts_table

    code_map_a = pd.read_sql(
        "SELECT id, api_name FROM log_files_api",
        conn
    ).set_index('api_name')['id'].to_dict()

    df_to_fill = logs_df[['IP','Api','Date','Protocol','Request','Referer']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'IP':'id_client','Api': 'id_API','Date': 'date', 'Protocol':'id_protocol', 'Request': 'id_type', 'Referer': 'id_referer'})
    df_to_fill['id_API'] = df_to_fill['id_API'].astype(str).map(code_map_a)

    code_map_c = pd.read_sql(
        "SELECT ip_client, id FROM log_files_clients",
        conn
    ).set_index('ip_client')['id'].to_dict()
    df_to_fill['id_client'] = df_to_fill['id_client'].astype(str).map(code_map_c)

    code_map_p = pd.read_sql(
        "SELECT id, p_name FROM log_files_protocol_version",
        conn
    ).set_index('p_name')['id'].to_dict()

    df_to_fill['id_protocol'] =df_to_fill['id_protocol'].astype(str).map(code_map_p)

    code_map_rt = pd.read_sql(
        "SELECT id, type_name FROM log_files_request_type",
        conn
    ).set_index('type_name')['id'].to_dict()

    df_to_fill['id_type'] = df_to_fill['id_type'].astype(str).map(code_map_rt)

    code_map_ref = pd.read_sql(
        "SELECT id, ref_name FROM log_files_referer",
        conn
    ).set_index('ref_name')['id'].to_dict()

    df_to_fill['id_referer'] = df_to_fill['id_referer'].astype(str).map(code_map_ref)
    #df_to_fill['user_agent'] =

    code_map_result = pd.read_sql("SELECT id FROM log_files_result ORDER BY id", conn)
    df_from1 = pd.DataFrame(code_map_result)

    df_to_fill['id_result'] = df_from1['id']

    #user agent
    res = logs_df[['OS','OS_kernel','Rendering_engine','Rendering_engine_version','Compatability','Browser','Version_of_browser']]
    res = res.rename(columns={'OS':'os','OS_kernel':'krnl','Rendering_engine':'ren_eng','Rendering_engine_version':'eng_ver','Compatability':'html_cmpbl','Browser':'browser','Version_of_browser':'browser_ver'})
    res.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT l.* FROM temp_table t LEFT JOIN log_files_user_agent l 
                    ON t.os = l.os AND t.krnl = l.krnl AND t.ren_eng = l.ren_eng AND t.eng_ver = l.eng_ver AND  t.html_cmpbl = l.html_cmpbl AND t.browser = l.browser AND t.browser_ver = l.browser_ver
            '''
    res = pd.read_sql_query(query, conn)

    df_to_fill['id_agent'] = res['agent_id']

    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    query = ''' SELECT t.* FROM temp_table t
                        LEFT JOIN log_files_facts_table l ON t.date = l.date
                        WHERE l.date IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)
    df_to_fill.to_sql("log_files_facts_table", conn, if_exists='append', index=False)
    #date

    conn.close()

#fill_db('log_files/logs/test.txt')
