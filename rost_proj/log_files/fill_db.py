import sqlite3
from django.db import connection
from rost_proj.log_files.parsing import parse_log

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
    # Таблица log_user_agent


#Таблица log_files_clients
    df_to_fill = logs_df[['IP',"UserID", 'Journal']].drop_duplicates()
    df_to_fill = df_to_fill.rename(columns={'IP': 'ip_client', 'UserID': 'user_id', 'Journal': 'journal_name', })
    df_to_fill['call'] = "-"

    # Проверка на наличие в бд
    df_to_fill.to_sql('temp_table', conn, if_exists='replace', index=False)
    # SQL-запрос для получения строк из temp_table, которые не имеют совпадений в log_files_clients
    query = ''' SELECT t.* FROM temp_table t LEFT JOIN log_files_clients l 
        ON t.ip_client = l.ip_client AND t.user_id = l.user_id AND t.journal_name = l.journal_name
        WHERE l.ip_client IS NULL AND l.user_id IS NULL AND l.journal_name IS NULL '''
    df_to_fill = pd.read_sql_query(query, conn)

    first_id = last_id("log_files_clients", 'id') + 1
    df_to_fill['id'] = range(first_id, first_id + len(df_to_fill))
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

    first_id = last_id("log_files_api", 'api_id') + 1
    df_to_fill['api_id'] = range(first_id, first_id + len(df_to_fill))
    df_to_fill.to_sql("log_files_api", conn, if_exists='append', index=False)


 # Таблица log_files_code_type
    df_to_fill = logs_df[['Status']].drop_duplicates()
    first_id = last_id("log_files_code_type", 'code_id') + 1
    df_to_fill['code_id'] = range(first_id, first_id + len(df_to_fill))
    df_to_fill = df_to_fill.rename(columns={'Status': 'code_name'})
    df_to_fill.to_sql("log_files_code_type", conn, if_exists='append', index=False)

    # Таблица log_files_protocol_version
    df_to_fill = logs_df[['Protocol',"Version",]].drop_duplicates()#названия столбцов проверить!
    first_id = last_id("log_files_protocol_version", 'protocol_id') + 1
    df_to_fill['protocol_id'] = range(first_id, first_id + len(df_to_fill))
    df_to_fill = df_to_fill.rename(columns={'Protocol': 'p_name', 'Version': 'p_version'})
    df_to_fill.to_sql("log_files_protocol_version", conn, if_exists='append', index=False)

    # Таблица log_files_referer
    df_to_fill = logs_df[['Referer']].drop_duplicates()
    first_id = last_id("log_files_referer", 'ref_id') + 1
    df_to_fill['ref_id'] = range(first_id, first_id + len(df_to_fill))
    df_to_fill = df_to_fill.rename(columns={'Referer': 'referer'})
    df_to_fill.to_sql("log_files_referer", conn, if_exists='append', index=False)

    # Таблица log_files_request_type
    df_to_fill = logs_df[['Request']].drop_duplicates()
    first_id = last_id("log_files_request_type", 'type_id') + 1
    df_to_fill['type_id'] = range(first_id, first_id + len(df_to_fill))
    df_to_fill = df_to_fill.rename(columns={'Request': 'type_name'})
    df_to_fill.to_sql("log_files_request_type", conn, if_exists='append', index=False)

    conn.close()
