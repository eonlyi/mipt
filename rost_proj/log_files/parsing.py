import pandas as pd
import re

def parse_user_agent(ua_string):
    data = {
        'OS': re.search(r'; (.*?)\)', ua_string).group(1) if re.search(r'; (.*?)\)', ua_string) else '-',
        'OS_kernel': re.search(r' \(([^;]+);', ua_string).group(1) if re.search(r' \(([^;]+);', ua_string) else '-',
        'Rendering_engine': re.search(r'\) (.*?)/', ua_string).group(1) if re.search(r'\) (.*?)/', ua_string) else '-',
        'Rendering_engine_version': re.search(r'.*?/.*?/(.*?)\(', ua_string).group(1) if re.search(r'.*?/.*?/(.*?)\(',                                                                                       ua_string) else '-',
    }

    compatability_match = re.search(r'.*?\).*? \((.*?)\)', ua_string)
    if compatability_match:
        data['Compatability'] = compatability_match.group(1)
        browsers_match = re.search(r'.*?\).*?\) (.*?)$', ua_string)
        browsers_match = browsers_match.group(1)
        matches = re.findall(r'(.*?/.*?) ', browsers_match)
        browsers_and_versions = [[i.split('/')[0], i.split('/')[1]] for i in matches]
        data['Browser'] = ', '.join([browser[0] for browser in browsers_and_versions])
        data['Version_of_browser'] = ', '.join([version[1] for version in browsers_and_versions])
    else:
        data['Compatability'] = '-'
        data['Browser'] = '-'
        data['Version_of_browser'] = '-'

    return pd.Series(data)

def parse_log(filename_path):
    column_names = [
        'IP', 'UserID', 'Journal', 'Date', 'Time', "Time_zone", 'Line1', 'Status',
        'Byte', 'Referer', 'UserAgent', 'Duration']
    logs = pd.read_csv(filename_path, delimiter=' ', quotechar='"', engine='python', names=column_names)
    logs['Date'] = logs['Date'] + ' ' + logs['Time'] + ' ' + logs['Time_zone']
    logs.drop(columns=['Time', 'Time_zone'], inplace=True)
    logs[['Request', 'Api', 'Protocol']] = logs['Line1'].str.split(expand=True)
    logs.drop(columns=['Line1'], inplace=True)
    logs = logs['UserAgent'].apply(parse_user_agent).reset_index(drop=True)
    logs = pd.concat([logs, df], axis=1)

    return (logs)

parse_log("logs/logfile1.log")
