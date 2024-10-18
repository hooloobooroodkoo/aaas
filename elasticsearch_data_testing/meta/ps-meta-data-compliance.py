# ps-meta-data-compliance - This module checks whether expected hosts are found in the Elasticsearch meta data(day before yesterday).
#                  It queries Elasticsearch for meta data and compares it with a list of expected hosts.
#                  The purpose of this function is to identify hosts that are not found in the Elasticsearch
#                  meta data, but are specified in the configurations. This is crucial for maintaining the integrity of the 
#                  configurations and ensuring that all relevant hosts' tests are captured in the monitoring 
#                  system database therefore the inner pipeline is in proper working order.
#
#                  The function returns a list of hosts that were not found in Elasticsearch. It creates an alarm
#                  for each host not found in the Elasticsearch.
#
# Author: Yana Holoborodko
# Copyright 2024

import time
import datetime as dt
import hashlib
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import psconfig.api
import urllib3
from alarms import alarms

def ConnectES():
    """
    Connects to the Elasticsearch instance using credentials from a file.
    Returns:
        Elasticsearch object if the connection is successful, None otherwise.
    """
    user, passwd = None, None
    with open("creds.key") as f:
        user = f.readline().strip()
        passwd = f.readline().strip()
    try:
        es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}],
                           request_timeout=240, http_auth=(user, passwd), max_retries=10)
        print('Success' if es.ping() else 'Fail')
        return es
    except Exception as error:
        print(">>> Elacticsearch Client Error:", error)

def FindPeriodDiff(dateFrom, dateTo):
    """
    Calculates the difference between two date values.
    Parameters:
        dateFrom: Starting date (in milliseconds or string).
        dateTo: Ending date (in milliseconds or string).
        
    Returns:
        Time difference as a timedelta object.
    """
    if isinstance(dateFrom, int) and isinstance(dateTo, int):
        d1 = datetime.fromtimestamp(dateTo / 1000)
        d2 = datetime.fromtimestamp(dateFrom / 1000)
        time_delta = (d1 - d2)
    else:
        fmt = '%Y-%m-%d %H:%M'
        d1 = datetime.strptime(dateFrom, fmt)
        d2 = datetime.strptime(dateTo, fmt)
        time_delta = d2 - d1
    return time_delta

def GetTimeRanges(dateFrom, dateTo, intv=1):
    """
    Generates a list of time intervals between two dates.
    Parameters:
        dateFrom: Starting date (in milliseconds or string).
        dateTo: Ending date (in milliseconds or string).
        intv: Number of intervals to create.    
    Returns:
        List of timestamps in milliseconds.
    """
    diff = FindPeriodDiff(dateFrom, dateTo) / intv
    t_format = "%Y-%m-%d %H:%M"
    tl = []
    for i in range(intv + 1):
        if isinstance(dateFrom, int):
            t = (datetime.fromtimestamp(dateFrom / 1000) + diff * i)
            tl.append(int(time.mktime(t.timetuple()) * 1000))
        else:
            t = (datetime.strptime(dateFrom, t_format) + diff * i).strftime(t_format)
            tl.append(int(time.mktime(datetime.strptime(t, t_format).timetuple()) * 1000))
    return tl

def queryIndex(es, datefrom, dateto, idx):
    """
    Queries the Elasticsearch index for data within the specified date range.
    Parameters:
        es: Elasticsearch connection object.
        datefrom: Start date (in milliseconds).
        dateto: End date (in milliseconds).
       x idx: Index to query.    
    Returns:
        Dictionary of results retrieved from Elasticsearch.
    """
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": datefrom,
                                "lt": dateto
                            }
                        }
                    }
                ]
            }
        }
    }
    try:
        data = scan(client=es, index=idx, query=query)
        ret_data = {}
        count = 0
        for item in data:
            ret_data[count] = item['_source']
            count += 1
        return ret_data
    except Exception as e:
        print(e)

def check_es_meta(date_from, date_to):
    """
    Checks the presence of expected hosts in the Elasticsearch meta data.
    This function queries the Elasticsearch for meta data and compares it
    with a list of expected hosts. It returns two lists:
    - Hosts that are not found in the Elasticsearch meta data.
    - Hosts that are found in Elasticsearch but not mentioned in the configurations.
    It also creates the separate alarm for each not found host. And the alarm about 
    presence in the Elasticsearch meta data third-party hosts which are not
    included in the our configurations.
    Parameters:
        es_connection: Elasticsearch connection object.
        meta_from: Start date for the meta data query (string).
        meta_to: End date for the meta data query (string).
        hosts_to_find: List of expected hosts to check against the meta data.
    Returns:
        tuple: A tuple containing two lists:
            - List of hosts not found in Elasticsearch.
            - List of hosts found in Elasticsearch but not in configurations.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    alarmType = 'missing host meta'
    alarmOnHost = alarms("Networking", 'Perfsonar', alarmType)
    alarmOnMulty = alarms('Networking', 'Sites', 'third-party hosts')
    mesh_url = "https://psconfig.aglt2.org/pub/config"
    mesh_config = psconfig.api.PSConfig(mesh_url)
    hosts_to_find = list(mesh_config.get_all_hosts())
    es = ConnectES()
    def extract_meta(dateFrom, dateTo, idx):
        # print(f'----- {dateFrom} - {dateTo} ----- ')
        time_period = GetTimeRanges(dateFrom, dateTo, 1)
        start, end = time_period[0], time_period[-1]
        data = queryIndex(es, start, end, idx)
        return pd.DataFrame(data).T
    meta_data = extract_meta(date_from, date_to, 'ps_meta')
    meta_data.set_index('host', inplace=True)
    not_found_in_es = []
    found_in_es_not_in_config = []
    for host in hosts_to_find:
        try:
            meta_data.loc[host, 'wlcg-role'] # check record for the host presence
            meta_data = meta_data.drop(host)
        except KeyError:
            not_found_in_es.append(host)
            doc = {
                'host': host,
                'alarm_type': alarmType
            }
            site = mesh_config.get_site(host)
            current_datetime = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            toHash = ','.join([host] + [site] + [current_datetime])
            doc['site'] = site if site is not None else 'site not found'
            doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
            # print(doc)
            alarmOnHost.addAlarm(body=alarmType, tags=[host] + [site], source=doc)
            print(f"\nHost not found in Elasticsearch ps-meta: {host}")
    # hosts in meta_data are found in Elasticsearch but not in configurations
    found_in_es_not_in_config.extend(meta_data.index.to_list())
    # if len(found_in_es_not_in_config)>=0:
    #     for h in found_in_es_not_in_config:
    #     # create the alarm source content
    #         doc = {'from': date_from, 'to': date_to, 'host': h}
    #         toHash = ','.join([h, date_from, date_to])
    #         doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
    #         # send the alarm with the proper message
    #         alarmOnMulty.addAlarm(body='third-party hosts', tags=[h], source=doc)
    #         print(f"\nThird-party host found in Elasticsearch ps-meta: {h}")
    #         # print(doc)
    
    return not_found_in_es, found_in_es_not_in_config


if __name__ == '__main__':
    today_date = dt.date.today()
    delta = today_date - dt.timedelta(days=2)
    time_from = dt.time(0, 0)
    time_to = dt.time(23, 59, 59)
    m_from = dt.datetime.combine(delta, time_from).strftime('%Y-%m-%d %H:%M')
    m_to = dt.datetime.combine(delta, time_to).strftime('%Y-%m-%d %H:%M')
    not_found_hosts, found_hosts = check_es_meta(m_from, m_to)
