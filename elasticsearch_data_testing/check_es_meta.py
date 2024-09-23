# check_es_meta - This function checks whether expected hosts are found in the Elasticsearch meta data.
#                  It queries Elasticsearch for meta data and compares it with a list of expected hosts.
#                  The purpose of this function is to identify hosts that are not found in the Elasticsearch
#                  meta data, as well as those that are found in Elasticsearch but are not mentioned in 
#                  the provided configurations. This is crucial for maintaining the integrity of the 
#                  configurations and ensuring that all relevant hosts are accounted for in the monitoring 
#                  system.
#
#                  The function returns a list of hosts that were not found in Elasticsearch and a list 
#                  of hosts that were found in Elasticsearch but are absent from the configurations.
#
# Author: Yana Holoborodko
# Copyright 2024

import time
from datetime import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import psconfig.api

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


def check_es_meta(es_connection, meta_from, meta_to, hosts_to_find):
    """
    Checks the presence of expected hosts in the Elasticsearch meta data.

    This function queries the Elasticsearch for meta data and compares it
    with a list of expected hosts. It returns two lists:
    - Hosts that are not found in the Elasticsearch meta data.
    - Hosts that are found in Elasticsearch but not mentioned in the configurations.

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
    
    def extract_meta(dateFrom, dateTo, idx):
        print(f'----- {dateFrom} - {dateTo} ----- ')
        time_period = GetTimeRanges(dateFrom, dateTo, 1)
        start, end = time_period[0], time_period[-1]
        data = queryIndex(es, start, end, idx)
        return pd.DataFrame(data).T

    meta_data = extract_meta(meta_from, meta_to, 'ps_meta')
    meta_data.set_index('host', inplace=True)
    
    not_found_in_es = []
    found_in_es_not_in_config = []

    for host in hosts_to_find:
        try:
            meta_data.loc[host, 'wlcg-role'] # checks whether host can be found in Elasticsearch
            meta_data = meta_data.drop(host)
        except KeyError:
            not_found_in_es.append(host)
    
    found_in_es_not_in_config.extend(meta_data.index.to_list()) # lists the rest of hosts in meta data which there were not mentioned/found in the configurations

    return not_found_in_es, found_in_es_not_in_config


if __name__ == '__main__':
    mesh_url = "https://psconfig.aglt2.org/pub/config"
    mesh_config = psconfig.api.PSConfig(mesh_url)
    all_hosts = mesh_config.get_all_hosts()

    es = ConnectES()
    m_from, m_to = '2024-09-19 00:00', '2024-09-19 23:59'
    not_found_hosts, found_hosts = check_es_meta(es, m_from, m_to, list(all_hosts))

    print(f"\nHosts not found in Elasticsearch ({len(not_found_hosts)}):")
    print(not_found_hosts)
    print(f"\nHosts found in Elasticsearch but not in configurations ({len(found_hosts)}):")
    print(found_hosts)

