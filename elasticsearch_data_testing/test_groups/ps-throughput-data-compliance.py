# check_data_in_es - This function checks whether expected hosts are found in the Elasticsearch throughput data the day before yesterday
#                    and compares it with the expected hosts from the provided mesh configuration.
#                    It queries Elasticsearch for specific test data (throughput)
#                    within a 24-hour time range and verifies if the hosts are listed in the index. 
#                    The function identifies hosts that are expected (in the mesh configuration) 
#                    but not found in Elasticsearch and hosts that were not found
#                    in the configuration.
#
#                    The process retrieves the hosts from the configuration, queries Elasticsearch 
#                    for the relevant test data, and counts the number of hosts found.
#                    In addition, the function can generate a plot comparing the number of hosts found 
#                    in the configuration versus those found in Elasticsearch. This information helps 
#                    maintain an accurate and up-to-date monitoring system by identifying discrepancies 
#                    between the expected and actual data.
#
#                    The function returns a DataFrame with the hosts, their status (found or not), 
#                    and lists hosts missing from both Elasticsearch and the configuration.
#
# Author: Yana Holoborodko
# Copyright 2024
import time
import datetime as dt
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import psconfig.api
import urllib3
import warnings
import hashlib
import plotly.express as px
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
        idx: Index to query.
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

def check_tests_for_host(host, mesh_config):
    """
    Classifies the host as belonging to throughput test group.
    """
    try:
        types = mesh_config.get_test_types(host)
    except Exception:
        return False
    throughput = any(test in ['throughput', 'rtt'] for test in types) # as rtt is now in ps_throughput    
    return host, throughput

def create_hosts_tests_types_grid(hosts, mesh_config):
    """
    Creates a dataframe with a list of all hosts and whether
    or not they are tested for throughput. 
    """
    host_test_type = pd.DataFrame({
    'host': list(hosts),
    'throughput': False
    })
    host_test_type = host_test_type['host'].apply(
        lambda host: pd.Series(check_tests_for_host(host, mesh_config))
    )
    host_test_type.columns = ['host', 'throughput']
    return host_test_type

def extract_data(dateFrom, dateTo, idx):
    """
    Extracts the data for a certain period and test group from Elasticsearch. 
    """
    es = ConnectES()
    # print(f'----- {dateFrom} - {dateTo} ----- ')
    time_period = GetTimeRanges(dateFrom, dateTo, 1)
    start, end = time_period[0], time_period[-1]
    data = queryIndex(es, start, end, f'ps_{idx}')
    return pd.DataFrame(data).T

def check_data_in_es(ips, mesh_config, data_from, data_to, test_type):
    """
    Checks whether all expected(mentioned in configurations) hosts 
    were found in the Elasticsearch, and returns the list of hosts which were omitted.
    Creates the plot for visualization of results.
    """
    expected_tests_types = create_hosts_tests_types_grid(ips, mesh_config)
    data = extract_data(data_from, data_to, test_type)
    hosts_data = pd.concat([data['src_host'], data['dest_host']])
    host_counts = hosts_data.value_counts().reset_index()
    host_counts.columns = ['hosts', 'count']
    host_counts.set_index('hosts', inplace=True)
    all_hosts_grid = pd.DataFrame({
    'host': ips, 
    'config': expected_tests_types[test_type],     
    'es_data': False  
    })
    all_hosts_grid.set_index('host', inplace=True)
    for host in ips:
            try:
                if host in list(host_counts.index):
                    all_hosts_grid.loc[host, 'es_data'] = True
            except KeyError:
                # meta_data.loc[host, 'wlcg-role']
                # print(f'\n---------- No records in META about {host} ----------\n')
                pass
    config_counts = all_hosts_grid['config'].value_counts()
    es_counts = all_hosts_grid['es_data'].value_counts()
    # # Display the results
    # print("\nConfig column counts:")
    # print(config_counts)
    # print("\nEs column counts:")
    # print(es_counts)
#     data = {
#     'test_group': [f'ps_{test_type}<br>from: {data_from}<br>to: {data_to}',
#                    f'ps_{test_type}<br>from: {data_from}<br>to: {data_to}'],
#     'source': ['config', 'es_data'],
#     'count': [config_counts[True], es_counts[True]]
# }
#     df = pd.DataFrame(data)
#     fig = px.bar(df, x='test_group', y='count', color='source', barmode='group',
#                  labels={'count': 'Number of Hosts', test_type: 'Test Group'},
#                  color_discrete_sequence=px.colors.qualitative.Pastel,
#                  title=f"Comparison of Hosts in Configurations vs Elasticsearch Data {test_type.upper()}")
#     fig.update_yaxes(range=[0, len(all_hosts)])
#     fig.show()    
    return config_counts, es_counts, all_hosts_grid[(all_hosts_grid['config'] == True) & (all_hosts_grid['es_data'] == False)].index.to_list(), all_hosts_grid[all_hosts_grid['config'] == False].index.to_list()

  
if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    mesh_url = "https://psconfig.aglt2.org/pub/config"
    mesh_config = psconfig.api.PSConfig(mesh_url)
    all_hosts = mesh_config.get_all_hosts()
    today_date = dt.date.today()
    delta = today_date - dt.timedelta(days=2)
    time_from = dt.time(0, 0)
    time_to = dt.time(23, 59, 59)
    time_from = dt.datetime.combine(delta, time_from).strftime('%Y-%m-%d %H:%M') # a day before yesterday
    time_to = dt.datetime.combine(delta, time_to).strftime('%Y-%m-%d %H:%M')
    # print(time_from, time_to)
    alarmOnMulty1 = alarms('Networking', 'Sites', f"tests' results for hosts not found in ps_throughput")
    alarmOnMulty2 = alarms('Networking', 'Sites', f"hosts' not found in ps_throughput tests' configurations")
    # line = '------------------------------------------------------------------'
    # print(line)
    # print(f"                           THROUGHPUT                             ")
    # print(line)
    counts_conf, counts_es, not_found_hosts_es, not_found_hosts_configs = check_data_in_es(list(all_hosts), mesh_config, time_from, time_to, 'throughput')
    doc1 = {'from': time_from, 
           'to': time_to, 
           'num_es': str(counts_conf[True]-counts_es[True]),
           'num_configs': str(counts_conf[True]),
           'percent': str(round(((counts_conf[True]-counts_es[True])/counts_conf[True])*100, 2)),
           'hosts_es': not_found_hosts_es}
    doc2 = {'from': time_from, 
           'to': time_to, 
           'all_hosts_num': str(len(all_hosts)),
           'config_num': str(len(not_found_hosts_configs)),
           'hosts_configs': not_found_hosts_configs}
    toHash1 = ','.join([str(not_found_hosts_es), time_from, time_to])
    toHash2 = ','.join([str(not_found_hosts_configs), time_from, time_to])
    doc1['alarm_id'] = hashlib.sha224(toHash1.encode('utf-8')).hexdigest()
    doc2['alarm_id'] = hashlib.sha224(toHash2.encode('utf-8')).hexdigest()
    # send the alarms with the proper message
    alarmOnMulty1.addAlarm(body='not found in the Elasticsearch', tags=['throughput'], source=doc1)
    alarmOnMulty2.addAlarm(body='not found in PWA configurations', tags=['throughput'], source=doc2)
    print(f"Hosts expected but not found in the Elasticsearch ps-throughput ({doc1['percent']}% out of included to configurations not found):\n{not_found_hosts_es}\n\n")
    print(f"Hosts not found in the configurations for ps-throughput ({doc2['config_num']}/{doc2['all_hosts_num']} not found):\n{not_found_hosts_configs}")
