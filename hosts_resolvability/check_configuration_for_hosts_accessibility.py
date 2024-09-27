# check_configuration_for_hosts_accessibility - This script verifies whether hosts defined in a mesh configuration
#                                               are resolvable via DNS. It is designed to scan all the hosts in the
#                                               provided mesh configuration, which is loaded using the psconfig API,
#                                               and checks if the hosts can be resolved by DNS queries. This can be
#                                               used to identify hosts that might have gone offline or have invalid DNS
#                                               records.
# 
#                                               The process iterates through all hosts specified in each configuration,
#                                               checks their DNS resolution status, and generates a list of hosts that 
#                                               are no longer resolvable and configurations where they 
#                                               still can be found. The output is a list of these hosts, indicating 
#                                               that they should be updated or removed from the configuration.
#
# Author: Yana Holoborodko
# Copyright 2024
import socket
import psconfig.api
import requests
import hashlib
from alarms import alarms

def host_resolvable(host):
    """
    Checks whether the host is resolvable via DNS.
    """
    try:
        socket.gethostbyname(host)
        print(f"{host} is resolvable via DNS")
        return True
    except socket.gaierror:
        print(f"{host} is NOT resolvable via DNS")
        return False

def extract_configs_from_url(mesh_url):
    """
    Fetches and extracts the configuration map from a mesh configuration URL.
    """
    try:
        response = requests.get(mesh_url)
        response.raise_for_status()
        mesh_config = psconfig.api.PSConfig(mesh_url)
        return extract_configs(mesh_config)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching mesh configuration from {mesh_url}: {e}")
        return {}

def extract_configs(mesh_config):
    """
    Extracts the configuration map from the mesh configuration.
    """
    return mesh_config.get_config_host_map()

def check_configuration_for_hosts_accessibility(config, all_conf_hosts):
    """
    Iterates through all hosts in a given configuration and checks if they are resolvable via DNS.
    Returns a list of hosts that are not resolvable and need to be updated.
    """
    hosts_to_update = []
    for h in all_conf_hosts:
        if not host_resolvable(h):
            hosts_to_update.append(h)
    msg = '\n'.join(hosts_to_update) if hosts_to_update else "All hosts are resolvable"
    print("------------------------------------------------------")
    return f"Information about following hosts in {config} configuration must be updated:\n{msg}", hosts_to_update

def main():
    mesh_url = "https://psconfig.aglt2.org/pub/config"
    configs = extract_configs_from_url(mesh_url)
    alarmType = 'Unresolvable Host'
    # create the alarm objects
    alarmOnHost = alarms("Internal Configurations' Pipeline", 'Host Accessibility', alarmType)
    
    if not configs:
        print("No configurations found or failed to load the mesh configuration.")
        return
        
    inaccessible_hosts = {}
    for config, hosts in configs.items():
        print("\n******************************************************")
        result_message, hosts_to_remove = check_configuration_for_hosts_accessibility(config, hosts)
        print(result_message)

        for host in hosts_to_remove:
            if host not in inaccessible_hosts:
                inaccessible_hosts[host] = []
            inaccessible_hosts[host].append(config)
        print("******************************************************\n")

    if inaccessible_hosts:
        print("\nSummary:")
        for host, host_configs in inaccessible_hosts.items():
            doc = {
                'host': host,
                'configurations': host_configs,
                'alarm_type': alarmType
            }
            
            toHash = ','.join([host] + host_configs)
            doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()

            # Add the alarm for this host
            alarmOnHost.addAlarm(body=alarmType, tags=[host] + host_configs, source=doc)

            # Print the summary
            print(f"Host '{host}' needs updates in configurations: {', '.join(host_configs)}")
    
    else:
        print("All hosts are resolvable. No updates needed.")
    
    return inaccessible_hosts

if __name__ == '__main__':
    hosts = main()
