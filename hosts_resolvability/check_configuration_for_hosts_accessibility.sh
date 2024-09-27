#!/bin/bash
date
python check_configuration_for_hosts_accessibility.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running hosts resolvability. Exiting."
    exit $rc
fi
