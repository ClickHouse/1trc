#!/bin/bash
curl -s https://clickhouse.com/ | sh > /dev/null 2>&1
sudo ./clickhouse install -y
