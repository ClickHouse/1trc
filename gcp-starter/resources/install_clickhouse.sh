#!/bin/bash
curl -s https://clickhouse.com/ | sh && sudo ./clickhouse install -y || true
