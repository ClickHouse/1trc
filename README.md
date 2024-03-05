# ClickHouse - 1 trillion row challenges

Query trillion row datasets in Object Storage for a few cents using ClickHouse.

## Objective

We aim to test the cost efficiency and performance of ClickHouse in querying files in object storage.

To this end, this repository contains Pulumi code to deploy a ClickHouse cluster in a Cloud provider of a specified instance type, run a configured query against object storage and shut the cluster down. The objective is to ensure this cost is as low as possible. In most cases (assuming pricing is linear), this should also mean faster queries.

For each cloud provider the approach can differ e.g. for AWS, we use spot instances. 

## Cloud providers

- [AWS](./aws-starter/) - AWS using configurable spot instances.

## Query

Any query should not require data to be loaded into ClickHouse i.e. it should query data in object storage via functions such as the [s3Cluster function](https://clickhouse.com/docs/en/sql-reference/table-functions/s3Cluster). The query is configurable for providers.

## Datasets

### 1 trillion weather measurements

Available at `s3://coiled-datasets-rp/1trc`. Requires requester to pay. This can be queried as shown below:

```sql
SELECT * FROM s3Cluster('default','https://coiled-datasets-rp.s3.us-east-1.amazonaws.com/1trc/measurements-*.parquet', '<AWS_ACCESS_KEY_ID>', '<AWS_SECRET_ACCESS_KEY>', headers('x-amz-request-payer' = 'requester'))
```

To avoid data transfer costs, ensure you query from `us-east-1`.

## Examples

For an example, see [ClickHouse and The One Trillion Row Challenge](https://clickhouse.com/blog/clickhouse-1-trillion-row-challenge). This queries 1 trillion rows for $0.56 in S3.

## Contributing

Contributions are welcome to improve the code for a provider. This can include making providers more flexible or ensuring resources are provisoned and destroyed faster.

For simplicity we request all orchestration code be in Pulumi.
