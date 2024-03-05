# AWS provider

This contains the Pulumi code for provisioning AWS spot instances, configuring a ClickHouse cluster, running a configured query against S3, and immediately shutting them down.

The code aims to provision and destroy resources as quickly as possible (with the aim of minimizing costs) - improvements here are welcome.

Users may wish to experiment with different datasets and instance types to minimize query runtime AND/OR cost. We recommend [Vantage](https://instances.vantage.sh) for exploring instance costs.

See [ClickHouse and The One Trillion Row Challenge](https://clickhouse.com/blog/clickhouse-1-trillion-row-challenge) for an original blog with further details.

## Dependencies

- [Pulumi](https://www.pulumi.com/docs/install/) >= v3.107.0

## Configuration

Currently single configuration (but we may create more stacks in the future).

`Pulumi.dev.yaml`
```yaml
config:
  aws:region: us-east-1
  1trc:aws_zone: us-east-1b
  1trc:instance_type: "c7g.12xlarge"
  1trc:number_instances: 8
  # this must exist as a key-pair in AWS
  1trc:key_name: "<your_key_name>"
  # change as required
  1trc:cluster_password: "clickhouse_admin"
  # AMD ami (us-east-1)
  1trc:ami: "ami-05d47d29a4c2d19e1"
  # Intel AMI (us-east-1)
  # 1trc:ami: "ami-0c7217cdde317cfec"
  # modify for your query
  1trc:query: "SELECT station, min(measure), max(measure), round(avg(measure), 2) FROM s3Cluster('default','https://coiled-datasets-rp.s3.us-east-1.amazonaws.com/1trc/measurements-*.parquet', '<AWS_ACCESS_KEY_ID>', '<AWS_SECRET_ACCESS_KEY>', headers('x-amz-request-payer' = 'requester')) GROUP BY station ORDER BY station ASC SETTINGS max_download_buffer_size = 52428800, max_threads=128"
```

By default, this queries a trillion row dataset `https://coiled-datasets-rp.s3.us-east-1.amazonaws.com/1trc/measurements-*.parquet` of weather measurements, computing a min, max and avg per station. This data is located in `us-east-1` and requires the requester pay.

To achieve this, it:

- Deploys infrastructure to `us-east-1` to avoid data transfer charges from requester pays.
- Uses 8 * `c7g.12xlarge` (ARM) in `us-east-1b` as these were shown to be cost-effective during initial testing. See [here](https://clickhouse.com/blog/clickhouse-1-trillion-row-challenge).
- Requires an AWS key pair to be configured. This will be used to make instances available.
- Uses AMI `ami-05d47d29a4c2d19e1` - for Ubuntu for ARM.

Users can either add stacks or change the above configuration.

**Important: Ensure you modify the `1trc:query` to include the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` with which you want to query**


The default configuration will cost around $0.6 to run and should complete in around 500 seconds.

## Deploying

```bash
pulumi stack select dev
./run.sh
```

This utility script performs a `pulumi up` followed by a `pulumi down`.

## Implementation

Pulumi code deploys the following to the configured region and zone:

1. A VPC with CIDR block `10.0.0.0/16`
2. A subnet with the above VPC for all instances
3. An internet gateway so all instances have external access - needed to install ClickHouse.
4. A route table so instances can communicate and use the gateway
5. A security group that allows:
   - Port 22 for SSH from the requester's IP address. 
   - Port 8123 is also opened (HTTP) to allow queries to be run from the requester's IP address. Note: ClickHouse password is configurable.
   - All traffic between instances on all ports
   - All external outbound traffic is allowed.
   These loose security rules are permitted as the instances should be available for minutes, even on datasets with 1 trillion rows.
6. The requested number of spot instances with a `gp3` 20GiB disk.
7. A [custom resource provider](./query.py) handles the querying of ClickHouse once spot instances are deployed and the ClickHouse cluster has formed.

The code will generate configurations for the number of specified nodes under `./tmp`.
