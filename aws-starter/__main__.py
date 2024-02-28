import hashlib
import os
from pathlib import Path

import boto3
from requests import get
import pulumi_aws as aws
import importlib.resources as pkg_resources
from pulumi import Output, export, ResourceOptions, Config
import resources
from config import ConfigGenerator
from pulumi_command.remote import ConnectionArgs, Command, CopyFile

from query import ClickHouseQuery

# override if needed
private_key = Path(os.path.expanduser("~/.ssh/id_rsa")).read_text()
availability_zone = Config("1trc").get("aws_zone")
instance_type = Config("1trc").get("instance_type")
number_instances = Config("1trc").get_int("number_instances")
key_name = Config("1trc").get("key_name")
password = Config("1trc").get("cluster_password")
ami = Config("1trc").get("ami")
# as seen by public service, needed for security group
public_ip = get('https://api.ipify.org').text
# Create a new VPC
vpc = aws.ec2.Vpc("1trc-vpc", cidr_block="10.0.0.0/16", enable_dns_support=True, enable_dns_hostnames=True,
                  tags={"Name": "1trc-vpc"})

subnet = aws.ec2.Subnet(f"1trc-subnet", vpc_id=vpc.id,
                        cidr_block="10.0.0.0/16",
                        tags={
                            "Name": f"1trc-subnet",
                        }, availability_zone=availability_zone)

internet_gateway = aws.ec2.InternetGateway(f"gw-1trc",
                                           vpc_id=vpc.id,
                                           tags={
                                               "Name": "1trc-gateway",
                                           })

# add a route to the internet gateway for the client subnet
internet_route = aws.ec2.RouteTable(f"1trc-internet-route",
                                    vpc_id=vpc.id,
                                    routes=[
                                        aws.ec2.RouteTableRouteArgs(
                                            cidr_block="0.0.0.0/0",
                                            gateway_id=internet_gateway,
                                        ),
                                    ],
                                    tags={
                                        "Name": f"1trc-public-route",
                                    })
# associate with subnet
aws.ec2.RouteTableAssociation(f"1trc-internet-route-association",
                              subnet_id=subnet.id,
                              route_table_id=internet_route.id)

# Create a security group allowing communication between nodes and outbound traffic
security_group = aws.ec2.SecurityGroup("my-1trc-security-group",
                                       vpc_id=vpc.id,
                                       ingress=[aws.ec2.SecurityGroupIngressArgs(
                                           description="SSH inbound",
                                           from_port=22, to_port=22, protocol="tcp",
                                           cidr_blocks=[f"{public_ip}/32"],
                                       ),
                                           aws.ec2.SecurityGroupIngressArgs(
                                               description="ClickHouse HTTP",
                                               from_port=8123, to_port=8123, protocol="tcp",
                                               cidr_blocks=[f"{public_ip}/32"],
                                           ),
                                           aws.ec2.SecurityGroupIngressArgs(
                                               description="Client Traffic to Clickhouse",
                                               from_port=0, to_port=65535, protocol="tcp",
                                               cidr_blocks=[vpc.cidr_block],
                                           )],
                                       egress=[
                                           aws.ec2.SecurityGroupIngressArgs(
                                               description="All outbound traffic",
                                               from_port=0, to_port=0, protocol="-1",
                                               cidr_blocks=["0.0.0.0/0"], ipv6_cidr_blocks=["::/0"],
                                           )
                                       ],
                                       tags={
                                           "Name": "my-1trc-security-group",
                                       })
# Create spot instances
spot_instances = []
for index in range(number_instances):
    spot_instance = aws.ec2.Instance(f"1trc_node_{index}",
                                     instance_type=instance_type,
                                     ami=ami,  # Replace with your desired AMI
                                     subnet_id=subnet,
                                     # Use the first public subnet for internet access
                                     vpc_security_group_ids=[security_group],
                                     # Attach security group allowing SSH
                                     root_block_device={
                                         "volume_size": 20,
                                         "volume_type": "gp3",
                                     },
                                     key_name=key_name,
                                     associate_public_ip_address=True,
                                     instance_market_options=aws.ec2.InstanceInstanceMarketOptionsArgs(
                                         market_type="spot"
                                     ),
                                     user_data=pkg_resources.read_text(resources, "install_clickhouse.sh"),
                                     tags={
                                         "Name": f"1trc-spot-instance-{index}",
                                     }, availability_zone=availability_zone)
    spot_instances.append(spot_instance)


def file_hash(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return Output.concat(hash_md5.hexdigest())


def configure_hosts(private_ips, public_ips):
    # generate host files
    gen = ConfigGenerator(number_instances)
    user_config_file = gen.generate_user_config(password)
    user_config_filename = os.path.basename(user_config_file)
    user_config_hash = file_hash(user_config_file)
    instances_ready = []
    for i in range(0, number_instances):
        file_path = gen.generate_host_file(i, private_ips)
        connection = ConnectionArgs(host=public_ips[i], user="ubuntu",
                                    private_key=private_key)
        # copy host files - hash allows more nodes to be added
        host_hash = file_hash(file_path)
        copy_host_file = CopyFile(f"copy_node_{i}_host_file", connection=connection,
                                  local_path=file_path, remote_path="/tmp/hosts",
                                  triggers=[host_hash])
        set_host_file = Command(f"set_{i}_host_file", connection=connection,
                                create=f"sudo mv /tmp/hosts /etc/hosts",
                                opts=ResourceOptions(depends_on=copy_host_file),
                                triggers=[host_hash])
        set_hostname = Command(f"set_node_{i}_hostname", connection=connection,
                               create=f"sudo hostnamectl set-hostname 1trc-node-{i}.localdomain",
                               opts=ResourceOptions(depends_on=set_host_file))
        # copy config for clickhouse
        config_file = gen.generate_server_configuration(i, password)
        config_hash = file_hash(file_path)
        filename = os.path.basename(config_file)
        clickhouse_file = CopyFile(f"copy_node_{i}_clickhouse_config", connection=connection,
                                   local_path=config_file,
                                   remote_path=f"/tmp/{filename}",
                                   # we use a hash to detect if we need to re-copy the file
                                   opts=ResourceOptions(depends_on=set_hostname), triggers=[config_hash])
        set_clickhouse_config = Command(f"set_node_{i}_clickhouse_config", connection=connection,
                                        create=f"sudo mv /tmp/{filename} /etc/clickhouse-server/config.d/{filename}",
                                        opts=ResourceOptions(depends_on=clickhouse_file),
                                        triggers=[config_hash])
        # copy user config file
        user_config_copy = CopyFile(f"copy_node_{i}_user_config", connection=connection,
                                    local_path=user_config_file,
                                    remote_path=f"/tmp/{user_config_filename}",
                                    opts=ResourceOptions(depends_on=set_hostname),
                                    triggers=[user_config_hash])
        set_user_config = Command(f"set_node_{i}_user_config", connection=connection,
                                  create=f"sudo mv /tmp/{user_config_filename} /etc/clickhouse-server/users.d/{user_config_filename}",
                                  opts=ResourceOptions(depends_on=user_config_copy),
                                  triggers=[user_config_hash])
        # restart clickhouse - restart as maybe running
        instances_ready.append(Command(f"restart_node_{i}_clickhouse", connection=connection,
                                       create=f"sudo clickhouse restart",
                                       opts=ResourceOptions(depends_on=[set_clickhouse_config, set_user_config]),
                                       triggers=[config_hash, user_config_hash]).stdout)
    return instances_ready


# generate the host file based on the private ips
ready_instances = Output.all([*[instance.private_ip for instance in spot_instances]],
                             [*[instance.public_ip for instance in spot_instances]]).apply(
    lambda args: configure_hosts(args[0], args[1]))

export("instance_ids", Output.all(*[instance.id for instance in spot_instances]))
export("instance_public_ips", Output.all(*[instance.public_ip for instance in spot_instances]))

# once our infra is ready we run the query as a pulumi resource
session = boto3.Session()
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()
Output.all(spot_instances[0].public_ip, ready_instances).apply(
    lambda args: ClickHouseQuery("1trc-clickhouse-query", ip_address=args[0],
                                 number_instances=number_instances,
                                 password=password,
                                 max_timeout=60, query=f"""
                                 SELECT station, min(measure), max(measure), round(avg(measure), 2) 
                                 FROM s3Cluster('default',
                                 'https://clickhouse-1trc.s3.us-east-1.amazonaws.com/1trc/measurements-10*.parquet', 
                                 '{current_credentials.access_key}', '{current_credentials.secret_key}') 
                                 GROUP BY station ORDER BY station ASC 
                                 SETTINGS max_download_buffer_size = 52428800, max_threads=32
                                 """))

