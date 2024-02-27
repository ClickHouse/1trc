import hashlib
import os
from pathlib import Path

import pulumi_aws as aws
import importlib.resources as pkg_resources
from pulumi import Output, export, ResourceOptions
import resources
from config import ConfigGenerator
from pulumi_command.remote import ConnectionArgs, Command, CopyFile

# change as required. A key_name is required.
instance_type = "m6i.xlarge"
number_instances = 3
key_name = "dalem"
# will need to be changed pending region and architecture. Use ubuntu.
ami = "ami-0c7217cdde317cfec"
# override if needed
private_key = Path(os.path.expanduser("~/.ssh/id_rsa")).read_text()

# Create a new VPC
vpc = aws.ec2.Vpc("1trc-vpc", cidr_block="10.0.0.0/16", enable_dns_support=True, enable_dns_hostnames=True,
                  tags={"Name": "1trc-vpc"})

subnet = aws.ec2.Subnet(f"1trc-subnet", vpc_id=vpc.id,
                        cidr_block="10.0.0.0/16",
                        tags={
                            "Name": f"1trc-subnet",
                        })

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

# Create a security group allowing SSH access from anywhere
security_group = aws.ec2.SecurityGroup("my-1trc-security-group",
                                       vpc_id=vpc.id,
                                       ingress=[{"protocol": "tcp", "from_port": 22, "to_port": 22,
                                                 "cidr_blocks": ["0.0.0.0/0"]},
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
                                     })
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
    for i in range(0, len(private_ips)):
        file_path = gen.generate_host_file(i, private_ips)
        connection = ConnectionArgs(host=public_ips[i], user="ubuntu",
                                    private_key=private_key)
        # copy host files - hash allows more nodes to be added
        host_hash = file_hash(file_path)
        set_hostname = Command(f"set_node_{i}_hostname", connection=connection,
                               create=f"sudo hostnamectl set-hostname 1trc-node-{i}.localdomain")
        copy_host_file = CopyFile(f"copy_node_{i}_host_file", connection=connection,
                                  local_path=file_path, remote_path="/tmp/hosts",
                                  opts=ResourceOptions(depends_on=set_hostname), triggers=[host_hash])
        Command(f"set_{i}_host_file", connection=connection,
                create=f"sudo mv /tmp/hosts /etc/hosts", opts=ResourceOptions(depends_on=copy_host_file),
                triggers=[host_hash])
        # copy config for clickhouse
        config_file = gen.generate_clickhouse_configuration(i)
        config_hash = file_hash(file_path)
        filename = os.path.basename(config_file)
        clickhouse_file = CopyFile(f"copy_node_{i}_clickhouse_config", connection=connection,
                                   local_path=config_file,
                                   remote_path=f"/tmp/{filename}",
                                   # we use a hash to detect if we need to re-copy the file
                                   opts=ResourceOptions(depends_on=copy_host_file), triggers=[config_hash])
        set_clickhouse_config = Command(f"set_node_{i}_clickhouse_config", connection=connection,
                                        create=f"sudo mv /tmp/{filename} /etc/clickhouse-server/config.d/{filename}",
                                        opts=ResourceOptions(depends_on=clickhouse_file),
                                        triggers=[config_hash])
        # restart clickhouse - restart as maybe running
        Command(f"restart_node_{i}_clickhouse", connection=connection,
                create=f"sudo clickhouse restart",
                opts=ResourceOptions(depends_on=set_clickhouse_config), triggers=[config_hash])


# generate the host file based on the private ips
Output.all([*[instance.private_ip for instance in spot_instances]],
           [*[instance.public_ip for instance in spot_instances]]).apply(lambda args: configure_hosts(args[0], args[1]))

export("instance_ids", Output.all(*[instance.id for instance in spot_instances]))
export("instance_public_ips", Output.all(*[instance.public_ip for instance in spot_instances]))
