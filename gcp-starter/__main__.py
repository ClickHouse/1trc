import hashlib
import os
from pathlib import Path

from requests import get
import pulumi_gcp as gcp
import importlib.resources as pkg_resources
from pulumi import Output, export, ResourceOptions, Config
import resources
from config import ConfigGenerator
from pulumi_command.remote import ConnectionArgs, Command, CopyFile

from query import ClickHouseQuery

# override if needed
private_key = Path(os.path.expanduser("~/.ssh/id_rsa")).read_text()
public_key = Path(os.path.expanduser("~/.ssh/id_rsa.pub")).read_text().strip()
zone = Config("1trc").get("zone")
region = Config("gcp").get("region")
instance_type = Config("1trc").get("instance_type")
number_instances = Config("1trc").get_int("number_instances")
password = Config("1trc").get("cluster_password")
image = Config("1trc").get("image")
query = Config("1trc").get("query")
project = Config("gcp").get("project")
# attach the project's default compute service account so ClickHouse can
# authenticate to GCS via the GCE metadata server (GCP OAuth)
default_service_account = gcp.compute.get_default_service_account(project=project)
# as seen by public service, needed for firewall rules
public_ip = get('https://api.ipify.org').text

# Create a VPC network
network = gcp.compute.Network(
    "1trc-network",
    auto_create_subnetworks=False,
    description="VPC network for 1trc ClickHouse cluster"
)

# Create a subnet
subnet = gcp.compute.Subnetwork(
    "1trc-subnet",
    ip_cidr_range="10.0.0.0/16",
    network=network.id,
    region=region,
    description="Subnet for 1trc ClickHouse cluster"
)

# Create a firewall rule for SSH and ClickHouse from requester's IP
firewall_rule = gcp.compute.Firewall(
    "1trc-firewall",
    network=network.id,
    allows=[
        gcp.compute.FirewallAllowArgs(
            protocol="tcp",
            ports=["22", "8123"]
        )
    ],
    source_ranges=[f"{public_ip}/32"],
    description="Allow SSH and ClickHouse HTTP from requester IP"
)

# Create a firewall rule for internal traffic - allow ALL protocols
internal_firewall = gcp.compute.Firewall(
    "1trc-internal-firewall",
    network=network.id,
    allows=[
        gcp.compute.FirewallAllowArgs(
            protocol="all"
        )
    ],
    source_ranges=["10.0.0.0/16"],
    description="Allow all internal traffic between nodes"
)

# Create a firewall rule for outbound traffic (allow all egress)
egress_firewall = gcp.compute.Firewall(
    "1trc-egress-firewall",
    network=network.id,
    direction="EGRESS",
    allows=[
        gcp.compute.FirewallAllowArgs(
            protocol="tcp",
            ports=["0-65535"]
        ),
        gcp.compute.FirewallAllowArgs(
            protocol="udp",
            ports=["0-65535"]
        )
    ],
    destination_ranges=["0.0.0.0/0"],
    description="Allow all outbound traffic"
)

# Create spot instances
spot_instances = []
for index in range(number_instances):
    spot_instance = gcp.compute.Instance(
        f"1trc_node_{index}",
        machine_type=instance_type,
        zone=zone,
        boot_disk=gcp.compute.InstanceBootDiskArgs(
            initialize_params=gcp.compute.InstanceBootDiskInitializeParamsArgs(
                image=image,
                size=20,
                type="hyperdisk-balanced"
            )
        ),
        network_interfaces=[gcp.compute.InstanceNetworkInterfaceArgs(
            network=network.id,
            subnetwork=subnet.id,
            access_configs=[gcp.compute.InstanceNetworkInterfaceAccessConfigArgs(
                nat_ip=None,  # ephemeral external IP
                network_tier="STANDARD"
            )]
        )],
        scheduling=gcp.compute.InstanceSchedulingArgs(
            preemptible=True,  # Spot instance
            automatic_restart=False,
            on_host_maintenance="TERMINATE"
        ),
        service_account=gcp.compute.InstanceServiceAccountArgs(
            email=default_service_account.email,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        ),
        metadata={
            "ssh-keys": f"ubuntu:{public_key}"
        },
        tags=["1trc", "spot"],
        opts=ResourceOptions(depends_on=[network, subnet])
    )
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

    # first pass - setup hosts, hostname, install ClickHouse and copy configs
    restart_deps = {}
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
        # install clickhouse before config
        install_clickhouse = Command(f"install_clickhouse_{i}", connection=connection,
                                     create=pkg_resources.read_text(resources, "install_clickhouse.sh"),
                                     opts=ResourceOptions(depends_on=set_hostname))
        # copy config for clickhouse
        config_file = gen.generate_server_configuration(i, password)
        config_hash = file_hash(config_file)
        filename = os.path.basename(config_file)
        clickhouse_file = CopyFile(f"copy_node_{i}_clickhouse_config", connection=connection,
                                   local_path=config_file,
                                   remote_path=f"/tmp/{filename}",
                                   opts=ResourceOptions(depends_on=install_clickhouse), triggers=[config_hash])
        set_clickhouse_config = Command(f"set_node_{i}_clickhouse_config", connection=connection,
                                        create=f"sudo mkdir -p /etc/clickhouse-server/config.d && sudo mv /tmp/{filename} /etc/clickhouse-server/config.d/{filename}",
                                        opts=ResourceOptions(depends_on=clickhouse_file),
                                        triggers=[config_hash])
        # copy user config file
        user_config_copy = CopyFile(f"copy_node_{i}_user_config", connection=connection,
                                    local_path=user_config_file,
                                    remote_path=f"/tmp/{user_config_filename}",
                                    opts=ResourceOptions(depends_on=install_clickhouse),
                                    triggers=[user_config_hash])
        set_user_config = Command(f"set_node_{i}_user_config", connection=connection,
                                  create=f"sudo mkdir -p /etc/clickhouse-server/users.d && sudo mv /tmp/{user_config_filename} /etc/clickhouse-server/users.d/{user_config_filename}",
                                  opts=ResourceOptions(depends_on=user_config_copy),
                                  triggers=[user_config_hash])
        restart_deps[i] = [set_clickhouse_config, set_user_config]

    # second pass - restart keeper nodes (0, 1, 2) first so they can form quorum
    instances_ready = []
    keeper_restarts = []
    for i in range(min(3, number_instances)):
        connection = ConnectionArgs(host=public_ips[i], user="ubuntu", private_key=private_key)
        restart = Command(f"restart_node_{i}_clickhouse", connection=connection,
                          create="sudo clickhouse restart || (sudo clickhouse stop; sudo clickhouse start)",
                          opts=ResourceOptions(depends_on=restart_deps[i]))
        keeper_restarts.append(restart)
        instances_ready.append(restart.stdout)

    # third pass - restart remaining nodes once the keeper quorum is up
    for i in range(3, number_instances):
        connection = ConnectionArgs(host=public_ips[i], user="ubuntu", private_key=private_key)
        restart = Command(f"restart_node_{i}_clickhouse", connection=connection,
                          create="sudo clickhouse restart || (sudo clickhouse stop; sudo clickhouse start)",
                          opts=ResourceOptions(depends_on=restart_deps[i] + keeper_restarts))
        instances_ready.append(restart.stdout)

    return instances_ready


# generate the host file based on the private ips
private_ips = Output.all(*[instance.network_interfaces[0].network_ip for instance in spot_instances])
public_ips = Output.all(*[instance.network_interfaces[0].access_configs[0].nat_ip for instance in spot_instances])
ready_instances = Output.all(private_ips, public_ips).apply(
    lambda args: configure_hosts(args[0], args[1]))

export("instance_ids", Output.all(*[instance.id for instance in spot_instances]))
export("instance_public_ips", Output.all(*[instance.network_interfaces[0].access_configs[0].nat_ip for instance in spot_instances]))

# use a non-keeper node for the query so the keeper quorum can form first
query_node_index = min(3, number_instances - 1)
query_node_public_ip = spot_instances[query_node_index].network_interfaces[0].access_configs[0].nat_ip

Output.all(query_node_public_ip, ready_instances).apply(
    lambda args: ClickHouseQuery("1trc-clickhouse-query", ip_address=args[0],
                                 number_instances=number_instances,
                                 password=password,
                                 max_timeout=300, query=query))
