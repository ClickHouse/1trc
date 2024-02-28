import hashlib
import os
from lxml import etree
config_dir = "./tmp"


class ConfigGenerator:

    def __init__(self, num_nodes):
        self._num_nodes = num_nodes

    @classmethod
    def prettify(cls, xml_str):
        parser = etree.XMLParser(remove_blank_text=True)
        elem = etree.XML(xml_str, parser=parser)
        return etree.tostring(elem, pretty_print=True, xml_declaration=True, encoding="UTF-8").decode("utf-8")

    def generate_host_file(self, node_num, private_ips):
        node_path = os.path.join(config_dir, f"node_{node_num}")
        os.makedirs(node_path, exist_ok=True)
        hosts_filename = os.path.join(node_path, f"hosts")
        with open(hosts_filename, "w") as hosts_file:
            hosts_file.write(f"127.0.0.1 1trc-node-{node_num}.localdomain 1trc-node-{node_num}\n")
            for i, ip in enumerate(private_ips):
                hosts_file.write(
                    f"{ip} 1trc-node-{i}.localdomain 1trc-node-{i}\n")
        return hosts_filename

    def _generate_sha256_hex(self, input_string):
        sha256_hash = hashlib.sha256(input_string.encode()).hexdigest()
        return sha256_hash

    def generate_user_config(self, password):
        filename = os.path.join(config_dir, "user_settings.xml")
        with open(filename, "w") as user_settings_file:
            hex_password = self._generate_sha256_hex(password)
            config = f"""<?xml version="1.0"?>
            <clickhouse>
                <users>
                    <default>
                        <access_management>1</access_management>
                        <password remove='1' />
                        <password_sha256_hex>{hex_password}</password_sha256_hex>
                    </default>
                </users>
            </clickhouse>"""
            user_settings_file.write(ConfigGenerator.prettify(config))
        return filename

    def generate_server_configuration(self, node_num):
        node_dir = os.path.join(config_dir, f"node_{node_num}")
        os.makedirs(node_dir, exist_ok=True)
        node_filename = os.path.join(node_dir, f"1trc_node_{node_num}.xml")
        replicas = "\n".join(
            f"<replica><port>9000</port><host>1trc-node-{i}</host></replica>" for i in range(self._num_nodes))
        # config keeper for only the 1st 3 nodes
        if node_num < 3:
            raft_configuration = "\n".join(f"<server><id>{i}</id><hostname>1trc-node-{i}</hostname>"
                                           f"<port>9234</port></server>" for i in range(min(self._num_nodes, 3)))
        # every node needs ref to keeper
        node_id = 1 if node_num > 3 else 2
        keepers = "\n".join(f"<node><index>1</index><host>1trc-node-{i}</host></node>" if node_num == i
                            else f"<node><index>1</index><host>1trc-node-{node_id}</host></node>" for i in
                            range(min(self._num_nodes, 3)))
        with open(node_filename, "w") as node_file:
            config = f"""<clickhouse>
                    <listen_host>::</listen_host>
                    <listen_host>0.0.0.0</listen_host>
                    <listen_try>1</listen_try>
                    <logger>
                        <level>debug</level>
                    </logger>
                    <remote_servers>
                        <default>
                            <shard>
                                <internal_replication>true</internal_replication>
                                {replicas}
                            </shard>
                        </default>
                    </remote_servers>
                    <keeper_server>
                        <tcp_port>2181</tcp_port>
                        <server_id>{node_num}</server_id>
                        <coordination_settings>
                            <operation_timeout_ms>10000</operation_timeout_ms>
                            <session_timeout_ms>30000</session_timeout_ms>
                        </coordination_settings>
                        <raft_configuration>
                            {raft_configuration}
                        </raft_configuration>
                    </keeper_server>
                    <zookeeper>
                        {keepers}
                    </zookeeper>
                </clickhouse>"""
            node_file.write(ConfigGenerator.prettify(config))
        return node_filename
