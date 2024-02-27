from xml.etree.ElementTree import Element, SubElement
import os
from xml.dom import minidom
from xml.etree.ElementTree import tostring

config_dir = "./tmp"


class ConfigGenerator:

    def __init__(self, num_nodes):
        self._num_nodes = num_nodes

    def generate_host_file(self, node_num, private_ips):
        hosts_filename = os.path.join(config_dir, f"node_{node_num}", f"hosts")
        with open(hosts_filename, "w") as hosts_file:
            hosts_file.write(f"127.0.0.1 1trc-node-{node_num}.localdomain 1trc-node-{node_num}\n")
            for i, ip in enumerate(private_ips):
                hosts_file.write(
                    f"{ip} 1trc-node-{i}.localdomain 1trc-node-{i}\n")
        return hosts_filename

    def generate_clickhouse_configuration(self, node_num):
        node_dir = os.path.join(config_dir, f"node_{node_num}")
        os.makedirs(node_dir, exist_ok=True)
        node_filename = os.path.join(node_dir, f"1trc_node_{node_num}.xml")
        root = Element('clickhouse')
        SubElement(root, "listen_host").text = "::"
        SubElement(root, "listen_host").text = "0.0.0.0"
        SubElement(root, "listen_try").text = "1"
        logger = SubElement(root, "logger")
        SubElement(logger, "level").text = "debug"
        remote_servers = SubElement(root, "remote_servers")
        # single cluster
        cluster_node = SubElement(remote_servers, "default")
        shard_node = SubElement(cluster_node, "shard")
        SubElement(shard_node, "internal_replication").text = "true"
        # add replicas
        for i in range(self._num_nodes):
            replica_node = SubElement(shard_node, "replica")
            SubElement(replica_node, "host").text = f"1trc-node-{i}"
            SubElement(replica_node, "port").text = "9000"
        # config keeper for only the 1st 3 nodes
        if node_num < 3:
            keeper_server = SubElement(root, "keeper_server")
            SubElement(keeper_server, "tcp_port").text = "2181"
            SubElement(keeper_server, "server_id").text = f"{node_num}"
            coordination_settings = SubElement(keeper_server, "coordination_settings")
            SubElement(coordination_settings, "operation_timeout_ms").text = "10000"
            SubElement(coordination_settings, "session_timeout_ms").text = "30000"
            raft_configuration = SubElement(keeper_server, "raft_configuration")
            # make upto 3 keeper nodes aware of each other
            for i in range(min(self._num_nodes, 3)):
                server = SubElement(raft_configuration, "server")
                SubElement(server, "id").text = f"{i}"
                SubElement(server, "hostname").text = f"1trc-node-{i}"
                SubElement(server, "port").text = "9234"
        zookeeper = SubElement(root, "zookeeper")
        # every node needs ref to keeper
        node_id = 1 if node_num > 3 else 2
        for i in range(min(self._num_nodes, 3)):
            node_elem = SubElement(zookeeper, "node")
            if node_num == i:
                # current node is always 1
                node_elem.set("index", "1")
            else:
                node_elem.set("index", f"{node_id}")
                node_id += 1
            SubElement(node_elem, "host").text = f"1trc-node-{i}"
        xmlStr = minidom.parseString(tostring(root)).toprettyxml(indent="\t")
        with open(node_filename, "w") as node_file:
            node_file.write(xmlStr)
        return node_filename
