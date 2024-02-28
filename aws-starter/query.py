import clickhouse_connect
import pulumi
from pulumi.dynamic import Resource, ResourceProvider, CreateResult, DiffResult, UpdateResult
import time


class ClickHouseQueryProvider(ResourceProvider):

    def _number_instances_ready(self, ip_address, password):
        client = clickhouse_connect.get_client(host=ip_address, username='default', password=password)
        response = client.query(
            "SELECT * FROM clusterAllReplicas('default', "
            "view(SELECT hostname() AS server, uptime() AS uptime FROM system.one)) ORDER BY server ASC",
            settings={"skip_unavailable_shards": "1"})
        return len(list(filter(lambda ready: ready, [row[1] for row in response.result_rows])))

    def _is_cluster_ready(self, props):
        pulumi.log.info(f"checking cluster is ready...")
        start_time = time.time()
        ip_address = props['ip_address']
        number_instances = props['number_instances']
        password = props['password']
        max_timeout = props['max_timeout']
        elapsed_time = 0
        while elapsed_time < max_timeout:
            num_ready = self._number_instances_ready(ip_address, password)
            if num_ready == number_instances:
                pulumi.log.info("cluster is ready!")
                return
            pulumi.log.debug(f"cluster is not ready - only {num_ready} ready")
            time.sleep(1)  # Sleep for a second and then try again
            elapsed_time = time.time() - start_time
        error_message = f"Timeout exceeded ({max_timeout} seconds) - cluster didn't form"
        pulumi.log.error(error_message)
        raise Exception(error_message)

    def _run_query(self, props):
        start_time = time.time()
        ip_address = props['ip_address']
        password = props['password']
        client = clickhouse_connect.get_client(host=ip_address, username='default', password=password)
        pulumi.log.info("running query...")
        client.query(query=props['query'])
        elapsed_time = time.time() - start_time
        pulumi.log.info(f"query took {elapsed_time}s")

    def diff(self, _id, olds, news):
        # Always return a difference to ensure the query runs on every update
        return DiffResult(changes=True)

    def update(self, id, olds, props):
        self._is_cluster_ready(props)
        self._run_query(props)
        return UpdateResult()

    def create(self, props):
        self._is_cluster_ready(props)
        self._run_query(props)
        return CreateResult(id_="1", outs={})


class ClickHouseQuery(Resource):
    def __init__(self, name, ip_address, number_instances, password, max_timeout, query, opts=None):
        super().__init__(ClickHouseQueryProvider(), name, {
            "ip_address": ip_address,
            "max_timeout": max_timeout,
            "number_instances": number_instances,
            "password": password,
            "query": query
        }, opts)
