import json
import os
import gzip
import sys
import time
import traceback
from typing import Optional

try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper

from yaml import dump
from urllib.parse import urlparse

from tqdm import tqdm
import qdrant_client
import requests
from invoke import task

timeout = None
SNAPSHOT_DOWNLOAD_PATH = f"./QdrantSnapshots"
## Helper functions first


def p_log(msg, severity="info"):
    """
    This function will output to the console useful information.
    """
    run_time = time.process_time()
    print("%s: %s. (%s)" % (severity.upper(), msg, run_time), file=sys.stderr)


def _scroll(
    collection,
    filter=None,
    offset=None,
    limit=None,
    server="http://localhost:6333",
):
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        # client = qdrant_client.QdrantClient(server, timeout=timeout)
        has_more = True
        scroll_args = {
            "collection_name": collection,
            "with_payload": True,
            "with_vector": True,
        }

        if filter != None:
            scroll_args["filter"] = filter
        if offset != None:
            scroll_args["offset"] = offset
        if limit != None:
            scroll_args["limit"] = int(limit)

        points = []
        while has_more:
            has_more = False
            # p_log(f"Scroll args: {scroll_args}", "DEBUG")
            # result, next = client.scroll(**scroll_args)
            headers = {"Content-Type": "application/json"}
            url = f"{server}/collections/{collection}/points/scroll"

            response = requests.post(url, json=scroll_args, headers=headers)
            response = json.loads(response.text)

            if response["result"]["next_page_offset"] != None and limit == None:
                next = response["result"]["next_page_offset"]
            else:
                next = None

            points += response["result"]["points"]

            if next:
                has_more = True
                scroll_args["offset"] = next
            else:
                print("End of scroll")

        return points
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error scrolling points on {url}")
        traceback.print_exc(file=sys.stderr)
        return -2


def _fetch_snapshot(url, download_path):
    """
    This is a gernic
    """
    try:
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get("content-length", 0))
        block_size = 1024 * 32

        with tqdm(
            total=total_size / (32 * 1024.0),
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            p_log(f"Downloading '{url}' to '{download_path}'")
            if not os.path.exists(os.path.dirname(download_path)):
                print(f"Making directory: {os.path.dirname(download_path)}")
                os.makedirs(os.path.dirname(download_path), mode=0o777, exist_ok=True)
            with open(download_path, "wb") as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)

        if total_size != 0 and progress_bar.n != total_size:
            raise RuntimeError("Could not download file")
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {url}: {e}")
        return -1
    except Exception:
        print(f"Error downloading snapshot: {url}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def get_cluster(c, server="http://localhost:6333", format="json"):
    """
    List the cluster details for the given server
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/cluster"
        response = requests.get(url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response["result"], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error fetching cluster information for: {server}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "peer": "The peer to remove from the cluster",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def delete_cluster_peer(c, peer, server="http://localhost:6333", format="json"):
    """
    Delete a peer in the cluster
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"/cluster/peer/{peer}"
        response = requests.get(url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response["result"], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error fetching cluster information for: {server}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name or id of the collection we want to get cluster information about",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def get_collection_cluster(
    c, collection, server="http://localhost:6333", format="json"
):
    """
    List the cluster details of a given collection
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/{collection}/cluster"
        response = requests.get(url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response["result"], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(
            f"Error fetching cluster collection information for: {server}/{collection}\n"
        )
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def get_collections(c, server="http://localhost:6333", format="json"):
    """
    List the collections in our qdrant server
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.get_collections()
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error fetching collections: GET {server}/collections\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name or id of the collection we want to get cluster information about",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def get_collection(c, collection, server="http://localhost:6333", format="json"):
    """
    Return the details on a specific collection
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.get_collection(collection_name=collection)
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error fetching collection: GET {server}/collections/{collection}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the collection we want to remove",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def delete_collection(c, collection, server="http://localhost:6333", format="json"):
    """
    Delete a specified collectoin
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.delete_collection(collection_name=collection)
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error deleteing collection: DELETE {server}/collections/{collection}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


def _create_collection(
    collection,
    vectors,
    shards=2,
    replication=1,
    write_consistency=2,
    on_disk=False,
    hnsw=None,
    optimizers=None,
    wal=None,
    quantization=None,
    init_from=None,
    timeout=None,
    sparse_vectors=None,
    sharding_method=None,
    server="http://localhost:6333",
):
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        collection_args = {
            "collection_name": collection,
            "vectors_config": vectors,
            "shard_number": shards,
            "replication_factor": replication,
            "write_consistency_factor": write_consistency,
            "on_disk_payload": on_disk,
        }

        if hnsw:
            collection_args["hnsw_config"] = hnsw
        if optimizers:
            collection_args["optimizers_config"] = optimizers
        if wal:
            collection_args["wal_config"] = wal
        if quantization:
            collection_args["quantization_config"] = quantization
        if timeout:
            collection_args["timeout"] = timeout
        if sparse_vectors:
            collection_args["sparse_vectors_config"] = sparse_vectors
        if sharding_method:
            collection_args["sharding_method"] = sharding_method

        response = client.create_collection(**collection_args)
        return response
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error create collection: PUT {server}/collections/{collection}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the collection we want to remove",
        "vectors": "The vectors configuration in JSON format",
        "shards": "Shard count for the collection or defaiult value if none set",
        "replication": "How many pieces of each shard",
        "write_consistency": "How many writes to confirm",
        "on_disk": "Should we serve from disk (bool)",
        "hnsw": "HNSW config in json",
        "optimizers": "Custom optimzer config",
        "wal": "WAL config",
        "quantization": "quantization",
        "init_from": "Which node to boot cluster from",
        "timeout": "How long to wait for the collection to be created",
        "sparse_vectors": "Sparce vector configuration",
        "sharding_method": "Defaults to auto, set this to custom if you will manage sharding",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=[
        "shards",
        "replication",
        "write_consistency",
        "on_disk",
        "hnsw",
        "optimizers",
        "wal",
        "quantization",
        "init_from",
        "timeout",
        "sparse_vectors",
        "sharding_method",
        "server",
        "format",
    ],
)
def create_collection(
    c,
    collection,
    vectors,
    shards=2,
    replication=1,
    write_consistency=2,
    on_disk=False,
    hnsw=None,
    optimizers=None,
    wal=None,
    quantization=None,
    init_from=None,
    timeout=None,
    sparse_vectors=None,
    sharding_method=None,
    server="http://localhost:6333",
    format="json",
):
    """
    Create a collection with all the fixins
    """
    response = _create_collection(
        collection=collection,
        vectors=vectors,
        shards=shards,
        replication=replication,
        write_consistency=write_consistency,
        on_disk=on_disk,
        hnsw=hnsw,
        optimizers=optimizers,
        wal=wal,
        quantization=quantization,
        init_from=init_from,
        timeout=timeout,
        sparse_vectors=sparse_vectors,
        sharding_method=sharding_method,
        server=server,
    )


def _get_vector_config(collection, server):
    """
    Helper function to get a current collections vector config
    """
    server = os.environ.get("QDRANT_SERVER", server)
    client = qdrant_client.QdrantClient(server, timeout=timeout)
    collection = client.get_collection(collection_name=collection)
    return collection.config.params.vectors


@task(
    help={
        "collection": "The collection to rebalance",
        "shards": "How man shards should we have this time?",
        "replicas": "How many copies should we keep you need at least 2 for redunancy",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    }
)
def rebalance(
    c,
    collection,
    shards,
    replicas,
    server="http://localhost:6333",
    format="json",
):
    """
    Rebalance is used to change a collections sharding and replica configuration
    """
    server = os.environ.get("QDRANT_SERVER", server)
    client = qdrant_client.QdrantClient(server, timeout=timeout)
    points = _scroll(collection=collection, server=server)
    vector_config = _get_vector_config(collection, server)

    upsert = []
    for point in points:
        record = {
            "upsert": {
                "batch": {
                    "ids": [point["id"]],
                    "payloads": [point["payload"]],
                    "vectors": [point["vector"]],
                }
            },
        }
        upsert.append(record)
    print(f"Upsert collection: {upsert}")

    # Make new collection to test the zerox
    collection = f"{collection}-new"
    collection_args = {
        "collection_name": collection,
        "vectors_config": {
            "size": vector_config.size,
            "distance": str(vector_config.distance),
        },
        "shard_number": shards,
        "replication_factor": replicas,
    }

    p_log(f"Trying to creating collection: {collection_args}")

    response = client.recreate_collection(**collection_args)

    p_log(f"Trying to batch upload collection: {collection_args}")
    response = client.batch_update_points(
        collection_name=collection, wait=True, update_operations=upsert
    )
    p_log(f"Done batching up points")


@task(
    help={
        "collection": "Name of the collection to retrieve from",
        "filter": "Look only for points which satisfies this conditions. If not provided - all points",
        "offset": "Start ID to read points from",
        "limit": "How many results per page",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=[
        "filter",
        "offset",
        "limit",
        "server",
        "format",
    ],
)
def scroll(
    c,
    collection,
    filter=None,
    offset=None,
    limit=None,
    server="http://localhost:6333",
    format="json",
):
    """
    Scroll request - paginate over all points which matches given filtering condition
    """
    points = _scroll(
        collection=collection, filter=filter, offset=offset, limit=limit, server=server
    )
    out_formatter(points, format)


@task(
    autoprint=False,
    help={
        "collection": "The collection to create an index for",
        "field": "Field we wish to index",
        "schema": "The schema for the index",
        "type": "Type of index",
        "order": "The order of the index",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def create_payload_index(
    c,
    collection,
    field,
    schema,
    type,
    order,
    wait=True,
    server="http://localhost:6333",
    format="json",
):
    """
    Create an index on a payload
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.create_payload_index(
            collection_name=collection,
            field_name=field,
            field_schema=schema,
            field_type=type,
            wait=(wait == True),
            ordering=order,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error creating payload index\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The collection to create an index for",
        "field": "Field we wish to index",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def delete_payload_index(
    c,
    collection,
    field,
    order,
    wait=True,
    server="http://localhost:6333",
    format="json",
):
    """
    Delete an index on a payload
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.create_payload_index(
            collection_name=collection,
            field_name=field,
            wait=(wait == True),
            ordering=order,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error deleteing payload index\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def get_aliases(c, server="http://localhost:6333", format="json"):
    """
    Get a list of aliases
    """

    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.get_aliases()
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error getting aliases\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def get_locks(c, server="http://localhost:6333", format="json"):
    """
    Fetch a list of locks on qdrant
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.get_locks()
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error fetching loks on {server}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "Give a specific collection to list snapshots for or ommit for all",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["collection", "format", "server"],
)
def get_snapshots(c, collection=None, server="http://localhost:6333", format="json"):
    """
    Get a list of snapshots for a given collection or list all snapshots for all collections if no --collection-id is given
    """

    server = os.environ.get("QDRANT_SERVER", server)

    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)

        snapshots = {}
        if collection is None:
            collections = []
            for collection in client.get_collections().collections:
                collections.append(collection.name)
        else:
            collections = [collection]

        for collection in collections:
            # response = client.list_snapshots(collection)
            headers = {"Content-Type": "application/json"}
            url = f"{server}/collections/{collection}/snapshots"
            response = requests.get(url, headers=headers)
            response = json.loads(response.text)
            snapshots[collection] = response["result"]

        out_formatter(snapshots, format)

    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error fetching snapshots on {server}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "Give a specific collection to snapshot",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def snapshot_collection(
    c, collection, wait=True, server="http://localhost:6333", format="json"
):
    """
    Create a snapshot of a given collection.
    ProTip: If you are running in a cluster make sure to snapshot each node in your cluster.
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        status = client.create_snapshot(collection_name=collection, wait=(wait == True))
        out_formatter(status, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error snapshoting collection {server}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "Give a specific collection to snapshot",
        "snapshot": "The name of the snapshot to download from the specified collection",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
    },
    optional=["server"],
)
def download_snapshot(c, collection, snapshot, server="http://localhost:6333"):
    """
    Download a specific snapshot from a collection
    """

    server = os.environ.get("QDRANT_SERVER", server)
    url = f"{server}/collections/{collection}/snapshots/{snapshot}"
    up = urlparse(url)
    _fetch_snapshot(
        url,
        f"{SNAPSHOT_DOWNLOAD_PATH}/{up.netloc}{up.path}",
    )


@task(
    autoprint=False,
    help={
        "snapshot": "The name of the snapshot to delete",
        "collection": "Collection we will delete a snapshot for",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def delete_snapshot(
    c, snapshot=None, collection=None, server="http://localhost:6333", format="json"
):
    """
    Delete a specific snapshot DELETE "{server}/collections/{collection}/snapshots/{snapshot}"
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/collections/{collection}/snapshots/{snapshot}"
        response = requests.request("DELETE", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response["result"], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception as e:
        print(f"Error deleteing snapshots: {e}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def list_full_snapshots(c, server="http://localhost:6333", format="json"):
    """
    This will list full snapshots of the server
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.list_full_snapshots()
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception as e:
        print(f"Error listing full snapshots: {e}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "snapshot": "The name of the snapshot to download from the specified collection",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
    },
    optional=["server"],
)
def download_full_snapshot(c, snapshot, server="http://localhost:6333"):
    """
    Download a full snapshot.  If running in a cluster you must snapshot each node
    """
    server = os.environ.get("QDRANT_SERVER", server)
    url = f"{server}/snapshots/{snapshot}"
    up = urlparse(url)
    _fetch_snapshot(
        url,
        f"{SNAPSHOT_DOWNLOAD_PATH}/{up.netloc}{up.path}",
    )


@task(
    autoprint=False,
    help={
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def create_full_snapshot(c, wait=True, server="http://localhost:6333", format="json"):
    """
    This will create a full snapshot of the server
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.create_full_snapshot(wait=(wait == True))
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception as e:
        print(f"Error creating full snapshots: {e}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "snapshot": "The name of the snapshot to delete",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def delete_full_snapshot(
    c, snapshot, wait=True, server="http://localhost:6333", format="json"
):
    """
    This will delete a full snapshot of the server
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.delete_full_snapshot(
            snapshot_name=snapshot, wait=(wait == True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error deleteing full snapshot: {server}/snapshots/{snapshot}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the colleciton to recover",
        "location": "The path on the file system or url to find the snapshot at",
        "priority": "One of either {replica, snapshot, no_sync} Defaults: 'replica'",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "priority", "format", "server"],
)
def recover_from_snapshot(
    c,
    collection,
    location,
    priority="replica",
    wait=True,
    server="http://localhost:6333",
    format="json",
):
    """
    This will try to recover a collection from the snapshot at the specified location
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.recover_snapshot(
            collection_name=collection,
            location=location,
            priority=priority,
            wait=(wait == True),
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error recovering collection: {collection} from snapshot: {location}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the colleciton and shard to list snapshots for",
        "shard": "The shard we want to list snapshots for",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def list_shard_snapshots(
    c, collection, shard, wait=True, server="http://localhost:6333", format="json"
):
    """
    This will list the shards of a given collection
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.list_shard_snapshots(
            collection_name=collection, shard_id=shard, wait=(wait == True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error listing shards for collection: {collection}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the colleciton to snapshot a shard of",
        "shard": "What we want to call this new shard",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def create_shard_snapshot(
    c, collection, shard, wait=True, server="http://localhost:6333", format="json"
):
    """
    This will create a new shard of a given collection
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.create_shard_snapshot(
            collection_name=collection, shard_id=shard, wait=(wait == True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error creating shard snapshot: {collection}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the colleciton remove te shard snapshot from",
        "snapshot": "The snapshot for the shard in the collection to delete",
        "shard": "The shard we want to remove",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def delete_shard_snapshot(
    c,
    collection,
    snapshot,
    shard,
    wait=True,
    server="http://localhost:6333",
    format="json",
):
    """
    This will delete the snapshot for a given collection/shard
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.delete_shard_snapshot(
            collection_name=collection,
            snapshot_id=snapshot,
            shard_id=shard,
            wait=(wait == True),
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(f"Error deleteing shard snapshot: {collection}/{shard}/{snapshot}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    autoprint=False,
    help={
        "collection": "The name of the colleciton for the shard to recover",
        "shard": "What we want to recover",
        "location": "The path on the file system or url to find the snapshot at",
        "priority": "One of either {replica, snapshot, no_sync} Defaults: 'replica'",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["wait", "format", "server"],
)
def recover_shard_snapshot(
    c,
    collection,
    shard,
    location,
    priority="replica",
    wait=True,
    server="http://localhost:6333",
    format="json",
):
    """
    This will create a new shard of a given collection
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        response = client.recover_shard_snapshot(
            collection_name=collection,
            shard_id=shard,
            location=location,
            priority=priority,
            wait=(wait == True),
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        print(
            f"Error recovering shard: {shard} for collection: {collection} from: {location}:{priority}\n"
        )
        traceback.print_exc(file=sys.stderr)
        return -2


def out_formatter(output=None, format="json"):
    if format.lower() == "json":
        print(
            json.dumps(
                output,
                default=lambda o: o.__dict__,
                sort_keys=True,
                indent=os.environ.get("QDRANT_OUTPUT_INDENT", 2),
            )
        )

    elif format.lower() == "yaml":
        print(dump(output, Dumper=Dumper))
    else:
        print("No output format selected")

    return output
