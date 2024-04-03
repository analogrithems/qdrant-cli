import json
import os
import gzip
import logging
import sys
import time
import traceback
import asyncio

import boto3

from typing import Optional
from datetime import date

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

timeout = 10000
SNAPSHOT_DOWNLOAD_PATH = f"./QdrantSnapshots"
logger = logging.getLogger("qdrantCLI")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter("%(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
## Helper functions first


def p_log(msg, severity="debug"):
    """
    This function will output to the console useful information.
    """
    run_time = time.process_time()
    logger.log(getattr(logging, severity.upper()), f"{msg} | {run_time}")


def chunks(xs, n):
    n = max(1, n)
    return (xs[i : i + n] for i in range(0, len(xs), n))


def _scroll(
    collection: str,
    limit: Optional[int] = None,
    filter: Optional[dict] = None,
    offset: Optional[int | str] = None,
    page_size: Optional[int] = 200,
    server: Optional[str] = "http://localhost:6333",
):
    """
    _scroll: Internal helper that bypasses the python client.

    The python client is missing the `next_page_offset` field in the response, so we need to use the raw API.

    Borrowed from MJ Berends
    """
    headers = {"Content-Type": "application/json"}
    url = f"{server}/collections/{collection}/points/scroll"

    if limit and page_size and page_size > limit:
        page_size = None
        logger.warning("page_size is greater than limit. Using limit instead.")

    try:
        has_more = True
        scroll_args = {
            "collection_name": collection,
            "with_payload": True,
            "with_vector": True,
        }

        if filter is not None:
            scroll_args["filter"] = filter
        if offset is not None:
            scroll_args["offset"] = offset
        if limit is not None:
            if page_size and page_size < limit:
                scroll_args["limit"] = page_size
            else:
                scroll_args["limit"] = limit
        elif page_size:
            scroll_args["limit"] = page_size

        total_points = 0

        while has_more:
            if limit and total_points >= limit:
                return

            elif limit and page_size and (total_points + page_size) >= limit:
                # last page
                scroll_args["limit"] = limit - page_size

            has_more = False
            response = requests.post(url, json=scroll_args, headers=headers)
            response = json.loads(response.text)

            if response["result"]["next_page_offset"] is not None:
                next = response["result"]["next_page_offset"]
            else:
                next = None

            total_points += len(response["result"]["points"])

            yield response["result"]["points"]

            if next:
                has_more = True
                scroll_args["offset"] = next

        logger.debug(f"_scroll Found {total_points} points in {collection}", "info")
    except Exception as exc:
        logger.exception(exc)
        logger.error(f"Error scrolling points on {url}")


async def _fetch_snapshot(url, download_path, zip=False):
    """
    This is a gernic
    """
    try:
        loop = asyncio.get_event_loop()
        future1 = loop.run_in_executor(None, requests.get, url)
        response = await future1
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
                logger.debug(f"Making directory: {os.path.dirname(download_path)}")
                os.makedirs(os.path.dirname(download_path), mode=0o777, exist_ok=True)
            if zip:
                with gzip.open(f"{download_path}.gz", "wb") as file:
                    for data in response.iter_content(block_size):
                        progress_bar.update(len(data))
                        file.write(data)
            else:
                with open(download_path, "wb") as file:
                    for data in response.iter_content(block_size):
                        progress_bar.update(len(data))
                        file.write(data)

        if total_size != 0 and progress_bar.n != total_size:
            raise RuntimeError("Could not download file")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Failed to connect to {url}: {e}")
        return -1
    except Exception:
        logger.error(f"Error downloading snapshot: {url}\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error fetching cluster information for: {server}\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error fetching cluster information for: {server}\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error fetching collections: GET {server}/collections\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
            f"Error fetching collection: GET {server}/collections/{collection}\n"
        )
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
            f"Error deleteing collection: DELETE {server}/collections/{collection}\n"
        )
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
            f"Error create collection: PUT {server}/collections/{collection}\n"
        )
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
        "overwrite": "Reuse the collections name Default to false and make {collection}-new",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    }
)
def rebalance(
    c,
    collection,
    shards,
    replicas,
    overwrite=False,
    server="http://localhost:6333",
    format="json",
):
    """
    Rebalance is used to change a collections sharding and replica configuration
    """
    server = os.environ.get("QDRANT_SERVER", server)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        points = _scroll(collection=collection, server=server, limit=1000)
        vector_config = _get_vector_config(collection, server)

        # Make new collection to test the zerox
        if overwrite == False:
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
        # @TODO get some error checking in here
        response = client.recreate_collection(**collection_args)

        # @TODO get some error checking in here
        response = client.upsert(collection_name=collection, wait=True, points=points)
        p_log(f"Done batching up points")
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error rebalancing collection: {collection}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    help={
        "shards": "How many shards should we have this time?",
        "replicas": "How many copies should we keep you need at least 2 for redunancy",
        "overwrite": "Reuse the collections name Default to false and make {collection}-new",
        "src": "Server address of qdrant reads default: 'http://localhost:6333'",
        "dest": "Server address of qdrant writes default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    }
)
def rebalance_cluster(
    c,
    shards,
    replicas,
    overwrite=True,
    src="http://localhost:6333",
    dest="http://localhost:6333",
    format="json",
):
    """
    Rebalance all collections in the cluster.
    Warning: This is crazy dangerous, do not cancel or interrupt
    """
    server = os.environ.get("QDRANT_SERVER", src)
    try:
        client = qdrant_client.QdrantClient(server, timeout=timeout)
        dest_client = qdrant_client.QdrantClient(dest, timeout=timeout)
        collections = client.get_collections()
        total = len(collections.collections)
        _count = 0
        # for scrolling src
        headers = {"Content-Type": "application/json"}
        for collection in collections.collections:
            _count += 1
            collection = collection.name
            url = f"{server}/collections/{collection}/points/scroll"
            p_log(f"Rebalancing collection: {collection} ({_count}/{total})", "info")

            try:
                p_log(f"Fetching vector_config for: {collection} ({_count}/{total})")
                vector_config = _get_vector_config(collection, server)

                # Make new collection to test the zerox
                if overwrite == False:
                    collection = f"{collection}-new"
                else:
                    logger.warn(
                        f"Overriting collection: {collection} ({_count}/{total})"
                    )

                collection_args = {
                    "collection_name": collection,
                    "vectors_config": {
                        "size": vector_config.size,
                        "distance": str(vector_config.distance),
                    },
                    "shard_number": shards,
                    "replication_factor": replicas,
                }

                p_log(
                    f"Trying to recreate collection: {collection_args} ({_count}/{total})",
                    "info",
                )
                response = dest_client.recreate_collection(**collection_args)
                p_log(
                    f"Recreate Collection response: {response} ({_count}/{total})",
                    "info",
                )

                p_log(f"Fetching points for: {collection} ({_count}/{total})")
                # Fetching all points is way too heavy, we should chunk results over 1k
                try:
                    has_more = True
                    scroll_args = {
                        "collection_name": collection,
                        "with_payload": True,
                        "with_vector": True,
                        "limit": 4000,
                    }

                    points = []
                    points_total = 0
                    while has_more:
                        has_more = False
                        response = requests.post(url, json=scroll_args, headers=headers)
                        response = json.loads(response.text)

                        if response["result"]["next_page_offset"] != None:
                            next = response["result"]["next_page_offset"]
                        else:
                            next = None

                        if next:
                            has_more = True
                            scroll_args["offset"] = next

                        points = response["result"]["points"]
                        if len(points) < 1:
                            continue

                        points_total += len(points)

                        p_log(
                            f"chunking another {len(points)} points in {collection}",
                            "info",
                        )
                        response = dest_client.upsert(
                            collection_name=collection, wait=True, points=points
                        )

                    p_log(f"{points_total} total points copied", "info")

                except qdrant_client.http.exceptions.ResponseHandlingException as e:
                    logger.error(f"Failed to connect to {server}: {e}")
                    return -1
                except Exception:
                    logger.error(f"Error scrolling points on {url}")
                    traceback.print_exc(file=sys.stderr)
                    return -2

                p_log(
                    f"... Done Rebalancing collection: {collection} ({_count}/{total})",
                    "info",
                )

            except Exception:
                logger.error(
                    f"Error rebalancing cluster collection: {collection} ({_count}/{total})\n"
                )
                traceback.print_exc(file=sys.stderr)
                return -2

    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error rebalancing cluster collection on server: {server}\n")
        traceback.print_exc(file=sys.stderr)
        return -2


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
            wait=wait,
            ordering=order,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error creating payload index\n")
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
            wait=wait,
            ordering=order,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error deleteing payload index\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error getting aliases\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error fetching loks on {server}\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error fetching snapshots on {server}\n")
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
        status = client.create_snapshot(collection_name=collection, wait=wait)
        out_formatter(status, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error snapshoting collection {server}\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception as e:
        logger.error(f"Error deleteing snapshots: {e}\n")
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
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception as e:
        logger.error(f"Error listing full snapshots: {e}\n")
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
        response = client.create_full_snapshot(wait=wait)
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception as e:
        logger.error(f"Error creating full snapshots: {e}\n")
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
        response = client.delete_full_snapshot(snapshot_name=snapshot, wait=wait)
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error deleteing full snapshot: {server}/snapshots/{snapshot}\n")
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
            wait=wait,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
            f"Error recovering collection: {collection} from snapshot: {location}\n"
        )
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
            collection_name=collection, shard_id=shard, wait=wait
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error listing shards for collection: {collection}\n")
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
            collection_name=collection, shard_id=shard, wait=wait
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(f"Error creating shard snapshot: {collection}\n")
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
            snapshot_name=snapshot,
            shard_id=shard,
            wait=wait,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
            f"Error deleteing shard snapshot: {collection}/{shard}/{snapshot}\n"
        )
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
            wait=wait,
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        logger.error(f"Failed to connect to {server}: {e}")
        return -1
    except Exception:
        logger.error(
            f"Error recovering shard: {shard} for collection: {collection} from: {location}:{priority}\n"
        )
        traceback.print_exc(file=sys.stderr)
        return -2


@task(
    help={
        "src": "The source server url ",
        "dest": "The destination server url",
        "collection": "Only migrate a specific collection",
    },
)
def migrate_node(c, src, dest, collection=None):
    """
    migrate_node - This will snapshot and restore all collections from a src server to a destination server
    if using in a cluster run this for each node.

    """
    client = qdrant_client.QdrantClient(src, timeout=timeout)

    collections = client.get_collections()
    total = len(collections.collections)
    _count = 0
    for _collection in collections.collections:
        _count -= 1
        if collection and collection != _collection.name:
            continue

        p_log(
            f"Starting to migrate collection: {_collection.name} ({_count}/{total})",
            "info",
        )

        try:
            dest_client = qdrant_client.QdrantClient(dest, timeout=timeout)

            p_log(
                f"\tCreating snapshot for {src}/collections/{_collection.name} ({_count}/{total})",
                "info",
            )
            snapshot_info = client.create_snapshot(
                collection_name=_collection.name, wait=True
            )
            snapshot_url = (
                f"{src}/collections/{_collection.name}/snapshots/{snapshot_info.name}"
            )
            snapshot_name = os.path.basename(snapshot_url)

            url = f"{dest}/collections/{_collection.name}/snapshots/upload?priority=snapshot"
            p_log(f"\tRestoring to: {url} ({_count}/{total})")
            dest_client.recover_snapshot(
                collection_name=_collection.name,
                location=snapshot_url,
                priority="snapshot",
                wait=True,
            )
            p_log(f"\tFinished with restore! ({_count}/{total})\n")

        except Exception:
            p_log(
                f"Error migrating {dest}/collections/{_collection.name} ({_count}/{total})\n\n\n"
            )
            traceback.print_exc(file=sys.stdout)
            continue


@task(
    autoprint=False,
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=["format", "server"],
)
def delete_all_collections(c, server="http://localhost:6333", format="json"):
    """
    delete-all-collections - this delete nuke every collection in your qdrant server.

    """
    server = os.environ.get("QDRANT_SERVER", server)
    client = qdrant_client.QdrantClient(server, timeout=timeout)

    collections = client.get_collections()
    total = len(collections.collections)
    _count = 0
    for collection in collections.collections:
        _count += 1
        p_log(
            f"Starting to delete collection: {collection.name} ({_count}/{total})",
            "info",
        )
        try:
            config = client.delete_collection(collection.name)

        except Exception:
            logger.error(f"Error deleteing {collection.name} ({_count}/{total})\n\n\n")
            traceback.print_exc(file=sys.stdout)
            return -3


"""
GET SESSION DETAILS
"""


def assumed_role_session():
    role_arn = os.getenv("AWS_ROLE_ARN")
    web_identity_token = None
    with open(os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE"), "r") as content_file:
        web_identity_token = content_file.read()

    role = boto3.client("sts").assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName="assume-role",
        WebIdentityToken=web_identity_token,
    )
    credentials = role["Credentials"]
    aws_access_key_id = credentials["AccessKeyId"]
    aws_secret_access_key = credentials["SecretAccessKey"]
    aws_session_token = credentials["SessionToken"]
    return boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
    )


@task(
    autoprint=False,
    help={
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333'",
        "format": "output format of the response [JSON|YAML]",
        "bucket": "The bucket to store the snapshot in. (Optional)",
        "bucket_path": "The path in the bucket to store the snapshot in",
    },
    optional=["wait", "format", "server", "bucket", "bucket_path"],
)
def create_cluster_snapshot(
    c,
    wait=True,
    server="http://localhost:6333",
    format="json",
    bucket=None,
    bucket_path="",
):
    """
    This will create a snapshot of each collection on each node in the cluster
    and then return it to the user in a snapshot file.
    """
    client = qdrant_client.QdrantClient(server, timeout=timeout)
    loop = asyncio.get_event_loop()
    aws_session = assumed_role_session()
    s3_client = aws_session.client("s3")
    collections = client.get_collections()
    total = len(collections.collections)
    _count = 0
    for _collection in collections.collections:
        _count += 1
        p_log(
            f"Starting to snapshot collection: {_collection.name} ({_count}/{total})",
            "info",
        )

        for id, node in client.http.cluster_api.cluster_status().result.peers.items():
            try:
                node = node.uri.replace("6335", "6333").strip("/")
                node_client = qdrant_client.QdrantClient(node, timeout=timeout)

                snapshot_info = node_client.create_snapshot(
                    collection_name=_collection.name, wait=True
                )
                snapshot_name = snapshot_info.name
                snapshot_url = (
                    f"{node}/collections/{_collection.name}/snapshots/{snapshot_name}"
                )
                p_log(
                    f"Snapshot created for: {node}/collections/{_collection.name} ({_count}/{total})",
                    "info",
                )
                _t = time.localtime()
                _file = f"{SNAPSHOT_DOWNLOAD_PATH}/{node.replace('http://', '').replace(':', '_')}/collections/{_collection.name}/snapshots/{snapshot_name}"
                _upload_path = f"{bucket_path}/{node.replace('http://', '').replace(':', '_')}/collections/{_collection.name}/snapshots/{snapshot_name}"

                loop.run_until_complete(_fetch_snapshot(snapshot_url, _file, True))
                _file = f"{_file}.gz"
                if bucket and bucket_path:
                    try:
                        s3_client.upload_file(_file, bucket, _upload_path)
                        os.unlink(_file)
                    except Exception:
                        p_log(
                            f"Error uploading snapshot for: {node.replace('http://', '').replace(':', '_')}/collections/{_collection.name} to S3://{bucket}/{_upload_path} ({_count}/{total})",
                            "error",
                        )
                        traceback.print_exc(file=sys.stdout)
            except Exception:
                p_log(
                    f"Error creating snapshot for: {node}/collections/{_collection.name} ({_count}/{total})",
                    "error",
                )
                traceback.print_exc(file=sys.stdout)


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
        logger.error("No output format selected")

    return output
