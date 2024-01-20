import io
import json
import os
import pprint
import qdrant_client
import requests
import re
import sys
import time
import traceback
import httpx

from invoke import task
from invoke.util import debug
from invoke.tasks import call

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


def p_log(msg, severity="info"):
    """
    This function will output to the console useful information.
    """
    run_time = time.process_time()
    print("%s: %s. (%s)" % (severity.upper(), msg, run_time), file=sys.stderr)

@task(
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_cluster(c, server="http://localhost:6333", format="json"):
    """
    List the cluster details for the given server
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/cluster"
        response = requests.request("GET", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response['result'], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching cluster information for: {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
        
@task(
    help={
        "peer": "The peer to remove from the cluster",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def delete_cluster_peer(c, peer, server="http://localhost:6333", format="json"):
    """
    Delete a peer in the cluster
    """

    server = os.environ.get("QDRANT_SERVER",server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"/cluster/peer/{peer}"
        response = requests.request("GET", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response['result'], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching cluster information for: {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
        
@task(
    help={
        "collection": "The name or id of the collection we want to get cluster information about",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_collection_cluster(c, collection, server="http://localhost:6333", format="json"):
    """
    List the cluster details of a given collection
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/{collection}/cluster"
        response = requests.request("GET", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response['result'], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching cluster collection information for: {server}/{collection}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
@task(
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_collections(c,  server="http://localhost:6333", format="json"):
    """
    List the collections in our qdrant server
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.get_collections()
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching collections: GET {server}/collections\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
        
@task(
    help={
        "collection": "The name or id of the collection we want to get cluster information about",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_collection(c,  collection, server="http://localhost:6333", format="json"):
    """
    Return the details on a specific collection
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.get_collection(
            collection_name=collection
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching collection: GET {server}/collections/{collection}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)


@task(
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_aliases(c, server="http://localhost:6333", format="json"):
    """
    Get a list of aliases
    """
    
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.get_aliases()
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error getting aliases\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
        
@task(
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_locks(c, server="http://localhost:6333", format="json"):
    """
    Fetch a list of locks on qdrant
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.get_locks()
        print(response)
        #out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching loks on {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)

@task(
    help={
        "collection": "Give a specific collection to list snapshots for or ommit for all",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['collection', 'format', 'server'],
)
def get_snapshots(c, collection=None, server="http://localhost:6333", format="json"):
    """
    Get a list of snapshots for a given collection or list all snapshots for all collections if no --collection-id is given
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        
        snapshots = {}
        if collection is None:
            collections = []
            for collection in client.get_collections().collections:
                collections.append(collection.name)
        else:
            collections = [collection]

        for collection in collections:
            #response = client.list_snapshots(collection)
            headers = {"Content-Type": "application/json"}
            url = f"{server}/collections/{collection}/snapshots"
            response = requests.request("GET", url, headers=headers)
            response = json.loads(response.text)
            snapshots[collection] = response['result']
        
        out_formatter(snapshots, format)
            
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error fetching snapshots on {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)

@task(
    help={
        "collection": "Give a specific collection to snapshot",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format', 'server'],
)
def snapshot_collection(c, collection, wait=True, server="http://localhost:6333", format="json"):
    """
    Create a snapshot of a given collection.  If you are running in a cluster make sure to snapshot each node in your cluster
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        status = client.create_snapshot(
            collection_name=collection,
            wait=(wait==True)
        )
        out_formatter(status, format)
            
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error snapshoting collection {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
@task(
    help={
        "collection": "Give a specific collection to snapshot",
        "snapshot": "The name of the snapshot to download from the specified collection",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format', 'server'],
)
def download_snapshot(c, collection, snapshot, wait=True, server="http://localhost:6333", format="json"):
    """
    Download a specific snapshot from a collection
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        url = f"{server}/collections/{collection}/snapshots/{snapshot}"
        response = requests.request("GET", url)
        with open(f"./{snapshot}", "wb") as f:
            f.write(response.content)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error downloading snapshot: {url}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
    
        
@task(
    help={
        "snapshot":"The name of the snapshot to delete",
        "collection": "Collection we will delete a snapshot for",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def delete_snapshot(c, snapshot=None, collection=None, server="http://localhost:6333", format="json"):
    """
    Delete a specific snapshot
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:        
        headers = {"Content-Type": "application/json"}
        url = f"{server}/collections/{collection}/snapshots/{snapshot}"
        response = requests.request("DELETE", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response["result"], format)
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error deleteing snapshots: {url}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)


@task(
    help={
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format','server']
)
def list_full_snapshots(c, server="http://localhost:6333", format="json"):
    """
    This will list full snapshots of the server
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.list_full_snapshots();
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error listing full snapshots: {url}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
        
        
@task(
    help={
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format','server']
)
def create_full_snapshot(c, wait=True, server="http://localhost:6333", format="json"):
    """
    This will create a full snapshot of the server
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.create_full_snapshot(wait=(wait==True))
        out_formatter(response, format) 
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error creating full snapshots: {url}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)


@task(
    help={
        "snapshot":"The name of the snapshot to delete",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format','server']
)
def delete_full_snapshot(c, snapshot, wait=True, server="http://localhost:6333", format="json"):
    """
    This will delete a full snapshot of the server
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.delete_full_snapshot(snapshot_name=snapshot, wait=(wait==True))
        out_formatter(response, format) 
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error deleteing full snapshot: {server}/snapshots/{snapshot}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
   
   
@task(
    help={
        "collection":"The name of the colleciton to recover",
        "location": "The path on the file system or url to find the snapshot at",
        "priority": "One of either {replica, snapshot, no_sync} Defaults: 'replica'",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait','priority', 'format','server']
)
def recover_from_snapshot(c, collection, location, priority="replica", wait=True, server="http://localhost:6333", format="json"):
    """
    This will try to recover a collection from the snapshot at the specified location
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.recover_snapshot(
            collection_name=collection,
            location=location,
            priority=priority,
            wait=(wait==True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error recovering collection: {collection} from snapshot: {location}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
   
   
@task(
    help={
        "collection":"The name of the colleciton and shard to list snapshots for",
        "shard": "The shard we want to list snapshots for",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format','server']
)
def list_shard_snapshots(c, collection, shard, wait=True, server="http://localhost:6333", format="json"):
    """
    This will list the shards of a given collection
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.list_shard_snapshots(
            collection_name=collection,
            shard_id=shard,
            wait=(wait==True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error listing shards for collection: {collection}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)


@task(
    help={
        "collection":"The name of the colleciton to snapshot a shard of",
        "shard": "What we want to call this new shard",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format','server']
)
def create_shard_snapshot(c, collection, shard, wait=True, server="http://localhost:6333", format="json"):
    """
    This will create a new shard of a given collection
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.create_shard_snapshot(
            collection_name=collection,
            shard_id=shard,
            wait=(wait==True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error creating shard snapshot: {collection}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)

 
@task(
    help={
        "collection":"The name of the colleciton remove te shard snapshot from",
        "snapshot": "The snapshot for the shard in the collection to delete",
        "shard": "The shard we want to remove",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format','server']
)
def delete_shard_snapshot(c, collection, snapshot, shard, wait=True, server="http://localhost:6333", format="json"):
    """
    This will delete the snapshot for a given collection/shard
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.delete_shard_snapshot(
            collection_name=collection,
            snapshot_id=snapshot,
            shard_id=shard,
            wait=(wait==True)
        )
        out_formatter(response, format)
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error deleteing shard snapshot: {collection}/{shard}/{snapshot}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)


@task(
    help={
        "collection":"The name of the colleciton for the shard to recover",
        "shard": "What we want to recover",
        "location": "The path on the file system or url to find the snapshot at",
        "priority": "One of either {replica, snapshot, no_sync} Defaults: 'replica'",
        "wait": "Wait till it finishes to return Default: True",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['wait', 'format','server']
)
def recover_shard_snapshot(c, collection, shard, location, priority='replica', wait=True, server="http://localhost:6333", format="json"):
    """
    This will create a new shard of a given collection
    """
    server = os.environ.get("QDRANT_SERVER",server)
    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        response = client.recover_shard_snapshot(
            collection_name=collection,
            shard_id=shard,
            location=location,
            priority=priority,
            wait=(wait==True)
        )
        out_formatter(response, format) 
    except qdrant_client.http.exceptions.ResponseHandlingException as e:
        print(f"Failed to connect to {server}: {e}")
        exit(-1)
    except Exception:
        print(f"Error recovering shard: {shard} for collection: {collection} from: {location}:{priority}\n")
        traceback.print_exc(file=sys.stdout)
        exit(-2)
   





def out_formatter(output=None, format="json"):
    if format.lower() == "json":
        print(json.dumps(
            output,
            default=lambda o: o.__dict__, 
            sort_keys=True,
            indent=os.environ.get("QDRANT_OUTPUT_INDENT", 2)
        ))
        
    elif format.lower() == "yaml":
        print(
            dump(
                output, 
                Dumper=Dumper
            )
        )
    else:
        print("No output format selected")
        
    return output



# @TODO
