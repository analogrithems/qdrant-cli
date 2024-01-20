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
        
    except Exception:
        print(f"Error fetching cluster information for: {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit()
        
        
@task(
    help={
        "collection_id": "The name or id of the collection we want to get cluster information about",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_collection_cluster(c, collection_id, server="http://localhost:6333", format="json"):
    """
    List the cluster details of a given collection
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/{collection_id}/cluster"
        response = requests.request("GET", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response['result'], format)
        
    except Exception:
        print(f"Error fetching cluster collection information for: {server}/{collection_id}\n")
        traceback.print_exc(file=sys.stdout)
        exit()
        
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
        headers = {"Content-Type": "application/json"}
        url = f"{server}/collections"
        response = requests.request("GET", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response['result'], format)
        
    except Exception:
        print(f"Error fetching collections: GET {server}/collections\n")
        traceback.print_exc(file=sys.stdout)
        exit()
        
        
@task(
    help={
        "collection_id": "The name or id of the collection we want to get cluster information about",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['format', 'server'],
)
def get_collection(c,  collection_id, server="http://localhost:6333", format="json"):
    """
    Return the details on a specific collection
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        headers = {"Content-Type": "application/json"}
        url = f"{server}/collections/{collection_id}"
        response = requests.request("GET", url, headers=headers)
        response = json.loads(response.text)
        out_formatter(response['result'], format)
        
    except Exception:
        print(f"Error fetching collection: GET {server}/collections/{collection_id}\n")
        traceback.print_exc(file=sys.stdout)
        exit()


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
        
    except Exception:
        print(f"Error fetching loks on {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit()

@task(
    help={
        "collection_id": "Give a specific collection to list snapshots for or ommit for all",
        "server": "Server address of qdrant default: 'http://localhost:6333",
        "format": "output format of the response [JSON|YAML]",
    },
    optional=['collection_id', 'format', 'server'],
)
def get_snapshots(c, collection_id=None, server="http://localhost:6333", format="json"):
    """
    Get a list of snapshots for a given collection or list all snapshots for all collections if no --collection-id is given
    """
    
    server = os.environ.get("QDRANT_SERVER",server)

    try:
        client = qdrant_client.QdrantClient(server,timeout=1000)
        
        
        if collection_id is None:
            collections = []
            for collection in client.get_collections().collections:
                collections.append(collection.name)
        else:
            collections = [collection_id]

        for collection in collections:
            #response = client.list_snapshots(collection)
            headers = {"Content-Type": "application/json"}
            url = f"{server}/collections/{collection}/snapshots"
            response = requests.request("GET", url, headers=headers)
            response = json.loads(response.text)
            out_formatter(response['result'], format)
            
        
    except Exception:
        print(f"Error fetching snapshots on {server}\n")
        traceback.print_exc(file=sys.stdout)
        exit()

   
def out_formatter(output=None, format="json"):
    if format.lower() == "json":
        print(json.dumps(
            output,
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
