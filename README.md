# qdrantCLI

This is a cli client for qdrant.  It's designed to be a simple script that allows formatting output in json of yaml.
It's a work in progress so I wouldn't use this on a production system if you do not know what you are doing. Data loss will 
probably happen if you do not know what you are doing.

## Requirements

Please make sure you have the following installed before install reform

* Python 3.8
* qdrant server v.1.7.3 

## Getting Started
The first thing you need to do is install qdrant-cli.  Currently the simplest way to do this is 

```
pip install git+https://git@github.com/analogrithems/qdrant-cli.git
```

## Usage 
You can see all the commands with qdrant --list

```
$ qdrant -l
Subcommands:

  create-collection        Create a collection with all the fixins
  create-full-snapshot     This will create a full snapshot of the server
  create-payload-index     Create an index on a payload
  create-shard-snapshot    This will create a new shard of a given collection
  delete-cluster-peer      Delete a peer in the cluster
  delete-collection        Delete a specified collectoin
  delete-full-snapshot     This will delete a full snapshot of the server
  delete-payload-index     Delete an index on a payload
  delete-shard-snapshot    This will delete the snapshot for a given collection/shard
  delete-snapshot          Delete a specific snapshot
  download-snapshot        Download a specific snapshot from a collection
  get-aliases              Get a list of aliases
  get-cluster              List the cluster details for the given server
  get-collection           Return the details on a specific collection
  get-collection-cluster   List the cluster details of a given collection
  get-collections          List the collections in our qdrant server
  get-locks                Fetch a list of locks on qdrant
  get-snapshots            Get a list of snapshots for a given collection or list all snapshots for all collections if no --collection-id is given
  list-full-snapshots      This will list full snapshots of the server
  list-shard-snapshots     This will list the shards of a given collection
  recover-from-snapshot    This will try to recover a collection from the snapshot at the specified location
  recover-shard-snapshot   This will create a new shard of a given collection
  snapshot-collection      Create a snapshot of a given collection.  If you are running in a cluster make sure to snapshot each node in your cluster
```

You can get deeper details about specific commands with the format `qdrant <subcommand> --help`

*Example:* 

```
$ qdrant --help create-collection
Usage: qdrant [--core-opts] create-collection [--options] [other tasks here ...]

Docstring:
  Create a collection with all the fixins

Options:
  --server[=STRING]                         Server address of qdrant default: 'http://localhost:6333
  -a [STRING], --wal[=STRING]               WAL config
  -c STRING, --collection=STRING            The name of the collection we want to remove
  -d [STRING], --sharding-method[=STRING]   Defaults to auto, set this to custom if you will manage sharding
  -e [STRING], --sparse-vectors[=STRING]    Sparce vector configuration
  -f [STRING], --format[=STRING]            output format of the response [JSON|YAML]
  -h [STRING], --hnsw[=STRING]              HNSW config in json
  -i [STRING], --init-from[=STRING]         Which node to boot cluster from
  -o [STRING], --on-disk[=STRING]           Should we serve from disk (bool)
  -p [STRING], --optimizers[=STRING]        Custom optimzer config
  -q [STRING], --quantization[=STRING]      quantization
  -r [INT], --replication[=INT]             How many pieces of each shard
  -s [INT], --shards[=INT]                  Shard count for the collection or defaiult value if none set
  -t [STRING], --timeout[=STRING]           How long to wait for the collection to be created
  -v STRING, --vectors=STRING               The vectors configuration in JSON format
  -w [INT], --write-consistency[=INT]       How many writes to confirm
```


## Learn more

A lot of this based off of the qdrant-client api
https://python-client.qdrant.tech/qdrant_client

Also the qdrant_remote code was very helpful in understanding the api
https://github.com/qdrant/qdrant-client/blob/efb876fe3915dc5e2855f60a5617e940c84591e5/qdrant_client/qdrant_remote.py

Here's there OpenAPI link 
https://qdrant.github.io/qdrant/redoc/index.html

Snapshots - https://qdrant.tech/documentation/concepts/snapshots/

Distributed cluster - https://qdrant.tech/documentation/guides/distributed_deployment/#cluster-scaling

PyInvoke Readme - https://docs.pyinvoke.org/_/downloads/en/latest/pdf/