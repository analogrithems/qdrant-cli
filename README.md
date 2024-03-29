# qdrant-client + pyInvoke = qdrantCLI

This is a cli client for qdrant.  It's designed to be a simple script that allows formatting output in json or yaml.
It's a work in progress so I wouldn't use this on a production system if you do not know what you are doing. Data loss will
probably happen if you do not know what you are doing.

It makes use of pyinvoke to just map the qdrant-client functions to subcommands of pyInvoke.

See: https://qdrant.tech/documentation/faq/qdrant-fundamentals/?selector=aHRtbCA%2BIGJvZHkgPiBkaXY6bnRoLW9mLXR5cGUoMSkgPiBzZWN0aW9uID4gZGl2ID4gZGl2ID4gZGl2Om50aC1vZi10eXBlKDIpID4gYXJ0aWNsZSA%2BIHA6bnRoLW9mLXR5cGUoMTYp&q=upgrade

### Versioning
#### Do you support downgrades?
We do not support downgrading a cluster on any of our products. If you deploy a newer version of Qdrant, your data is automatically migrated to the newer storage format. This migration is not reversible.

#### How do I avoid issues when updating to the latest version?
We only guarantee compatibility if you update between consecutive versions. You would need to upgrade versions one at a time: 1.1 -> 1.2, then 1.2 -> 1.3, then 1.3 -> 1.4.

#### Do you guarantee compatibility across versions?
In case your version is older, we only guarantee compatibility between two consecutive minor versions. This also applies to client versions. Ensure your client version is never more than one minor version away from your cluster version. While we will assist with break/fix troubleshooting of issues and errors specific to our products, Qdrant is not accountable for reviewing, writing (or rewriting), or debugging custom code.


## Requirements

Please make sure you have the following installed before install reform

* Python 3.8+
* qdrant server v.1.7.3+

### Python Deps

* requests - common python module that you probably already have
* qdrant-client - partial wrapper around the qdrant-api
* pyinvoke - provides the simple task format
* tqdm - Used for progress bar in downloading snapshots

## Getting Started
The first thing you need to do is install qdrant-cli.  Currently the simplest way to do this is

```
pip install git+https://github.com/analogrithems/qdrant-cli.git
```


### port-forward

You will may need to port-forward to kubernetes to run these commands

```
$ kubectl port-forward -n qdrant svc/qdrant 6333 6334
```

Then set your QDRANT_SERVER environment variable to

```
$ export QDRANT_SERVER=http://localhost:6333
```

## Helpful Commands
In addition to the standard qdrant-client api calls, we've also creates some of our own such as `rebalance-cluster`


### Rebalance Cluster
This command will copy all collections from `--src` server to `--dest` server while setting the shards to 10 and replicas to 3

```
 $ qdrant rebalance-cluster --shards 10 --replicas 3 --src http://qdrant-yurts.default:6333 --dest http://qdrant.qdrant:6333
```

### Snapshot Cluster
The snapshot-cluster command will go to each collection on each node and snapshot it.  It then downloads the snapshots to a local file structure for restoring,

```
$ qdrant create-cluster-snapshot --server http://qdrant.qdrant:6333
```

## Usage
You can see all the commands with qdrant --list

```
$ qdrant -l
Subcommands:

  create-cluster-snapshot   This will create a snapshot of each collection on each node in the cluster
  create-collection         Create a collection with all the fixins
  create-full-snapshot      This will create a full snapshot of the server
  create-payload-index      Create an index on a payload
  create-shard-snapshot     This will create a new shard of a given collection
  delete-all-collections    delete-all-collections - this delete nuke every collection in your qdrant server.
  delete-cluster-peer       Delete a peer in the cluster
  delete-collection         Delete a specified collectoin
  delete-full-snapshot      This will delete a full snapshot of the server
  delete-payload-index      Delete an index on a payload
  delete-shard-snapshot     This will delete the snapshot for a given collection/shard
  delete-snapshot           Delete a specific snapshot DELETE "{server}/collections/{collection}/snapshots/{snapshot}"
  download-full-snapshot    Download a full snapshot.  If running in a cluster you must snapshot each node
  download-snapshot         Download a specific snapshot from a collection
  get-aliases               Get a list of aliases
  get-cluster               List the cluster details for the given server
  get-collection            Return the details on a specific collection
  get-collection-cluster    List the cluster details of a given collection
  get-collections           List the collections in our qdrant server
  get-locks                 Fetch a list of locks on qdrant
  get-snapshots             Get a list of snapshots for a given collection or list all snapshots for all collections if no --collection-id is given
  list-full-snapshots       This will list full snapshots of the server
  list-shard-snapshots      This will list the shards of a given collection
  migrate-node              migrate_node - This will snapshot and restore all collections from a src server to a destination server
  rebalance                 Rebalance is used to change a collections sharding and replica configuration
  rebalance-cluster         Rebalance all collections in the cluster.
  recover-from-snapshot     This will try to recover a collection from the snapshot at the specified location
  recover-shard-snapshot    This will create a new shard of a given collection
  scroll                    Scroll request - paginate over all points which matches given filtering condition
  snapshot-collection       Create a snapshot of a given collection.
```

You can get deeper details about specific commands with the format `qdrant <subcommand> --help`

*Example:*

```
$ qdrant --help create-collection
Usage: qdrant [--core-opts] create-collection [--options] [other tasks here ...]

Docstring:
  Create a collection with all the fixins

Options:
  --server[=STRING]                         Server address of qdrant default: 'http://localhost:6333'
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

The responses are in json by default you if you pass the --format=yaml flag it will return results in YAML

```
$ qdrant get-cluster
{
  "consensus_thread_status": {
    "consensus_thread_status": "working",
    "last_update": "2024-01-21T00:40:12.487643457Z"
  },
  "message_send_failures": {
    "http://qdrant-0.qdrant-headless:6335/": {
      "count": 28,
      "latest_error": "Error in closure supplied to transport channel pool: status: Unavailable, message: \"Failed to connect to http://qdrant-0.qdrant-headless:6335/, error: transport error\", details: [], metadata: MetadataMap { headers: {} }"
    }
  },
  "peer_id": 7875359758390565,
  "peers": {
    "1237494046554172": {
      "uri": "http://qdrant-0.qdrant-headless:6335/"
    },
    "3958396421676712": {
      "uri": "http://qdrant-2.qdrant-headless:6335/"
    },
    "6172692231348117": {
      "uri": "http://qdrant-1.qdrant-headless:6335/"
    },
    "7875359758390565": {
      "uri": "http://qdrant-3.qdrant-headless:6335/"
    }
  },
  "raft_info": {
    "commit": 24605,
    "is_voter": true,
    "leader": 3958396421676712,
    "pending_operations": 0,
    "role": "Follower",
    "term": 28
  },
  "status": "enabled"
}
```

or in YAML

```
$ qdrant get-cluster --format=yaml
consensus_thread_status:
  consensus_thread_status: working
  last_update: '2024-01-21T00:43:26.380192898Z'
message_send_failures:
  http://qdrant-0.qdrant-headless:6335/:
    count: 28
    latest_error: 'Error in closure supplied to transport channel pool: status: Unavailable,
      message: "Failed to connect to http://qdrant-0.qdrant-headless:6335/,
      error: transport error", details: [], metadata: MetadataMap { headers: {} }'
peer_id: 7875359758390565
peers:
  '1237494046554172':
    uri: http://qdrant-0.qdrant-headless:6335/
  '3958396421676712':
    uri: http://qdrant-2.qdrant-headless:6335/
  '6172692231348117':
    uri: http://qdrant-1.qdrant-headless:6335/
  '7875359758390565':
    uri: http://qdrant-3.qdrant-headless:6335/
raft_info:
  commit: 24605
  is_voter: true
  leader: 3958396421676712
  pending_operations: 0
  role: Follower
  term: 28
status: enabled
```

## Learn more

A lot of this based off of the qdrant-client api
[https://python-client.qdrant.tech/qdrant_client](https://python-client.qdrant.tech/qdrant_client)

Also the qdrant_remote code was very helpful in understanding the api
[https://github.com/qdrant/qdrant-client/blob/efb876fe3915dc5e2855f60a5617e940c84591e5/qdrant_client/qdrant_remote.py](https://github.com/qdrant/qdrant-client/blob/efb876fe3915dc5e2855f60a5617e940c84591e5/qdrant_client/qdrant_remote.py)

Here's there OpenAPI link
[https://qdrant.github.io/qdrant/redoc/index.html](https://qdrant.github.io/qdrant/redoc/index.html)

Snapshots - [https://qdrant.tech/documentation/concepts/snapshots/](https://qdrant.tech/documentation/concepts/snapshots/)

Distributed cluster - [https://qdrant.tech/documentation/guides/distributed_deployment/#cluster-scaling](https://qdrant.tech/documentation/guides/distributed_deployment/#cluster-scaling)

PyInvoke Readme - [https://docs.pyinvoke.org/_/downloads/en/latest/pdf/](https://docs.pyinvoke.org/_/downloads/en/latest/pdf/)