# qdrantCLI

This is a cli client for qdrant.  It's designed to be a simple script that allows formatting output in json of yaml.

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

Subcommands:

  create-full-snapshot     This will create a full snapshot of the server
  create-shard-snapshot    This will create a new shard of a given collection
  delete-cluster-peer      Delete a peer in the cluster
  delete-full-snapshot     This will delete a full snapshot of the server
  delete-shard-snapshot    This will delete the snapshot for a given collection/shard
  delete-snapshot          Delete a specific snapshot
  download-snapshot        Download a specific snapshot from a collection
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
$ qdrant create-full-snapshot --help
Usage: qdrant [--core-opts] create-full-snapshot [--options] [other tasks here ...]

Docstring:
  This will create a full snapshot of the server

Options:
  -f [STRING], --format[=STRING]   output format of the response [JSON|YAML]
  -s [STRING], --server[=STRING]   Server address of qdrant default: 'http://localhost:6333
  -w [STRING], --wait[=STRING]     Wait till it finishes to return Default: True
```