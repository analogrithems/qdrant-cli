# qdrantCLI

This is a cli client for qdrant.  It's designed to be a simple script that allows formatting output in json of yaml.

## Usage 
You can see all the commands with qdrant --list

```
$ qdrant -l
Subcommands:

  get-cluster              List the cluster details for the given server
  get-collection           Return the details on a specific collection
  get-collection-cluster   List the cluster details of a given collection
  get-collections          List the collections in our qdrant server
  get-locks                Fetch a list of locks on qdrant
  get-snapshots            Get a list of snapshots for a given collection or list all snapshots for all collections if no --collection-id is given
```

You can get deeper details about specific commands with the format `qdrant <subcommand> --help`

