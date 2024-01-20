from invoke import Collection, Program
from qdrant_cli import tasks

program = Program(namespace=Collection.from_module(tasks), version="0.1")
