import os
import csv
import sys
import json
import logging
from glob import glob
from logging.config import dictConfig

import click
import neo4j
import requests


dictConfig({
    'version': 1,
    'formatters': {
        'simple': {
            'format': '%(asctime)s - %(filename)s:%(lineno)s: %(message)s',
        }
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            "stream": "ext://sys.stdout",
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        }
    }
})
logger = logging.getLogger('cli')


def create_entity_index(neo4j_client, entity_type, property_name):
    with neo4j_client.session() as session:
        session.run(f"CREATE INDEX ON :{entity_type}({property_name})")
        logger.info(
            "created index on property '%s' of entity type `%s`",
            entity_type, property_name
        )


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def main():
    pass


@main.command("import-to-neo4j")
@click.option("--url", default="bolt://localhost:7687/")
@click.option("--auth", default="neo4j:myneo4j")
@click.option("-d", "--data-dir", required=True)
@click.option("-b", "--batch-size", type=int, default=1000)
@click.option("--dropall", is_flag=True)
def import_to_neo4j(url, auth, data_dir, batch_size, dropall):
    """导入数据到 Neo4j"""
    def convert_csv_row(csv_row):
        row = {}
        for header, value in csv_row.items():
            key, *remain = header.split(':')
            if key:
                row[key] = value

        return row

    user, password = auth.split(':')
    client = neo4j.GraphDatabase.driver(url, auth=(user, password))
    if dropall:
        with client.session() as session:
            session.run('MATCH (n) DETACH DELETE n')
            logger.info("Dropped all data in Neo4j server")

    metadata = None
    metadata_file = os.path.join(data_dir, "metadata.json")
    if not os.path.exists(metadata_file):
        logger.error("Cannot found 'metadata.json' in directory '%s'", data_dir)
        sys.exit(1)

    with open(metadata_file) as f:
        metadata = json.load(f)

    query_tmpl = 'UNWIND {values} as data create (:%s {%s})'
    for entity_type, entity_file in metadata["entity-data"].items():
        create_entity_index(client, entity_type, "id")
        query = ''
        with open(os.path.join(data_dir, entity_file)) as f:
            entities, reader = [], csv.DictReader(f)
            for row in reader:
                entities.append(convert_csv_row(row))
                if not query:
                    query = query_tmpl % (
                        entity_type,
                        ','.join([f'{prop}:data.{prop}' for prop in entities[-1]])
                    )

                if len(entities) == batch_size:
                    with client.session() as session:
                        session.run(query, {'values': entities})

                    logger.info("wrote %d entities in Neo4j server", batch_size)
                    entities = []

            if entities:
                with client.session() as session:
                    session.run(query, {'values': entities})

                logger.info("wrote %d entities in Neo4j server", len(entities))

    query_tmpl = (
        'UNWIND {values} as data '
        'MATCH (a:%s {id:data.start_id}) '
        'MATCH (b:%s {id:data.end_id}) '
        'CREATE (a)-[:`%s`]->(b)'
    )
    for relation_type, relation_file in metadata.get("relation-data", {}).items():
        start_type, relation, end_type = relation_type.split('|')
        query = query_tmpl % (start_type, end_type, relation)
        with open(os.path.join(data_dir, relation_file)) as f:
            relations, reader = [], csv.DictReader(f)
            for row in reader:
                relations.append({
                    "start_id": row[":START_ID"],
                    "end_id": row[":END_ID"],
                })
                if len(relations) == batch_size:
                    with client.session() as session:
                        session.run(query, {'values': relations})

                    logger.info("wrote %d relations in Neo4j server", batch_size)
                    relations = []

            if relations:
                with client.session() as session:
                    session.run(query, {'values': relations})

                logger.info("wrote %d relations in Neo4j server", len(relations))


if __name__ == '__main__':
    main()
