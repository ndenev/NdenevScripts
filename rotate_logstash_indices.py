#!/usr/local/bin/python
'''
This script can be used to keep the number of ElasticSearch indices used by Logstash to a certain number.
It can be installed as a crontab to delete all indices older than X days daily.
'''
import logging
import argparse
import elasticsearch
from datetime import datetime, timedelta

logger = logging.getLogger("ES")

CLUSTER_OK = ['green', 'yellow']

def setup_logging():
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--keep', action='store', type=int, help='number of indices to keep', default=7)
    parser.add_argument('--delete', action='store_true', default=False, help='specify to really delete old indices')
    args = parser.parse_args()
    if args.keep < 1:
        raise ValueError('argument can not be less than 1')
    return args

def connect_to_es():
    logger.info("Establishing ES connection...")
    es = elasticsearch.Elasticsearch()
    if not es.ping():
        raise RuntimeError("Unable to ping ES cluster")
    logger.info("Cluster is alive")
    return es

def check_cluster_health(es):
    logger.info("Checking cluster health...")
    cluster_health = es.cluster.health()
    cluster_status = cluster_health['status']
    if cluster_status not in CLUSTER_OK:
        raise RuntimeError('Cannot proceed with cluster status: {}'.format(cluster_status))

def get_logstash_indices(es):
    logger.info("Getting logstash indices...")
    logstash_indices = [index for index in es.indices.status()['indices'] if index.startswith('logstash-')]
    logger.info("Got {} indices...".format(len(logstash_indices)))
    return logstash_indices

def get_indices_older_than_x_days(indices, days):
    delta = timedelta(days)
    indices_bydate = {datetime.strptime(idx, 'logstash-%Y.%m.%d'): idx for idx in indices}
    return [idx for idx_date, idx in indices_bydate.items() if datetime.now() - idx_date > delta]

def delete_logstash_indices(es, indices_to_delete, delete):
    logger.info("Indices to be deleted : {}".format(", ".join(indices_to_delete)))
    if delete:
        es.indices.delete("{}".format(",".join(indices_to_delete)))
        logger.info("Deleted : {}".format(", ".join(indices_to_delete)))


def main():
    setup_logging()

    args = get_args()
    num_indices_to_keep = args.keep

    es = connect_to_es()

    check_cluster_health(es)

    logstash_indices = get_logstash_indices(es)

    indices_to_delete = get_indices_older_than_x_days(logstash_indices, args.keep)

    if indices_to_delete:
        delete_logstash_indices(es, indices_to_delete, args.delete)

    logger.info("Done.") 

if __name__ == "__main__":
    main()
