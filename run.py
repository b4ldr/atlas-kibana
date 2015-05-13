#!/usr/bin/env python
import pdb
import sys
import json
import probe
import logging
import requests
import argparse
import measuerments
import elasticsearch
import elasticsearch.helpers

#index_items(client, items, index, doc_type, chunk_size)
def index_items(hosts, actions, index, doc_type, chunk_size=200):
    client = elasticsearch.Elasticsearch(hosts=hosts)
    for ok, result in elasticsearch.helpers.streaming_bulk(
        client,
        actions,
        index=index,
        doc_type=doc_type,
        chunk_size=chunk_size):
        action, result = result.popitem()
        if not ok:
            doc_id = result['_id']
            logger.warning('Could not {0} document {1}: {2}'.format(action, doc_id, result))

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v', action='count')
    parser.add_argument('-A', '--api',
            default='https://atlas.ripe.net/api/v1/measurement-latest/',
            help='api url to use api/v1/measurement-latest' )
    parser.add_argument('-I', '--index', default='testing-atlas-index',
            help="Index to store probe and measurement data in. Defaults to 'atlas-index'")
    parser.add_argument('measurement_ids',  nargs='+', 
            help="measurement(s) to index in Elasticsearch")
    return parser.parse_args()

def set_log_level(verbose):
    '''set the logger level'''
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose > 1:
        level = logging.DEBUG
    logging.basicConfig(level=level)

def main():
    args     = get_args()
    set_log_level(args.verbose)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(level=logging.ERROR)
    requests.packages.urllib3.disable_warnings()
    doc_type = 'dns-results'
    probes   = probe.Probes()
    for measurement_id in args.measurement_ids:
        url = '{}{}'.format(args.api, measurement_id)
        measurement_data = requests.get(url).json()
        for probe_id, measurement_json in measurement_data.items():
            logging.debug('Fetch probe {}'.format(probe_id))
            p = probes.get(probe_id)
            if not p:
                logging.error('Unable to find Probe: {}'.format(probe_id))
            #measurement_json is norally an array with 1 entry
            for m in measurement_json:
                measurement = measuerments.MeasurmentDNS(m, p)
                for actions in measurement.get_elasticsearch_source():
                    print json.dumps(actions, indent=4, sort_keys=True)

    #index_items(client, items, index, doc_type, chunk_size)


if __name__ == '__main__':
    sys.exit(main())
