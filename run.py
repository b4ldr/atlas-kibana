#!/bin/usr/env python
import probe
import requests
import argparse
import elasticsearch
import elasticsearch.helpers

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
    parser.add_argument('measurements',
            help="Comma-separated list of measurements to index in Elasticsearch")
    return parser.parse_args()

def main():
    args = get_args()
    log_level = logging.WARNING
    client = es
    items = probes.elasticsearch_source
    index = index
    doc_type = 'dns-results'
    
    if args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose > 1:
        log_level = logging.DEBUG
    logging.basicConfig(log_level)
    probes = probe.Probes()
    
    for msm_id in args.measurements.split(','):
        url = '{}{}'.format(args.api, msm_id)
        msms   = requests.get(url)
        for probe_id, msm in msms.items():
            probe = probes.get(probe_id)
            measurment = measuerments.MeasurmentDNS(msm[0], probe)
            for i in measurement.get_elasticsearch_source():
                print i

    index_items(client, items, index, doc_type, chunk_size)


if __name__ == '__main__':
    sys.exit(main())
