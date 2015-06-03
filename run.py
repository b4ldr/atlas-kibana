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

ATLAS_LATEST_API = 'https://atlas.ripe.net/api/v1/measurement-latest/{}/'
ATLAS_BULK_API = 'https://atlas.ripe.net/api/v1/measurement/{}/result/?start={}&end={}'
ATLAS_STREAM_API = 'https://atlas.ripe.net/api/v1/measurement-latest/'

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
            logging.warning('Could not {0} document {1}: {2}'.format(action, doc_id, result))

def add_default_args(parser, api_url):
    ''' add the default set of arguments to each sub parser so the cli documenbtation and use is more intuative'''
    parser.add_argument('--verbose', '-v', action='count')
    parser.add_argument('-D', '--doc-type', required=True,
            help="Index to store probe and measurement data in. Defaults to 'atlas-index'")
    parser.add_argument('-I', '--index', required=True,
            help="Index to store probe and measurement data in. Defaults to 'atlas-index'")
    parser.add_argument('-H', '--hosts', default='localhost:9200',
            help="elastic search backend servers")
    parser.add_argument('measurement_ids',  nargs='+', 
            help="measurement(s) to index in Elasticsearch")
    parser.add_argument('-U', '--url', 
            help='api url to use ({})'.format(api_url),
            default=api_url)

def get_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='api')

    latest_parser = subparsers.add_parser('latest', help='use the atlas latest api')
    bulk_parser   = subparsers.add_parser('bulk', help='use the atlas bulk api')
    bulk_parser.add_argument('--start', default=1262304000,
            help='get measuerment from this date, unix time')
    bulk_parser.add_argument('--end', default=4102444800,
            help='get measuerment upto date, unix time')
    stream_parser = subparsers.add_parser('stream', help='use the atlas stream api')

    add_default_args(latest_parser, ATLAS_LATEST_API)
    add_default_args(bulk_parser, ATLAS_BULK_API)
    add_default_args(stream_parser, ATLAS_STREAM_API)

    return parser.parse_args()

def set_log_level(verbose):
    '''set the logger level'''
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose > 1:
        level = logging.DEBUG
    logging.basicConfig(level=level)

def process_latest(args, hosts, probes):
    for measurement_id in args.measurement_ids:
        url = args.url.format(measurement_id)
        measurement_data = requests.get(url).json()
        for probe_id, measurement_json in measurement_data.items():
            logging.debug('Fetch probe {}'.format(probe_id))
            p = probes.get(probe_id)
            if not p:
                logging.error('Unable to find Probe: {}'.format(probe_id))
            for m in measurement_json:
                measurement = measuerments.MeasurmentDNS(m, p)
                index_items(hosts, measurement.get_elasticsearch_source(), 
                        args.index, args.doc_type)

def process_bulk(args, probes):
    for measurement_id in args.measurement_ids:
        url = args.url.format(measurement_id, args.start, args.end)
        measurement_data = requests.get(url).json()
        for measurement_json in measurement_data:
            logging.debug('Fetch probe {}'.format(probe_id))
            p = probes.get(probe_id)
            if not p:
                logging.error('Unable to find Probe: {}'.format(probe_id))
            measurement = measuerments.MeasurmentDNS(m, p)
            index_items(hosts, measurement.get_elasticsearch_source(), 
                    args.index, args.doc_type)

def main():
    args     = get_args()
    set_log_level(args.verbose)
    hosts   = []
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(level=logging.ERROR)
    requests.packages.urllib3.disable_warnings()
    probes   = probe.Probes()
    for host in args.hosts.split(','):
        tokens    = host.split(':',1)
        port      = 9200
        host_name = tokens[0]
        if len(tokens) == 2:
            port = tokens[1]
        hosts.append({
            'host' : host_name,
            'port' : port }) 
        if args.api == 'latest':
            process_latest(args, hosts, probes)


if __name__ == '__main__':
    sys.exit(main())
