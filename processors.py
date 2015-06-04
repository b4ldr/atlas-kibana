import pdb
import sys
import json
import time
import probe
import logging
import requests
import argparse
import measuerments
import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions

ATLAS_LATEST_API = 'https://atlas.ripe.net/api/v1/measurement-latest/{}/'
ATLAS_BULK_API   = 'https://atlas.ripe.net/api/v1/measurement/{}/result/?start={}&stop={}'
ATLAS_STREAM_API = 'https://atlas.ripe.net/api/v1/measurement-latest/'

class Processor(object):

    hosts   = []
    api_url = ''

    def __init__(self, args):
        self.logger          = logging.getLogger('atlas-kibana.Processor')
        self.probes          = probe.Probes(args.refresh_probes)
        self.index           = args.index
        self.doc_type        = args.doc_type
        self.api_url         = args.url
        self.measurement_ids = args.measurement_ids
        self._format_hosts(args.hosts)
        
    def _format_hosts(self, hosts):
        '''format the hosts argument into a json blob'''
        for host in hosts.split(','):
            tokens    = host.split(':',1)
            port      = 9200
            host_name = tokens[0]
            if len(tokens) == 2:
                port = tokens[1]
            self.hosts.append({
                'host' : host_name,
                'port' : port })

    def _index_items(self, actions, chunk_size=200, timeout=30):
        client = elasticsearch.Elasticsearch(hosts=self.hosts, timeout=timeout)
        try:
            for ok, result in elasticsearch.helpers.streaming_bulk(
                client,
                actions,
                index=self.index,
                doc_type=self.doc_type,
                chunk_size=chunk_size):
                action, result = result.popitem()
                if not ok:
                    doc_id = result['_id']
                    self.logger.warning('Could not {0} document {1}: {2}'.format(action, doc_id, result))
        except elasticsearch.exceptions.ConnectionTimeout:
            self.logger.error('Timed out submitting\n{}'.format(action))

    @staticmethod
    def add_args(parser):
        ''' add the default set of arguments to each sub parser so the cli documenbtation and use is more intuative'''
        parser.add_argument('--verbose', '-v', action='count')
        parser.add_argument('-D', '--doc-type', required=True,
                help='Document to store probe and measurement data in.')
        parser.add_argument('-I', '--index', required=True,
                help='Index to store probe and measurement data in.')
        parser.add_argument('-H', '--hosts', default='localhost:9200',
                help='elastic search backend servers')
        parser.add_argument('--refresh-probes', action='store_true', 
                help='Refresh the probe pickle data')
        parser.add_argument('measurement_ids',  nargs='+',
                help='measurement(s) to index in Elasticsearch')
    def process(self):
        raise NotImplementedError('Subclasses should implement this!')

class ProcessorLatest(Processor):

    def __init__(self, args):
        super(ProcessorLatest, self).__init__(args)
        self.logger  = logging.getLogger('atlas-kibana.ProcessorLatest')

    @staticmethod
    def add_args(subparsers):
        url = ATLAS_LATEST_API
        parser = subparsers.add_parser('latest', help='use the atlas latest api')
        super(ProcessorLatest, ProcessorLatest).add_args(parser)
        parser.add_argument('-U', '--url',
                help='api url to use ({})'.format(url),
                default=url)

    def process(self):
        for measurement_id in self.measurement_ids:
            self.logger.info('fetching measuerments: {}'.format(url))
            url = self.api_url.format(measurement_id)
            self.logger.info('finished fetching measuerments: {}'.format(url))
            measurement_data = requests.get(url).json()
            for probe_id, measurement_json in measurement_data.items():
                self.logger.info('Fetch probe {}'.format(probe_id))
                probe = self.probes.get(probe_id)
                if not probe:
                    self.logger.warning('Unable to find Probe, skipping: {}'.format(probe_id))
                    continue
                for m in measurement_json:
                    measurement = measuerments.MeasurmentDNS(m, probe)
                    self._index_items(measurement.get_elasticsearch_source())

class ProcessorBulk(Processor):

    def __init__(self, args):
        super(ProcessorBulk, self).__init__(args)
        self.logger       = logging.getLogger('atlas-kibana.ProcessorBulk')
        self.start_time   = args.start_time
        self.stop_time     = min(args.stop_time, int(time.time()))
        self.chunk_period = args.chunk_period

    @staticmethod
    def add_args(subparsers):
        url = ATLAS_BULK_API
        parser = subparsers.add_parser('bulk', help='use the atlas latest api')
        super(ProcessorBulk, ProcessorBulk).add_args(parser)
        parser.add_argument('-U', '--url',
                help='api url to use ({})'.format(url),
                default=url)
        parser.add_argument('--start-time', default=1262304000, type=int,
                help='get measuerment from this date in unix time')
        parser.add_argument('--stop-time', default=4102444800, type=int,
                help='get measuerment upto this date in unix time')
        parser.add_argument('--chunk-period', default=86400, type=int,
                help='to save on memory we fetch data in chunks.  value in seconds default: 86400')

    def process(self):
        for measurement_id in self.measurement_ids:
            chunk_stop_time   = self.start_time + self.chunk_period
            chunk_start_time = self.start_time
            while chunk_start_time < self.stop_time:
                url = self.api_url.format(measurement_id, chunk_start_time, chunk_stop_time)
                self.logger.info('fetching measuerments: {}'.format(url))
                measurement_data = requests.get(url).json()
                self.logger.info('finished fetching measuerments: {}'.format(url))
                for measurement_json in measurement_data:
                    probe_id = measurement_json['prb_id']
                    self.logger.info('Fetch probe {}'.format(probe_id))
                    probe = self.probes.get(probe_id)
                    if not probe:
                        self.logger.warning('Unable to find Probe, skipping: {}'.format(probe_id))
                        continue
                    measurement = measuerments.MeasurmentDNS(measurement_json, probe)
                    self._index_items(measurement.get_elasticsearch_source())
                chunk_start_time = chunk_stop_time + 1
                chunk_stop_time  = chunk_stop_time + self.chunk_period
