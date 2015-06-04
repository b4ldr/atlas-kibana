import pdb
import sys
import json
import time
import probe
import logging
import requests
import argparse
import measuerments
import socketIO_client
import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions

ATLAS_LATEST_API = 'https://atlas.ripe.net/api/v1/measurement-latest/{}/'
ATLAS_BULK_API   = 'https://atlas.ripe.net/api/v1/measurement/{}/result/?start={}&stop={}'
ATLAS_STREAM_API = 'http://atlas-stream.ripe.net/stream/socket.io'

class Processor(object):

    hosts          = []
    api_url        = ''
    already_warned = []

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

    def _index_items(self, actions, chunk_size=200, timeout=60):
        self.logger.info('start: index {} actions'.format(len(actions)))
        client = elasticsearch.Elasticsearch(hosts=self.hosts, timeout=timeout)
        try:
            success, errors = elasticsearch.helpers.bulk(
                client,
                actions,
                index=self.index,
                doc_type=self.doc_type,
                chunk_size=chunk_size)
            self.logger.info('completed: index {} actions'.format(success))
            if len(errors) > 0:
                self.logger.error('problem inserting:\n{}'.format(errors))
                self.logger.debug('actions\n{}'.format(action))
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
                    self.logger.warning('{}:Unable to find Probe, skipping: {}'.format(measurement_id, probe_id))
                    continue
                for m in measurement_json:
                    measurement = measuerments.MeasurmentDNS(m, probe)
                    self._index_items(measurement.get_actions())

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
            self.logger.info('process measurement: {}'.format(measurement_id))
            chunk_stop_time   = self.start_time + self.chunk_period
            chunk_start_time = self.start_time
            while chunk_start_time < self.stop_time:
                actions = []
                url     = self.api_url.format(measurement_id, chunk_start_time, chunk_stop_time)
                self.logger.info('fetching measuerments: {}'.format(url))
                measurement_data = requests.get(url).json()
                self.logger.info('finished fetching measuerments: {}'.format(url))
                count = 0
                for measurement_json in measurement_data:
                    probe_id = measurement_json['prb_id']
                    self.logger.debug('{}:Fetch probe {}'.format(measurement_id, probe_id))
                    probe = self.probes.get(probe_id)
                    if not probe:
                        if probe_id not in self.already_warned:
                            self.logger.warning('{}:Unable to find Probe, skipping: {}'.format(measurement_id, probe_id))
                            self.already_warned.append(probe_id)
                        continue
                    measurement = measuerments.MeasurmentDNS(measurement_json, probe)
                    actions += measurement.get_actions()
                    count += 1
                    if not count % 1000:
                        self.logger.info('{}: parsed {} measuerments'.format(measurement_id, count)) 

                self._index_items(actions)
                chunk_start_time = chunk_stop_time + 1
                chunk_stop_time  = chunk_stop_time + self.chunk_period

class ProcessorStream(Processor):

    actions = []

    def __init__(self, args):
        super(ProcessorStream, self).__init__(args)
        self.logger  = logging.getLogger('atlas-kibana.ProcessorStream')

    @staticmethod
    def add_args(subparsers):
        url = ATLAS_STREAM_API
        parser = subparsers.add_parser('stream', help='use the atlas stream api')
        super(ProcessorStream, ProcessorStream).add_args(parser)
        parser.add_argument('-U', '--url',
                help='api url to use ({})'.format(url),
                default=url)

    def _process_measurement(self, measurement_json):
        probe_id = measurement_json['prb_id']
        self.logger.debug('Fetch probe {}'.format(probe_id))
        probe = self.probes.get(probe_id)
        if not probe:
            if probe_id not in self.already_warned:
                self.logger.warning('Unable to find Probe, skipping: {}'.format(probe_id))
                self.already_warned.append(probe_id)
            return
        measurement   = measuerments.MeasurmentDNS(measurement_json, probe)
        self.actions += measurement.get_actions()
        if len(self.actions) == 200:
            self._index_items(self.actions)
            self.actions = []
            
        
    def process(self):
        s = socketIO_client.SocketIO('atlas-stream.ripe.net/stream', 80, socketIO_client.LoggingNamespace)
        s.on('atlas_result', self._process_measurement)
        for measurement_id in self.measurement_ids:
            s.emit('atlas_subscribe', { 'stream_type': 'atlas_result', 'msm': measurement_id })
        s.wait()
