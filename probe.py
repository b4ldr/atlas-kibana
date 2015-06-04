import Queue
import pickle
import netaddr
import logging
import requests
import ripestat
import threading

class Probes(object):
    '''return a class with all atlas probes'''
    probe_api_url     = 'https://atlas.ripe.net/api/v1/probe/'
    probe_archive_url = 'https://atlas.ripe.net/api/v1/probe-archive/?format=json'

    def __init__(self, refresh=False, probes_file='probes.p', threads=200):
        '''initialise class'''
        self.probes_file = probes_file
        self.logger      = logging.getLogger('atlas-kibana.Probes')
        self.threads     = threads
        self.queue       = Queue.Queue(self.threads * 2)
        self.probes      = set()
        self.load()
        if refresh:
            self.refresh()
        self.save()
    
    def exists(self, probe_id):
        '''check if we already have this probe'''
        for probe in self.probes.copy():
            if probe.id == int(probe_id):
                return True
        return False

    def get(self, probe_id):
        '''get an probe element'''
        for probe in self.probes.copy():
            if probe.id == int(probe_id):
                return probe
        return False

    def load(self):
        '''load pickle data'''
        try:
            self.logger.info('Loading probes from pickle file')
            self.probes = pickle.load(open(self.probes_file, 'rb'))
            self.logger.info('Finished loading probes')
        except IOError:
            self.logger.warning('unable to load probes file {}'.format(self.probes_file))

    def load_probe(self, force=False):
        '''load a probe from the queue'''
        while True:
            probe = self.queue.get()
            try:
                if not self.exists(probe['id']):
                    self.probes.add(Probe(probe))
                    self.logger.info('{}:Added'.format(probe['id']))
                else:
                    self.logger.debug('{}:Already Exists'.format(probe['id']))
            except Exception as e: 
                # This will likly come back and bite me :S
                self.logger.warning('{}:could not process\n{}'.format(probe['id'], e))
            self.queue.task_done()

    def refresh(self, force=False):
        '''load all atlas probes'''
        try:
            self.logger.info('fetching {}'.format(self.probe_archive_url))
            probes = requests.get(self.probe_archive_url, verify=False).json().get(
                    'objects', [])
            self.logger.info('fetched {}'.format(self.probe_archive_url))
        except Exception as e:
            self.logger.error('Could not fetch {0}:\n{1}'.format(self.probe_archive_url, e))
            return False
        for i in range(self.threads):
            thread        = threading.Thread(target=self.load_probe)
            thread.daemon = True
            thread.start()
        if force:
            self.logger.info('Clering all probes')
            self.probes.clear()
        for probe in probes:
            self.queue.put(probe)
        self.queue.join()
        return True

    def save(self):
        '''save the probes data'''
        pickle.dump(self.probes, open(self.probes_file, 'wb'))

class Probe(object):

    def __init__(self, probe):
        self.logger       = logging.getLogger('atlas-kibana.Probe')
        self.status       = probe.get('status_name',  None)
        self.status_since = probe.get('status_since', None)
        self.address_v4   = probe.get('address_v4',   None)
        self.address_v6   = probe.get('address_v6',   None)
        self.asn_v4       = probe.get('asn_v4',       None)
        self.asn_v6       = probe.get('asn_v6',       None)
        self.country_code = probe.get('country_code', None)
        self.latitude     = probe.get('latitude',     None)
        self.longitude    = probe.get('longitude',    None)
        self.prefix_v4    = probe.get('prefix_v4',    None)
        self.prefix_v6    = probe.get('prefix_v6',    None)
        self.id           = probe.get('id',           None)
        self.is_anchor    = probe.get('is_anchor',    None)
        self.is_public    = probe.get('is_public',    None)
        self.resource_uri = probe.get('resource_uri', None)
        self.tooltip      = 'Probe #{0}: https://atlas.ripe.net/probes/{0}/'.format(self.id)
        self.geojson      = [self.longitude, self.latitude]
        self.stat_api     = ripestat.StatAPI('Atlas-Kibana')
        self.tags         = []
        self.tags.extend(probe.get('tags', None))
        self.location     = dict()
        self.asn_v4_name  = None
        self.asn_v6_name  = None
        self.prefixlen_v4 = None
        self.rir_v4       = None
        self.prefixlen_v6 = None
        self.rir_v6       = None

        self.update()

    def __getstate__(self):
        '''remove logger and api from logger'''
        d = dict(self.__dict__)
        del d['logger']
        del d['stat_api']
        return d

    def __setstate__(self, d):
        '''recreate logger and stat_api'''
        d['stat_api'] = ripestat.StatAPI('Atlas-Kibana')
        d['logger']   = logging.getLogger('atlas-kibana.Probe')
        self.__dict__.update(d)


    def __eq__(self, other):
        '''Check equality'''
        if isinstance(other, Probe):
            return self.id == other.id
        return False

    def __ne__(self, other):
        '''Check inequality'''
        if isinstance(other, Probe):
            return not (self.id == other.id)
        return True

    def __repr__(self):
        '''represent object'''
        return 'Probe({})'.format(self.id)

    def __hash__(self):
        '''hash object'''
        return hash(self.__repr__())

    def get_location_meta(self, country_code):
        '''use restcountries.eu to get the location'''
        url = 'https://restcountries.eu/rest/v1/alpha?codes={}'.format(country_code.lower())
        try:
            location = requests.get(url, verify=False).json()
            self.logger.debug('{}:Add location for {}'.format(self.id, country_code))
            return location[0]
        except Exception as e:
            self.logger.error('Could not fetch {}:\n{}'.format(url, e))

    def get_location(self):
        location          = self.get_location_meta(self.country_code)
        return { 'country'  : location['name'],
                'region'    : location['region'],
                'subregion' : location['subregion']}

    def update(self):
        '''update probe metadata'''
        if self.country_code:
            self.location = self.get_location()
        if self.asn_v4:
            self.asn_v4_name  = self.get_asn_name(self.asn_v4)
        if self.asn_v6:
            if self.asn_v6 == self.asn_v4:
                self.asn_v6_name  = self.asn_v4_name
            else:
                self.asn_v6_name  = self.get_asn_name(self.asn_v6)
        if self.prefix_v4:
            self.prefixlen_v4 = self.get_prefix_len(self.prefix_v4)
            self.rir_v4       = self.get_rir(self.prefix_v4)
        if self.prefix_v6:
            self.prefixlen_v6 = self.get_prefix_len(self.prefix_v6)
            self.rir_v6       = self.get_rir(self.prefix_v6)

    def get_rir(self, prefix):
        '''use RIPEstat to get the rir name'''
        try:
            whois = self.stat_api.get_data('whois', {'resource': prefix})
            rir   = ','.join(whois.get('authorities', []))
            self.logger.debug('{}:Add RIR "{}" for {}'.format(self.id, rir, prefix))
            return rir
        except Exception as e:
            self.logger.error('Could not fetch holder for {}:\n{}'.format(prefix, e))

    def get_asn_name(self, asn):
        '''use RIPEstat to get the asn name'''
        try:
            asn_name = self.stat_api.get_data('as-overview',
                {'resource': asn}).get('holder', None)
            self.logger.debug('{}:Add AS name "{}" for {}'.format(
                self.id, asn_name, asn))
            return asn_name
        except Exception as e:
            self.logger.error('Could not fetch holder for {}:\n{}'.format(asn, e))
        
    def get_prefix_len(self, prefix):
        try:
            prefix_len = int(netaddr.IPNetwork(prefix).prefixlen)
            self.logger.debug('{}:Add prefix length "{}" for {}'.format(
                self.id, prefix_len, prefix))
            return prefix_len
        except netaddr.AddrFormatError as e:
            self.logger.warning('could not get prefix length: {}'.format(e))
