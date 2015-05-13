import logging
from ripe.atlas.sagan import Result, ResultParseError

class Measurment(object):
    '''Parent object for atlas measurment'''
    parsed_error = None
    parsed       = None

    def __init__(self, payload, probe):
        '''Initiate generic measurment data'''
        self.logger   = logging.getLogger('atlas-kibana.Measurment')
        self.probe    = probe
        self.payload  = payload
        try:
            self.parsed = Result(payload).get(payload, on_error=Result.ACTION_IGNORE)
        except ResultParseError as e:
            self.parsed_error = e
            self.logger.debug('Error parsing msm:\n{}'.format(e))


    @staticmethod
    def _clean_dict(dict_in):
        '''clean dict usless stuuf'''
        remove = ['fw', 'lts', 'msm_name', 'is_error', 'error_message', 'is_malformed', 
                '_on_error', '_on_malformation', 'klass', 'raw_data', 'stat_api', 'logger']
        for word in remove:
            try:
                del dict_in[word]
            except KeyError:
                pass
        return dict_in

    def _hammer_to_dict(self, list_in):
        '''try to force a dict from a list of objects'''
        return { idx : self._clean_dict(value.__dict__) for idx, value in enumerate(list_in) }

class MeasurmentDNS(Measurment):

    def __init__(self, payload, probe):
        super(MeasurmentDNS, self).__init__(payload, probe)
        self.logger = logging.getLogger('atlas-kibana.MeasurmentDNS')

    def get_elasticsearch_source(self):
        '''Genrater to create elasic search source as used by 
        elasticsearch.helpers.streaming_bulk
        http://elasticsearch-py.readthedocs.org/en/latest/helpers.html'''
        source = dict()
        for response in self.parsed.responses:
            source          = self._clean_dict(self.payload)
            msm_type        = source['type']
            source['probe'] = self._clean_dict(self.probe.__dict__)

            #remove the result we will replace this with something nicer
            if 'result' in source:
                del source['result']

            if response.abuf.header:
                source['header'] = self._clean_dict(response.abuf.header.__dict__)
            if response.abuf.edns0:
                source['edns0'] = response.abuf.edns0.__dict__
            if response.abuf.questions:
                source['questions']   = self._hammer_to_dict(response.abuf.questions)
            if response.abuf.answers:
                source['answers']     = self._hammer_to_dict(response.abuf.answers)
            if response.abuf.authorities:
                source['authorities'] = self._hammer_to_dict(response.abuf.authorities)
            if response.abuf.additionals:
                source['additionals'] = self._hammer_to_dict(response.abuf.additionals)
            yield(source)

