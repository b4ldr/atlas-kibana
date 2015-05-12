import logging
from ripe.atlas.sagan import Result, ResultParseError

class Measurment(object):
    '''Parent object for atlas measurment'''
    parsed_error = None

    def __init__(self, payload, probe):
        '''Initiate generic measurment data'''
        self.logger = logging.getLogger('atlas-kibana.Measurment')
        self.probe  = probe
        try:
            self.parsed = Result(payload).get(payload, on_error=Result.ACTION_IGNORE)
        except ResultParseError as e:
            self.parsed_error = e
            self.logger.debug('Error parsing msm:\n{}'.format(e))

    @staticmethod
    def _list_to_dict(list_in):
        return { i : list_in[i] for i in range(0, len(list_in)) }

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
            response.questions   = self._list_to_dict(response.questions)
            response.answers     = self._list_to_dict(response.answers)
            response.authorities = self._list_to_dict(response.authorities)
            response.additionals = self._list_to_dict(response.additionals)
            yeild(response)

