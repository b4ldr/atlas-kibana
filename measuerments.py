import logging
import datetime
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

    def _clean_array(self, list_in):
        '''try to force a dict from a list of objects'''
        return [ self._clean_dict(value.__dict__) for value in list_in ]

    def _get_source(source):
        source          = self._clean_dict(self.payload)
        source['probe'] = self._clean_dict(self.probe.__dict__)
        #remove the result we will replace this with something nicer
        if 'result' in source:
            del source['result']
        source['timestamp']  = datetime.datetime.utcfromtimestamp(source['timestamp']).isoformat()
        return source

class MeasurmentDNS(Measurment):

    def __init__(self, payload, probe):
        super(MeasurmentDNS, self).__init__(payload, probe)
        self.logger = logging.getLogger('atlas-kibana.MeasurmentDNS')

    def get_actions(self):
        source  = self._get_source()
        actions = []
        for response in self.parsed.responses:
            if response.abuf.header:
                source['header'] = self._clean_dict(response.abuf.header.__dict__)
            if response.abuf.edns0:
                source['edns0']            = self._clean_dict(response.abuf.edns0.__dict__)
                source['edns0']['options'] = self._clean_array(source['edns0']['options'])
            if response.abuf.questions:
                source['questions']   = self._clean_array(response.abuf.questions)
            if response.abuf.answers:
                source['answers']     = self._clean_array(response.abuf.answers)
            if response.abuf.authorities:
                source['authorities'] = self._clean_array(response.abuf.authorities)
            if response.abuf.additionals:
                source['additionals'] = self._clean_array(response.abuf.additionals)
            self.logger.debug('Yeild measuerment {}'.format(source))
            actions.append(source)
        return actions

class MeasurmentTraceroute(Measurment):

    def __init__(self, payload, probe):
        super(MeasurmentTraceroute, self).__init__(payload, probe)
        self.logger = logging.getLogger('atlas-kibana.MeasurmentTraceroute')

    def get_actions(self):
        source  = self._get_source()
        actions = []
        for response in self.parsed.responses:
            actions.append(source)
        print actions
        #return actions
