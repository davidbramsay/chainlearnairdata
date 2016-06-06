#!/usr/bin/python

from chaincrawler import chainCrawler
from chaincrawler import chainSearch
from dateutil.parser import parse
import logging
import time
import sys
import json
import requests

logging.basicConfig(stream=sys.stderr)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.propagate = 0
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
log.addHandler(ch)

#state that prints current node, and past history with {'command':'crawl', 'href': 'http://', 'type':'type'}
#'find' will crawl us to a node as a starting point, any actions are done relative to that node
#'move' will move us 1 away from the current node, as a traversal.  any actions are done relative to that node
#'back' will move us back up in our history one and print
#add resources at each node
#read and return values

class ChainTraversal(object):

    def __init__(self, crawl_delay=200,
            entry_point='http://learnair.media.mit.edu:8000/',
            namespace='http://learnair.media.mit.edu:8000/rels/'):

        self.namespace = namespace
        self.entry_point = entry_point

        self.current_node = self.entry_point
        self.current_node_type = 'entry_point'
        self.current_command = 'entry_point'

        self.history = []
        self.forward_history = []

        self.crawl_delay = crawl_delay

        self.crawler = chainCrawler.ChainCrawler(entry_point, crawl_delay=crawl_delay)
        self.searcher = chainSearch.ChainSearch(entry_point, crawl_delay=crawl_delay)


    def print_state(self):

        log.info('current node: %s', self.current_node)
        log.info( 'current node type: %s', self.current_node_type)
        log.debug('---history----')
        log.debug('%s', self.history)


    def back(self):

        if len(self.history):
            prev = self.history.pop()

            self.forward_history.append({
                'href':self.current_node,
                'type':self.current_node_type,
                'command':self.current_command })

            self.searcher.reset_entrypoint(prev['href'])
            self.current_node = prev['href']
            self.current_node_type = prev['type']
            self.current_command = prev['command']

            self.print_state()

        else:
            log.info("can't go back, no history left")


    def forward(self):

        if len(self.forward_history):
            prev = self.forward_history.pop()

            self.history.append({
                'href':self.current_node,
                'type':self.current_node_type,
                'command':self.current_command })

            self.searcher.reset_entrypoint(prev['href'])
            self.current_node = prev['href']
            self.current_node_type = prev['type']
            self.current_command = prev['command']

            self.print_state()

        else:
            log.info("can't go forward, no history left")


    def find_a_resource(self, resource, name=None):

        if name is not None:
            log.info('>> FIND/MOVE TO %s', name.upper())
        else:
            log.info('>> FIND/MOVE TO FIRST %s', resource.upper())

        uri = self.crawler.find(namespace=self.namespace, \
                resource_title=name, resource_type=resource)
        log.info('found %s %s: %s', resource, name, uri)

        self.searcher.reset_entrypoint(uri)
        self.history.append({'href':self.current_node, 'type':self.current_node_type, 'command':self.current_command})
        self.current_node = uri
        self.current_node_type = resource
        self.current_command = 'crawl'

        self.print_state()


    def find_a_deployment(self, name=None):
        self.find_a_resource('deployment', name)


    def find_a_site(self, name=None):
        self.find_a_resource('site', name)


    def find_an_organization(self, name=None):
        self.find_a_resource('organization', name)


    def find_a_device(self, name=None):
        self.find_a_resource('device', name)


    def add_a_resource(self, resource_type, post_data, \
            plural_resource_type=None):
        '''this is safe to call if resource already exists-
        checks first and returns False if it is found.  Otherwise
        this returns the server response upon trying to create the resource'''

        degrees = 1

        #first, pull out resource_name
        resource_name = None
        for key, val in post_data.iteritems():
            if (key in ['first_name','unique_name','name','metric']):
                resource_name = val

        if resource_name is None:

            log.warn('post data malformed')
            return False

        log.info('>> ADD %s %s', resource_type.upper(), resource_name.upper())

        #search for it and make sure it doesn't already exist
        if len(self.searcher.find_first(
                namespace=self.namespace,
                resource_type=resource_type,
                plural_resource_type=plural_resource_type,
                resource_title=resource_name,
                max_degrees=degrees)):

            log.warn('This resource already exists')
            return False

        #get create link
        post_link = self.searcher.find_create_link(
                namespace=self.namespace,
                resource_type=resource_type,
                plural_resource_type=plural_resource_type,
                degrees=degrees)[0]

        #post to create link with post_data and return
        headers = {'Content-Type':'application/json'}
        resp = requests.post(post_link, data=json.dumps(post_data), headers=headers)

        log.info('attempted to add resource, response returned: %s', resp)

        return resp


    def add_data(self, post_data, resource_type='dataHistory'):
        '''does not check for duplicates or overwrites, will simply create
        multiple copies of the same timestamp/values over and over'''

        log.info('>> ADD DATA')

        degrees = 1

        #get create link
        post_link = self.searcher.find_create_link(
                namespace=self.namespace,
                resource_type=resource_type,
                degrees=degrees)[0]

        log.info('DATA POST LINK: %s', post_link)

        #post to create link with post_data and return
        headers = {'Content-Type':'application/json'}
        resp = requests.post(post_link, data=json.dumps(post_data), headers=headers)

        log.info('attempted to add data, response returned: %s', resp)

        return resp


    def move_to_resource(self, resource_type, name=None, \
            plural_resource_type=None):

        if name is not None:
            log.info('>> MOVE TO %s', name.upper())
        else:
            log.info('>> MOVE TO FIRST %s', resource_type.upper())

        degrees = 1

        try:
            uri = self.searcher.find_first(
                    namespace = self.namespace,
                    resource_type = resource_type,
                    resource_title = name,
                    max_degrees = degrees)[0]

            log.info('traverse to %s %s: %s', resource_type, name, uri)

            self.searcher.reset_entrypoint(uri)
            self.history.append({'href':self.current_node, 'type':self.current_node_type, 'command':self.current_command})
            self.current_node = uri
            self.current_node_type = resource_type
            self.current_command = 'traverse'

            self.print_state()
            return True

        except:
            log.warn('could not find %s resource %s', resource_type, name)
            return False


    def pull_data_one_direction(self, start_uri, direction, max_empty_steps):
        '''takes a URI, direction 'next' or 'previous', and max_steps, and returns
        a list of dicts of the data'''
        step_counter = 0
        return_data = []
        current_uri = start_uri

        while step_counter < max_empty_steps:

            time.sleep(self.crawl_delay/1000.0)

            log.debug('current link %s', current_uri)

            #pull resource, collect data in a list of dict
            try:
                req = requests.get(current_uri)
                resource_json = req.json()
            except requests.exceptions.ConnectionError:
                log.warn( 'URI "%s" unresponsive', current_uri )

            #add data to list
            try:
                if (len(resource_json['data'])):
                    return_data.extend(resource_json['data'])
                    step_counter = 0
                else:
                    step_counter = step_counter + 1
            except:
                log.warn( 'could not find data to add from %s', self.current_uri )
                step_counter = step_counter + 1

            #get next resource
            try:
                current_uri = resource_json['_links'][direction]['href']
            except:
                log.warn( 'could not update to %s link' , direction )
                return return_data

        return return_data


    def get_all_data(self, max_empty_steps=10, resource_type='dataHistory'):

        print 'GET ALL DATA'
        degrees = 1
        return_data = []

        #get uri of starting point
        start_uri = self.searcher.find_first(
                namespace = self.namespace,
                resource_type =resource_type,
                max_degrees=degrees)[0]

        #run through previous until max_empty_steps in a row are empty
        return_data.extend(
                self.pull_data_one_direction(start_uri, 'previous', max_empty_steps))

        #run through previous until max_empty_steps in a row are empty
        return_data.extend(
                self.pull_data_one_direction(start_uri, 'next', max_empty_steps))

        #order data
        return_data.sort(key=lambda d: d['timestamp'])

        #return data
        return return_data


    def safe_add_data(self, post_data, max_empty_steps=10):
        '''check previous data in this sensor and do not overwrite prev data
        with identical timestamps, warn.  Add new data'''

        existing_data = self.get_all_data(max_empty_steps)
        existing_timestamps = [parse(ts['timestamp']) for ts in existing_data]

        new_data = [pd for pd in post_data \
                if parse(pd['timestamp']) not in existing_timestamps]

        num_ignored = len(post_data)-len(new_data)

        if (num_ignored):
            print "%s timestamps in post_data already exist and ignored" % num_ignored
        else:
            print "no timestamp conficts! all data will be posted"

        return self.add_data(new_data)


    def add_and_move_to_resource(self, resource_type, post_data, \
            plural_resource_type=None):

        #create the resource
        self.add_a_resource(resource_type, post_data,
                plural_resource_type=plural_resource_type)

        #get the name of the resource
        resource_name = None
        for key, val in post_data.iteritems():
            if (key in ['first_name','unique_name','name','metric']):
                resource_name = val

        if resource_name is None:
            log.warn('post data malformed')

        #move to it
        return self.move_to_resource(resource_type, resource_name,
                plural_resource_type=plural_resource_type)


    def find_and_move_path_exists(self, path_list):
        ''' expects a list of dicts that will guide us through chain.
        it will crawl/search for the first object, and then move along
        the path of all subsequent objects.

        path_list= [{'type':'organization', 'name':'testOrg Name'},
        {'type':'deployment', 'name':'learnairNet'}, {'type':'device',
        'name':'device1'] ...'''

        self.find_a_resource(path_list[0]['type'], path_list[0]['name'])

        for x in range(len(path_list)-1):
            self.move_to_resource(path_list[x+1]['type'], path_list[x+1]['name'])

    def find_and_move_path_create(self, path_list):
        ''' expects a list of dicts that will guide us through chain.
        it will crawl/search for the first object, and then move along
        the path of all subsequent objects.

        path_list= [{'type':'organization', 'name':'testOrg Name'},
        {'type':'deployment', 'post_data':{'name':'learnairNet'}}, {'type':'device',
        'post_data':{'name':'device1'}}] ...'''

        self.find_a_resource(path_list[0]['type'], path_list[0]['name'])

        for x in range(len(path_list)-1):
            self.add_and_move_to_resource(path_list[x+1]['type'], path_list[x+1]['post_data'])


class PromptedChainTraverse(object):


    def __init__(self, crawl_delay_default=200,
            entry_point_default='http://learnair.media.mit.edu:8000/',
            namespace_default='http://learnair.media.mit.edu:8000/rels/'):

        try:
            crawl_delay = int(raw_input('Craw Delay: [%s] ' % crawl_delay_default))
        except:
            crawl_delay = crawl_delay_default

        entry_point = raw_input('Entry Point: [%s] ' % entry_point_default)
        entry_point = entry_point or entry_point_default

        namespace = raw_input('Namespace: [%s] ' % namespace_default)
        namespace = namespace or namespace_default

        print 'Creating Traverser with crawl delay %s ms' % crawl_delay
        print '-- entry point: %s' % entry_point
        print '-- namespace: %s' % namespace

        self.traveler = ChainTraversal(
                crawl_delay=crawl_delay,
                entry_point=entry_point,
                namespace=namespace)


    def prompt_loop(self):
        '''
        interaction loop with state for traversing chain.
        Get a list of current state, give movement options to resources from current location.
        Check if datahistory, allow to 'add data', give options for formating/pulling.
        Allow "back to x" if history, forward if we have gone back.
        Allow crawl, add resource.
        Allow printing history.
        Allow exit.
        '''
        options = []


if __name__ == "__main__":
    '''
    #find a deployment
    x = find_a_deployment("Test Deployment #2")
    #add a device under the deployment
    print add_a_resource(x, 'device', {'unique_name':'testdeviceProgrammatic', 'device_type':'learnairv1'})
    y = move_from_to(x, 'device', 'testdeviceProgrammatic')
    '''
    traveler = ChainTraversal()
    traveler.print_state()
    #traveler.find_a_deployment("Test Deployment #2")
    traveler.find_an_organization('ResEnv Test Organization #1')
    #traveler.back()
    traveler.move_to_resource('deployment', 'Test Deployment #2')
    #traveler.forward()
    traveler.move_to_resource('device', 'testdevice001')
    #traveler.add_and_move_to_resource('device', {'unique_name':'testdeviceProg6', 'device_type':'learnairv1'})
    traveler.add_and_move_to_resource('sensor', {'metric':'COT','sensor_type':'alphasenseCOT','unit':'ppb'})
    #traveler.back()
    #traveler.move_to_resource('dataHistory', 'sensor_data')
    '''
    traveler.add_data([{'value':54.1,'timestamp':'2016-05-18 20:10:00+0000'},
        {'value':44.1,'timestamp':'2016-05-18 20:11:00+0000'},
        {'value':34.1,'timestamp':'2016-05-18 20:12:00+0000'},
        {'value':24.1,'timestamp':'2016-05-18 20:13:00+0000'},
        {'value':14.1,'timestamp':'2016-05-18 20:14:00+0000'},
        {'value':4.1,'timestamp':'2016-05-18 20:15:00+0000'}]
        )
    '''
    #traveler.get_all_data()
    traveler.safe_add_data([{'value':64.1,'timestamp':'2016-05-18 20:09:00+0000'},
        {'value':5,'timestamp':'2016-05-18 20:11:00+0000'},
        {'value':5,'timestamp':'2016-05-18 20:12:00+0000'},
        {'value':5,'timestamp':'2016-05-18 20:13:00+0000'},
        {'value':5,'timestamp':'2016-05-18 20:14:00+0000'},
        {'value':5,'timestamp':'2016-05-18 20:15:00+0000'}]
        )
    #traveler.forward()
    traveler.print_state()
