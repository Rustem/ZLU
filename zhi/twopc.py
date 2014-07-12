from kazoo.retry import (
    KazooRetry,
    RetryFailedError,
    ForceRetryError
)
from kazoo.client import KazooClient
import json
import functools
import logging
logging.basicConfig()


class TwoPCState(object):
    BEGIN = 'begin'
    PREPARE = 'prep'

    COMMIT = 'commit'
    ACK_COMMIT = 'ack_commit'

    ABORT = 'abort'
    ACK_ABORT = 'ack_abort'

    STATUSES = (BEGIN, PREPARE, COMMIT, ACK_COMMIT, ABORT, ACK_ABORT)

class TwoPCConstraintError(object):
    pass


class Coordinator(object):

    tx_path = '/zone'

    def __init__(self, client, nodes, path, query):
        self.client = client
        self.path = path
        self.tx_path = self.path + '/' + 'tx'
        self.query = str(query).encode('utf-8')
        self.wake_event = client.handler.event_object()
        self.commit_evt = None
        self.tx_created = False
        self.nodes = nodes
        self.intermediate_results = [None] * len(nodes)

    def begin_2pc(self):
        self.cnt = 0
        threshold = len(self.nodes)
        self._inner_2pc(self.nodes, threshold)
        self._clean_everything()

    def _inner_2pc(self, nodes, threshold):
        # initial
        self.client.ensure_path(self.path)
        if not self.client.exists(self.tx_path):
            self.client.create(self.tx_path)
        self.wait_for_cohort(nodes, threshold)
        self.reset_state(nodes)
        # prepare (phase 1)
        print '------------PREPARE------------'
        print 'All parties are ready. Begin transaction'
        self.client.set(self.tx_path, json.dumps({
            'query': self.query,
            'state': TwoPCState.BEGIN}))
        self.wake_event.wait()
        # commit (phase 2)
        print '------------COMMIT------------'
        print 'All parties are executed transaction.', self.intermediate_results
        decision = self._make_decision(self.intermediate_results)
        print 'Coordinator decided', decision
        self.client.set(self.tx_path, json.dumps({
            'status': decision}))
        self.reset_state(nodes)

        print '------------COMMIT ACK------------'
        self.wake_event.wait()
        print 'Coordinator finished', self.intermediate_results
        self.reset_state(nodes)

    def reset_state(self, nodes):
        self.wake_event.clear()
        self.cnt = 0
        self.intermediate_results = []
        self._register_watchers(nodes)

    def _make_decision(self, results):
        raw_results = list(r['status'] for r in results)
        print raw_results
        try:
            raw_results.index(TwoPCState.ABORT)
        except ValueError:
            return TwoPCState.COMMIT
        else:
            return TwoPCState.ABORT


    def _register_watchers(self, nodes):
        for node in nodes:
            node_path = self.tx_path + '/' + node
            fn = functools.partial(self._on_node_tx_status, node_path)
            self.client.get(node_path, fn)

    def _on_node_tx_status(self, node_path, evt):
        if evt.type == 'CHANGED':
            value = json.loads(self.client.get(node_path)[0])
            if value['status'] in TwoPCState.STATUSES:
                self.cnt += 1
                self.intermediate_results.append(value)
        if self.cnt == len(self.nodes):
            self.wake_event.set()

    def wait_for_cohort(self, nodes, threshold):
        self.wake_event.clear()
        self.cnt = 0

        def on_node_presence(node_path, state):
            print state
            if state.type == 'CREATED':
                self.cnt += 1
            elif state.type == 'DELETED':
                fn = functools.partial(on_node_presence, node_path)
                self.client.exists(node_path, fn)
            if self.cnt == threshold:
                self.wake_event.set()

        for node in nodes:
            node_path = self.tx_path + '/' + node
            on_node_create_or_delete = functools.partial(
                on_node_presence, node_path)
            self.client.exists(node_path, on_node_create_or_delete)

        print 'Waiting'
        self.wake_event.wait()
        self.cnt = 0
        return True

    def _clean_everything(self):
        self.client.delete(self.path, recursive=True)
        # print self.clinet.delete(self.path, )


class Member(object):

    def __init__(self, client, path, name):
        self.client = client
        self.path = path
        self.tx_path = self.path + '/' + 'tx'
        self.prefix = name
        self.create_path = self.tx_path + '/' + self.prefix
        self.create_tried = False
        self.wake_up = self.client.handler.event_object()
        self.initialize()

    def initialize(self):
        self.client.ensure_path(self.path)
        self.wake_up.clear()

        def on_changed_presence(evt):
            if evt.type == 'DELETED':
                self.wake_up.set()

        if not self.client.exists(self.create_path, on_changed_presence):
            node = self.client.create(self.create_path, ephemeral=True)
        else:
            self.wake_up.wait()
            node = self.client.create(self.create_path, ephemeral=True)
            self.wake_up.clear()
        self.client.get(self.tx_path, self._on_new_tx)
        print node

    def two_pc(self):
        self.wake_up.wait()
        self.wake_up.clear()
        self.client.get(self.tx_path, self._on_new_tx)
        print '------------PREPARE------------'
        print 'Begin transaction.', self.data
        rv = eval(self.data['query'])
        print 'Transaction calculated. answer is', rv
        self.client.set(self.create_path, json.dumps({
            'result': rv,
            'status': TwoPCState.ABORT}))
        self.wake_up.wait()
        self.wake_up.clear()
        print "------------COMMIT------------", self.data
        if self.data['status'] == TwoPCState.COMMIT:
            self.client.set(self.create_path, json.dumps({
                'status': TwoPCState.ACK_COMMIT}))
            print 'acknowledging'
            print '------------TRANSACTION FINISHED------------'
        elif self.data['status'] == TwoPCState.ABORT:
            self.client.set(self.create_path, json.dumps({
                'status': TwoPCState.ACK_ABORT}))
            print 'acknowledging'
            print '------------TRANSACTION FAILED--------------'

    start = two_pc

    def _on_new_tx(self, evt):
        data = ''
        if evt.type == 'CHANGED':
            data = self.client.get(self.tx_path, self._on_tx_changed)
        self.data = json.loads(data[0])
        self.wake_up.set()

    def _on_tx_changed(self, evt):
        if evt.type == 'CHANGED':
            data = self.client.get(self.create_path, self._on_tx_changed)
            print data


if __name__ == '__main__':
    main_cli = KazooClient(hosts='znode1.zcluster.com:2181,znode2.zcluster.com:2181,znode3.zcluster.com:2181')
    main_cli.start()
    node_names = [
        '1', '2'
    ]

    coord = Coordinator(main_cli, node_names, 'twopc', '1 + 1')
    coord.begin_2pc()
