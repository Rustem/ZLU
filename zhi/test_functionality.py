from kazoo.client import KazooClient

exit = False


def on_zconn(state):
    print state, state.__class__


def on_node_presence(evt):
    print evt
    global exit
    exit = True


if __name__ == "__main__":
    zk = KazooClient(hosts='znode1.zcluster.com:2181,znode2.zcluster.com:2181,znode3.zcluster.com:2181')
    zk.add_listener(on_zconn)
    zk.start()

    while True:
        zk.ensure_path('/lu/')
        if zk.exists('/lu/node1/', on_node_presence):
            print 'ok'

        if exit:
            break
