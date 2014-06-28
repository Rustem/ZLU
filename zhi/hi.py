from kazoo.client import KazooClient
import logging
logging.basicConfig()


def on_zconn(state):
    print state, "hello"

if __name__ == "__main__":

    zk = KazooClient(hosts='znode1.zcluster.com:2181,znode2.zcluster.com:2181,znode3.zcluster.com:2181')
    zk.add_listener(on_zconn)
    zk.start()

