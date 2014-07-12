import sys
from twopc import Member
from kazoo.client import KazooClient

def on_conn(evt):
    print evt

if __name__ == '__main__':
    client1 = KazooClient(hosts='znode1.zcluster.com:2181,znode2.zcluster.com:2181,znode3.zcluster.com:2181')
    client1.add_listener(on_conn)
    client1.start()
    member1 = Member(client1, 'twopc', sys.argv[1])
    member1.start()
    # client2 = KazooClient(hosts='znode1.zcluster.com:2181,znode2.zcluster.com:2181,znode3.zcluster.com:2181')
    # client2.start()
    # member2 = Member(client2, 'twopc', '2')

    # client3 = KazooClient(hosts='znode1.zcluster.com:2181,znode2.zcluster.com:2181,znode3.zcluster.com:2181')
    # client3.start()
    # member3 = Member(client3, 'twopc', '3')



