from argh import arg, command, ArghParser
from ec2_cluster.base import PostgresqlCluster


def promote(args):
    """ Promote a PostgreSQL read-slave to the master role.
    """
    print 'promote'

def init(args):
    """ Initialise this instance as a master or slave.
    """
    print 'init'
    cluster = PostgresqlCluster()

def main():
    p = ArghParser()
    p.add_commands([init, promote])
    p.dispatch()

if __name__ == '__main__':
    main()
