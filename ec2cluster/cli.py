import logging
import utils
import argparse
from ec2cluster.base import PostgresqlCluster


logger = logging.getLogger('ec2cluster')


def promote(args):
    """ Promote a PostgreSQL read-slave to the master role.
    """
    print 'promote'
    cluster = PostgresqlCluster()
    cluster.promote()


def init(args):
    """ Initialise this instance as a master or slave.
    """
    print 'init'
    cluster = PostgresqlCluster()
    cluster.initialise()


def _add_default_args(parsers, args):
    """ Adds args to the given parser. Helper to make it easier to use the same arg for
        multiple commands.

        default_args = [
            {'name': '--nametest', 'help': 'some help text' },
        ]

        parsers = [my_parser, my_other_parser]
    """
    for parser in parsers:
        for arg in args:
            tmp_arg = arg.copy()
            name = tmp_arg.pop('name')
            parser.add_argument(name, **tmp_arg)


def main():
    utils.configure_logging()
    parser = argparse.ArgumentParser()

    # top-level parser, generic args used by all commands
    subparsers = parser.add_subparsers(help='Subcommand help')

    # init command
    parser_init = subparsers.add_parser('init', help='Initialise the cluster')
    parser_init.add_argument('--bar', help='init arg')
    parser_init.set_defaults(func=init)

    # promote command
    parser_promote = subparsers.add_parser('promote', help='Promote a slave')
    parser_promote.add_argument('--baz', help='promote arg')
    parser_promote.set_defaults(func=promote)

    default_args = [
        {'name': '--settings', 'help': 'Path to settings file'},
    ]

    _add_default_args([parser_init, parser_promote], default_args)

    # Parse the args, and pass them to the function for the chosen subcommand
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
