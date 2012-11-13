import sys
import unittest2
import mock
import os
from mock import patch
from ec2cluster.base import BaseCluster, PostgresqlCluster, ScriptCluster
from ec2cluster import settings


path = os.path.dirname(__file__)
parent = os.path.join(path, '../')
sys.path.append(parent)


class BaseTest(unittest2.TestCase):

    def get_metadata(self):
        """ Returns some 'fake userdata', to simulate running in EC2.
        """
        return {
            'cluster': 'test-cluster',
            'instance-id': 'i-12345',
            'public-hostname': 'dummy'
        }


@patch.multiple(ScriptCluster,
    determine_role=mock.DEFAULT,
    get_metadata=mock.DEFAULT,
    acquire_master_cname=mock.DEFAULT,
    prepare_master=mock.DEFAULT,
    prepare_slave=mock.DEFAULT,
)
@patch.multiple('subprocess',
    check_call=mock.DEFAULT
)
class ScriptClusterTest(BaseTest):
    def test_init_master(self, *args, **kwargs):
        kwargs['determine_role'].return_value = BaseCluster.MASTER
        kwargs['get_metadata'].return_value = self.get_metadata()
        self.cluster = ScriptCluster()
        self.cluster.initialise()
        kwargs['prepare_master'].assert_called_with()
        kwargs['check_call'].assert_called_with(['/etc/init.d/testservice', 'start'])
        kwargs['prepare_master'].assert_called_with()

    def test_init_slave(self, *args, **kwargs):
        kwargs['determine_role'].return_value = BaseCluster.SLAVE
        kwargs['get_metadata'].return_value = self.get_metadata()
        self.cluster = ScriptCluster()
        self.cluster.initialise()
        kwargs['prepare_slave'].assert_called_with()
        kwargs['check_call'].assert_called_with(['/etc/init.d/testservice', 'start'])


@patch.multiple(PostgresqlCluster,
    determine_role=mock.DEFAULT,
    get_metadata=mock.DEFAULT,
    acquire_master_cname=mock.DEFAULT,
    add_to_slave_cname_pool=mock.DEFAULT,
    remove_from_slave_cname_pool=mock.DEFAULT,
    write_recovery_conf=mock.DEFAULT,
    configure_cron_backup=mock.DEFAULT,

)
@patch.multiple('subprocess',
    check_call=mock.DEFAULT
)
class PostgresqlClusterTest(BaseTest):
    """ Tests a postgresql cluster.

        Example recovery template file:
            standby_mode = on
            recovery_target_timeline = latest
            pause_at_recovery_target = false
            restore_command = '/usr/bin/s3cmd --config=/var/lib/postgresql/.s3cfg get s3://%(cluster)s/archive/wal/%%f %%p'
            primary_conninfo = 'host=%(master_cname)s port=5432 user=postgres password=secret sslmode=disable
    """
    def test_init_master(self, *args, **kwargs):
        kwargs['determine_role'].return_value = BaseCluster.MASTER
        kwargs['get_metadata'].return_value = self.get_metadata()
        self.cluster = PostgresqlCluster()
        self.cluster.initialise()
        kwargs['configure_cron_backup'].assert_called_with()

    def test_init_slave(self, *args, **kwargs):
        kwargs['determine_role'].return_value = BaseCluster.SLAVE
        kwargs['get_metadata'].return_value = self.get_metadata()
        self.cluster = PostgresqlCluster()
        self.cluster.initialise()
        kwargs['write_recovery_conf'].assert_called_with(settings.RECOVERY_TEMPLATE_SLAVE)
