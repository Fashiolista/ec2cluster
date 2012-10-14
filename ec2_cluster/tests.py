import sys
import unittest2
import mock
import contextlib
import os

from mock import patch

path = os.path.abspath(__file__)
parent = os.path.join(path, '../', '../')
sys.path.append(parent)

from ec2_cluster.base import BaseCluster, PostgresqlCluster, ScriptCluster

class BaseTest(unittest2.TestCase):

    def get_metadata(self):
        """ Returns some 'fake userdata', to simulate running in EC2.
        """
        return {
            'cluster': 'test-cluster'
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
        kwargs['acquire_master_cname'].assert_called_with()

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
)
@patch.multiple('subprocess',
    check_call=mock.DEFAULT
)
class PostgresqlClusterTest(BaseTest):

    def setUp(self):
        self.settings = {
            'recovery_template': '/tmp/recovery.tmp'
        }

        recovery_template = open(self.settings['recovery_template'], 'w')
        recovery_template.write('test')
        recovery_template.close()
    
    def test_init_master(self, *args, **kwargs):
        kwargs['determine_role'].return_value = BaseCluster.MASTER
        kwargs['get_metadata'].return_value = self.get_metadata()
        self.cluster = PostgresqlCluster()
        self.cluster.initialise()

    def test_init_slave(self, *args, **kwargs):
        kwargs['determine_role'].return_value = BaseCluster.SLAVE
        kwargs['get_metadata'].return_value = self.get_metadata()
        self.cluster = PostgresqlCluster(self.settings)
        self.cluster.initialise()
        kwargs['write_recovery_conf'].assert_called_with()

