import sys
import unittest2
import mock
import contextlib
import os

path = os.path.abspath(__file__)
parent = os.path.join(path, '../', '../')
sys.path.append(parent)

from ec2_cluster.base import BaseCluster, PostgresqlCluster

class BaseTest(unittest2.TestCase):

    def get_metadata(self):
        """ Returns some 'fake userdata', to simulate running in EC2.
        """
        return {
            'cluster': 'test-cluster'
        }

    def _get_mocks(self):
        """ Returns a list of mocks which are used in most tests - additional mocks will be
            added by test functions if necessary.
        """
        return {
            'determine_role': mock.DEFAULT,
            'get_metadata': mock.DEFAULT,
            'start_process': mock.DEFAULT,
            'prepare_slave': mock.DEFAULT,
            'poll_process': mock.DEFAULT,
        }

    def _set_mock_return_values(self, mocks):
        """ Sets the return_value for some mocks - some mocks will always return the same value
            no matter the test.
        """
        true_mocks = ['start_process', 'prepare_slave', 'acquire_master_cname']

        for k, v in mocks.iteritems():
            if k in true_mocks:
                v.return_value = True

        if 'get_metadata' in mocks:
            mocks['get_metadata'].return_value = self.get_metadata()

        return mocks

class BaseClusterTest(BaseTest):
    """ Tests super-basic functionality, making sure all of the correct functions have been called.
    """

    def test_init_master(self):
        mocks = self._get_mocks()
        mocks.update({
            'acquire_master_cname': mock.DEFAULT,
        })

        with mock.patch.multiple(BaseCluster, **mocks) as values:
            mocks = self._set_mock_return_values(mocks)
            values['determine_role'].return_value = BaseCluster.MASTER
            values['poll_process'].return_value = True

            self.cluster = BaseCluster()
            self.cluster.initialise()
            values['start_process'].assert_called_with()
            values['poll_process'].assert_called_with()
            values['acquire_master_cname'].assert_called_with()

    def test_init_slave(self):
        mocks = self._get_mocks()
        with mock.patch.multiple(BaseCluster, **mocks) as values:
            mocks = self._set_mock_return_values(mocks)
            values['determine_role'].return_value = BaseCluster.SLAVE
            values['poll_process'].return_value = True
            
            self.cluster = BaseCluster()
            self.cluster.initialise()
            values['determine_role'].assert_called_with()
            values['prepare_slave'].assert_called_with()


class PostgresqlClusterTest(BaseTest):
    
    def test_init_master(self):
        mocks = self._get_mocks()
        
        with mock.patch.multiple(PostgresqlCluster, **mocks) as values:
            values['determine_role'].return_value = BaseCluster.MASTER
            values['poll_process'].return_value = True

            self.cluster = PostgresqlCluster()
            self.cluster.initialise()

            values['start_process'].assert_called_with()
            values['prepare_master'].assert_called_with()


