import json
import boto
import dns
import os


# TODO move to settings
MASTER_CNAME = 'master.%(cluster)s.example.com'



class EC2Mixin(object):
    def get_metadata(self):
        data = json.loads(boto.utils.get_instance_userdata())
        data.update(boto.utils.get_instance_metadata())
        return data

    def acquire_master_cname(self, force=False):
        """ Use Route53 to update the master_cname record to point to this instance.

            If the CNAME already exists and force is False, an exception will be raised.
            Setting force to True will cause this function to 'take' the DNS record.
        """
        raise NotImplementedError

    def add_to_slave_cname_pool(self):
        """ Add this instance to the slave_cname weighted resource record DNS pool.
        """
        raise NotImplementedError


class VagrantMixin(object):
    def get_metadata(self):
        return os.environ

def get_cluster_class(infrastructureClass, serviceClass):
    clusterClass = type('className', (serviceClass, infrastructureClass), {})


class BaseCluster(object):
    """ Base class for generic master/slave operations.
    """

    MASTER = 'master'
    SLAVE = 'slave'
    POLL_TIMEOUT = 60

    def get_metadata(self):
        raise NotImplementedError

    def __init__(self):
        self.metadata = self.get_metadata()
        self.master_cname = self.get_master_cname()
        self.roles = self.get_roles()

    def get_roles(self):
        return {
            self.MASTER: self.prepare_master,
            self.SLAVE: self.prepare_slave
        }

    def initialise(self):
        """ Initialises this server as a master or slave.
        """
        self.role = self.determine_role()
        if self.role in self.roles:
            # Call the function for this role, as declared in get_roles().
            self.roles[self.role]()
        else:
            raise Exception('Unrecognised role: %s' % self.role)
        self.start_process()
        # Poll the process until it either starts successfully, or fails. This will result
        # in a call to process_started or process_failed.
        self.poll_process()

    def get_master_cname(self):
        """ Returns the CNAME of the master server for this cluster.
        """
        return MASTER_CNAME % self.metadata

    def determine_role():
        """ Should we be a master or a slave?

            If the self.master_cname DNS record exists, we should be a slave.
        """
        try:
            answers = dns.resolver.query(self.master_cname, 'CNAME')
        except dns.exceptions.NXDOMAIN:
            return self.MASTER
        else:
            return self.SLAVE

    def prepare_master(self):
        """ Initialise the master server.
        """
        self.acquire_master_cname()

    def prepare_slave(self):
        """ Initialise the slave server.
        """
        raise NotImplementedError

    def acquire_master_cname(self):
        """ Updates the master CNAME to point to this instance's public DNS name.
        """
        raise NotImplementedError

    def poll_process(self):
        raise NotImplementedError

    def start_process(self):
        """ Attempt to start the process (e.g. via supervisorctl).
        """
        raise NotImplementedError

    def process_started(self):
        if self.role == MASTER:
            self.acquire_master_cname()

    def process_failed(self):
        print 'oh shit something broke'


# TODO move to settings
POSTGRESQL_DIR = '/var/lib/postgresql/9.1/main'
RECOVERY_FILENAME = '%s/recovery.conf' % POSTGRESQL_DIR
RECOVERY_TEMPLATE = '/tmp/recovery.conf'


class PostgresqlCluster(BaseCluster):
    """ PostgreSQL cluster.

        Master: Starts postgres normally
        Slave: Writes a recovery.conf file and starts postgres as a read slave
    """
    def start_process(self):
        pass

    def poll_process(self):
        pass

    def write_recovery_conf(self):
        """ Using the template specified in settings, create a recovery.conf file in the
            postgres config dir.
        """

    def prepare_master(self):
        """ Init postgres as a master.
        """
        pass

    def prepare_slave(self):
        """ Init postgres as a read-slave by writing a recovery.conf file.
        """
        self.write_recovery_conf()
