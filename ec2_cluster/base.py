import json
from boto.utils import get_instance_userdata, get_instance_metadata
import dns
import os
import subprocess
import psycopg2
import logging
from crontab import CronTab

# TODO move to settings
MASTER_CNAME = 'master.%(cluster)s.example.com'


class EC2Mixin(object):
    def get_metadata(self):
        data = json.loads(get_instance_userdata())
        data.update(get_instance_metadata())
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
        data = os.environ
        data['cluster'] = 'vagranttest'
        return data

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

    def __init__(self, settings=None):
        self.logger = logging.getLogger('%s.%s' % (__name__, self.__class__.__name__))
        self.logger.warning('test')

        self.settings = settings
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
            logger.critical('Unknown role: %s' % self.role)
            raise exceptions.UnknownRole()
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
        raise NotImplementedError

    def prepare_slave(self):
        """ Initialise the slave server.
        """
        raise NotImplementedError

    def acquire_master_cname(self):
        """ Updates the master CNAME to point to this instance's public DNS name.
        """
        raise NotImplementedError

    def release_master_cname(self):
        """ Deletes the master CNAME if it is pointing to this instance. Called when
            the master process fails to start.
        """
        raise NotImplementedError

    def poll_process(self):
        raise NotImplementedError

    def start_process(self):
        """ Attempt to start the process (e.g. via supervisorctl).
        """
        raise NotImplementedError

    def process_started(self):
        if self.role == self.MASTER:
            self.acquire_master_cname()

    def process_failed(self):
        print 'oh shit something broke'


# TODO settings
SERVICE_NAME = 'testservice'
MASTER_SCRIPT = '/tmp/master.py'
SLAVE_SCRIPT = '/tmp/slave.py'

class ScriptCluster(BaseCluster):
    """ Basic cluster - simply runs scripts when preparing a master/slave, and
        starts a service via init.d scripts.
    """
    def start_process(self):
        subprocess.check_call(['/etc/init.d/%s' % SERVICE_NAME, 'start'])

    def prepare_master(self):
        subprocess.check_call([MASTER_SCRIPT, ])

    def prepare_slave(self):
        subprocess.check_call([SLAVE_SCRIPT, ])

    def poll_process(self):
        self.process_started()



# TODO move to settings
PG_DIR = '/var/lib/postgresql/9.1/main'
RECOVERY_FILENAME = '%s/recovery.conf' % PG_DIR
RECOVERY_TEMPLATE = '/tmp/recovery.conf'
PG_CTL = '/usr/lib/postgresql/9.1/bin/pg_ctl'
PG_USER = 'postgres'
PG_TIMEOUT = 20 # Time to wait when attempting to connect to postgres


#class PostgresqlCluster(EC2Mixin, BaseCluster):
class PostgresqlCluster(VagrantMixin, BaseCluster):
    """ PostgreSQL cluster.

        Master: Starts postgres normally
        Slave: Writes a recovery.conf file and starts postgres as a read slave

        The prepare_[master|slave] functions will put the instance in a state whereby
        '/etc/init.d/postgresql start' can be executed.
    """
    def start_process(self):
        """ Starts postgresql using the init.d scripts.
        """
        subprocess.check_call(['/etc/init.d/postgresql', 'start'])

    def poll_process(self):
        pass

    def write_recovery_conf(self):
        """ Using the template specified in settings, create a recovery.conf file in the
            postgres config dir.
        """
        data = dict(self.metadata.items() + self.settings.items())
        data.update(
            {'master_cname': self.master_cname}
        )
        template_file = open(self.settings['recovery_template'], 'r')
        template = template_file.read()
        template_file.close()
        output = open(self.settings['recovery_filename'], 'w')
        output.write(template % data)
        output.close()

    def configure_cron_backup(self):
        """ Creates a cronjob to perform backups via snaptastic.

            Default behaviour is to take backups at 08:00 each day.
        """
        cron = CronTab('postgres')
        backup_job = cron.new(command='/usr/bin/echo testingcron',
            comment='Created by ec2_cluster')
        backup_job.hour.every(8)
        cron.write()

    def prepare_master(self):
        """ Init postgres as a master.
        """
        self.configure_cron_backup()

    def prepare_slave(self):
        """ Init postgres as a read-slave by writing a recovery.conf file.
        """
        self.write_recovery_conf()

    def _get_conn(self, host=None, dbname=None, user=None):
        """ Returns a connection to postgresql server.
        """
        conn_str = ''
        if host:
            conn_str += 'host="%s" ' % host
        if dbname:
            conn_str += 'dbname=%s ' % dbname
        if user:
            conn_str += 'user=%s ' % user

        # TODO verify this works as expected - should make it quicker to detect a failed
        # master, as we don't have to wait the full 60 seconds.
        conn_str += 'timeout=%s' % PG_TIMEOUT

        return psycopg2.connect(conn_str)

    def check_master(self):
        """ Returns true if there is a postgresql server running on the master CNAME
            for this cluster, and this instance believes it is the master.
            
            This is a safety check to avoid promoting a slave when we already have a
            master in the cluster.
        """
        conn = self._get_conn(host=self.master_cname)
        cur = conn.cursor()
        cur.execute('SELECT pg_is_in_recovery()')
        res = cur.fetchone()
        if res == 't':
            # We server we connected to thinks it is a slave
            return False

        # Perform a basic query to make sure postgresql is operational
        cur.execute('SELECT 1')
        res = cur.fetchone()
        if res == '1':
            return True

        # If we get here, something went wrong
        return False

    def check_slave(self):
        """ Returns true if there is a postgresql server running on localhost, and
            the server is in recovery mode (i.e. it is a read slave).
        """
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute('SELECT pg_is_in_recovery()')
        res = cur.fetchone()
        # TODO result won't be a plain string
        return (res == 't')

    def promote(self, force=False):
        """ Promote a read-slave to the master role.

            If force is True, safety checks are ignored and the promotion is forced.
        """
        active_master = self.check_master()
        if active_master == True:
            print 'There is an active server at %s' % self.master_cname
            if force == False:
                print 'Refusing to promote slave without "force", exiting.'
                return
        promote_cmd = 'sudo -u %(user)s %(pg_ctl)s -D %(dir)s promote' % {
            'user': PG_USER,
            'pg_ctl': PG_CTL,
            'dir': PG_DIR}
        print 'Running promote command: %s' % promote_cmd
        # TODO error checking, log output
        subprocess.check_call(promote_cmd.split())












