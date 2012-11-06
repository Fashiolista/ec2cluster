Generic clustering package for EC2, suitable for redis/postgresql etc.

This package makes it easier to deploy clustered applications such as PostgreSQL and Redis on EC2 by handling generic logic, including::
    
    * Deciding which role the instance should assume
    * Creating and updating DNS records
    * Preparing the instance for its role (e.g. writing a recovery.conf file for postgres)

Basic Usage:
    
The default ec2cluster classes assume your EC2 instances have JSON-encoded user data containing some specific attributes. The following attributes are required::
    
    * cluster - the name of the cluster, e.g. maindb

Install ec2cluster with pip::
    
    pip install ec2cluster

Create a configuration file::
    
    MASTER_CNAME = 'master.%(cluster)s.example.com'
    SLAVE_CNAME = 'slave.%(cluster)s.example.com'
    INIT_MASTER_SCRIPT = '/path/to/some_script.py'
    INIT_SLAVE_SCRIPT = '/path/to/another_script.py'

Run ec2cluster, specifying the path to the config file::
    
    ec2cluster init # initialise the cluster service
    ec2cluster promote # promote a slave to the master role


PostgreSQL cluster:
-------------------

When starting a postgres read-slave, a file named recovery.conf must be written to the postgres configuration directory. A template file is used to make it easy to customise your recovery options.

Config file::
    
    MASTER_CNAME = 'master.%(cluster)s.example.com'
    SLAVE_CNAME = 'slave.%(cluster)s.example.com'
    RECOVERY_TEMPLATE = '/path/to/template.conf'
    RECOVERY_FILENAME = '/var/lib/postgresql/9.1/main/recovery.conf'

In the recovery template file, specify the options required for your read-slaves. Instance metadata and userdata can be used for string replacement. For example::
    
    standby_mode = on
    recovery_target_timeline = latest
    pause_at_recovery_target = false
    restore_command = '/usr/bin/s3cmd --config=/var/lib/postgresql/.s3cfg get s3://%(cluster)s/archive/wal/%%f %%p'
    primary_conninfo = 'host=%(master_cname) port=5432 user=postgres password=secret sslmode=disable

Note the use of "%%f" - because we are using string formatting we need to escape the percentage sign in order to end up with "%f" as required by postgres.

