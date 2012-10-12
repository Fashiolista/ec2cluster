Generic clustering package for EC2, suitable for redis/postgresql etc.

This package makes it easier to deploy clustered applications such as PostgreSQL and Redis on EC2 by handling generic logic, including::
    
    * Deciding which role the instance should assume
    * Creating and updating DNS records

Basic Usage:
    
The default ec2-cluster classes assume your EC2 instances have JSON-encoded user data containing some specific attributes. The following attributes are required::
    
    * cluster - the name of the cluster, e.g. maindb

Install ec2-clusters with pip::
    
    pip install ec2-clusters

Create a configuration file::
    
    MASTER_CNAME = 'master.%(cluster)s.example.com'
    SLAVE_CNAME = 'slave.%(cluster)s.example.com'
    INIT_MASTER_SCRIPT = '/path/to/some_script.py'
    INIT_SLAVE_SCRIPT = '/path/to/another_script.py'
