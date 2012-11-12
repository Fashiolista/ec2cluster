

# Generic settings
MASTER_CNAME = 'master.%(cluster)s.goteam.be'
SLAVE_CNAME = 'slave.%(cluster)s.goteam.be'
MASTER_CNAME_TTL = '60'
SLAVE_CNAME_TTL = '60'

# AWS settings
ROUTE53_ZONE_ID = 'Z1Q3XCRU2HIT7F'
# TODO use proper keys with correct perms, these just have route53 perms.
# make an IAM policy template describing required permissions
AWS_ACCESS_KEY_ID = 'AKIAJQK7ZX65SKSWD74Q'
AWS_SECRET_ACCESS_KEY = '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'


# Postgres settings
PG_DIR = '/var/lib/postgresql/9.1/main'
RECOVERY_FILENAME = '%s/recovery.conf' % PG_DIR
RECOVERY_TEMPLATE_SLAVE = '/etc/postgresql/9.1/main/recovery_template_slave.conf'
RECOVERY_TEMPLATE_MASTER = '/etc/postgresql/9.1/main/recovery_template_master.conf'
PG_CTL = '/usr/lib/postgresql/9.1/bin/pg_ctl'
PG_USER = 'postgres'
PG_TIMEOUT = 20  # Time to wait when attempting to connect to postgres
