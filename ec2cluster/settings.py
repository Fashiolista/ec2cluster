from ec2cluster.default_settings import *
import imp
import logging
import os


logger = logging.getLogger(__name__)

setting_files = [
    os.path.join('/etc', 'ec2cluster_settings.py'),
    os.path.join('/etc', 'ec2cluster', 'ec2cluster_settings.py'),
]
logger.info('trying ec2cluster_settings in sys.path')


SETTINGS_FILE = None
logger.info('didnt find ec2cluster settings file in sys.path, search the filesystem')
for settings_file in setting_files:
    logger.info('trying settings file %s', settings_file)
    if os.path.isfile(settings_file):

        ec2cluster_settings = imp.load_source(
            'ec2cluster_settings', settings_file)
        module_variables = [k for k in dir(
            ec2cluster_settings) if not k.startswith('_')]
        module_dict = dict([(k, getattr(
            ec2cluster_settings, k)) for k in module_variables])
        globals().update(module_dict)
        logger.info('found settings file at %s', settings_file)
        SETTINGS_FILE = settings_file
        break
else:
    error_format = 'Couldnt locate settings file in sys.path or %s'
    error_message = error_format % setting_files
    logger.warn(error_message)
