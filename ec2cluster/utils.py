import logging.config

BASE_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'basic': {
            'format': '%(message)s [%(name)s:%(levelname)s]'
        },
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
            'stream': 'ext://sys.stdout',
        },
        'error': {
            'level': 'ERROR',
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
            'stream': 'ext://sys.stderr',
        },
    },
    'loggers': {
        'ec2cluster': {
            'handlers': ['default', 'error'],
            'level': 'DEBUG',
            'propagate': True
        },
    }
}


def configure_logging():
    logging.config.dictConfig(BASE_LOGGING_CONFIG)
