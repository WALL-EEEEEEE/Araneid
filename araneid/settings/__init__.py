order=1
logger = {
    "version": 1,
    "formatters": {
        "araneid.standard_format": {
            "format": "[ %(asctime)s, %(levelname)s, %(filename)s, %(lineno)d, %(threadName)s ] - %(message)s",
        },
    },
    "handlers": {
        'araneid.logger': {
            'class': 'logging.StreamHandler',
            'formatter': 'araneid.standard_format',
            "stream"  : "ext://sys.stdout"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["araneid.logger"]
    }
}