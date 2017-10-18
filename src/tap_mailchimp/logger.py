from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
import traceback
import re
import singer
import tap_mailchimp.jsonext as json

_logger = None

def get_logger():
    global _logger
    if _logger is None:
        _logger = singer.get_logger()
    return _logger

_level = {DEBUG: 'debug',
          INFO: 'info',
          WARNING: 'warning',
          ERROR: 'error',
          CRITICAL: 'critical'}

def _msg(level, obj):
    try:
        return json.dumps(dict(obj, level=_level.get(level, 'notset')),
                          skipkeys=True)
    except TypeError as e:
        # Last effort to log as JSON
        return {'action': 'stringify',
                'reason': 'unserializable object',
                'obj_type': type(obj).__name__,
                'obj_str': str(obj)}

def log_json(logger, level, obj):
    logger.log(level, 'JSON: %s', _msg(level, obj))

def log(level, obj):
    log_json(get_logger(), level, obj)

def debug(obj):
    log(DEBUG, obj)

def info(obj):
    log(INFO, obj)

def warning(obj):
    log(WARNING, obj)

def error(obj):
    log(ERROR, obj)

def exception(e, **context):
    error({'type': 'exception',
           'exception_type': type(e).__name__,
           'message': str(e),
           'args': e.args,
           'context': context,
           'traceback': traceback.format_exception(etype=type(e),
                                                   value=e,
                                                   tb=e.__traceback__)})

def parse(line):
    match = re.match(r'^[A-Z]+ JSON: (.*)$', line)
    if match:
        return json.loads(match.group(1))
    else:
        return None
