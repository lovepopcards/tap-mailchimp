import json
from json import load, loads

class JsonObject:
    def __init__(self, d, required_keys=None, defaults=None):
        if required_keys:
            for k in required_keys:
                setattr(self, k, d[k])
        if defaults:
            for k, v in defaults.items():
                setattr(self, k, d.get(k, v))
    def __json__(self):
        return {k: getattr(self, k) for k in self.__dict__ if k[:1] != '_'}
    def __str__(self):
        return dumps(self)
    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.__json__())

class JsonEncoderExt(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, '__json__'):
            return obj.__json__()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, set):
            return list(obj)
        return super().default(obj)

def dump(obj, fp, *, cls=JsonEncoderExt, **kwargs):
    json.dump(obj, fp, cls=cls, **kwargs)

def dumps(obj, *, cls=JsonEncoderExt, **kwargs):
    return json.dumps(obj, cls=cls, **kwargs)
