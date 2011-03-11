from __future__ import with_statement
from os import stat
from time import time, mktime
from rfc822 import parsedate
from calendar import timegm
import urllib2
import re
import weakref
import new

from dictdb import DictDB

try:
    from threading import Lock
except ImportError:
    from dummy_threading import Lock

NOT_INITIALIZED = object()

class Entry(object):
    def __init__(self, key):
        self.key = key
        self.value = NOT_INITIALIZED
        self.lock = Lock()

    def __getstate__(self):
        odict = self.__dict__.copy()
        if 'lock' in odict:
            del odict['lock']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self.lock = Lock()        

    def __str__(self):
        return str(self.value)

class Cache(object):
    def __init__(self, filename=None, max_size=0):
        self.max_size = max_size
        self.filename = filename
        if filename:
            self.dict = DictDB(filename, format='pickle')
        else:
            self.dict = {}
        self.lock = Lock()
        self.hits = 0
        self.misses = 0

        if self.max_size:
            self.head = Entry(None)
            self.head.previous = self.head
            self.head.next_ = self.head

    def sync(self):
        if self.filename:
            self.dict.sync()
    def close(self):
        if self.filename:
            self.dict.close()

    def __setitem__(self, name, value):
        key = self.key(name)
        entry = self.get_entry(key)

        with entry.lock:
            self.pack(entry, value)
            self.commit()
        
    def __getitem__(self, name):
        return self.checkitem(name)[2]

    def __delitem__(self, name):
        with self.lock:
            key = self.key(name)
            del self.dict[key]

    def get_entry(self, key):
        with self.lock:
            entry = self.dict.get(key)
            if not entry:
                entry = Entry(key)
                self.dict[key] = entry
                if self.max_size:
                    entry.next_ = entry.previous = None
                    self.access(entry)
                    self.checklru()
            elif self.max_size:
                self.access(entry)
            return entry

    def checkitem(self, name):
        key = self.key(name)
        entry = self.get_entry(key)

        with entry.lock:
            value = self.unpack(entry)
            is_new = False
            if value is NOT_INITIALIZED:
                self.misses += 1
                opened = self.check(key, name, entry)
                value = self.build(key, name, opened, entry)
                is_new = True
                self.pack(entry, value)
                self.commit()
            else:
                self.hits += 1
                opened = self.check(key, name, entry)
                if opened is not None:
                    value = self.build(key, name, opened, entry)
                    is_new = True
                    self.pack(entry, value)
                    self.commit()
            return (is_new, key, value, entry)

    def mru(self):
        if self.max_size:
            with self.lock:
                return self.head.previous.key
        else:
            return None

    def lru(self):
        if self.max_size:
            with self.lock:
                return self.head.next_.key
        else:
            return None

    def key(self,name):
        return name

    def commit(self):
        pass

    def clear(self):
        with self.lock:
            self.dict.clear()
            if self.max_size:
                self.head.next_ = self.head
                self.head.previous = self.head

    def check(self, key, name, entry):
        return None

    def build(self, key, name, opened, entry):
        raise NotImplementedError()

    def access(self, entry):
        if entry.next_ is not self.head:
            if entry.previous is not None:
                entry, previous, next_ = entry.next_
                entry.next_.previous = entry.previous
            entry.previous = self.head.previous
            entry.previous.next_ = entry
            entry.next_ = self.head
            entry.next_.previous = entry
            if self.head.next_ is self.head:
                self.head.next_=entry

    def checklru(self):
        if len(self.dict) > self.max_size:
            lru = self.head.next_
            lru.previous.next_ = lru.next_
            lru.next_.previous = lru.previous
            del self.dict[lru.key]

    def pack(self, entry, value):
        entry.value = value

    def unpack(self, entry):
        return entry.value


def parseRFC822Time(t):
    return mktime(parsedate(t))

class HTTPEntity(object):
    def __init__(self, entity, metadata):
        self.entity = entity
        self.metadata = metadata

    def __repr__(self):
        return "<HTTPEntity: %s, %s>" % (repr(self.entity), self.metadata)

    def __str__(self):
        return self.entity

re_max_age=re.compile('max-age\s*=\s*(\d+)', re.I)

class HTTPCache(Cache):
    def sync(self):
        super(HTTPCache, self).sync()

    def check(self, key, name, entry):
        request = urllib2.Request(key)

        try:
            if time() < entry.expires:
                return None
        except AttributeError:
            pass

        try:
            header, value = entry.validator
            request.headers[header] = value
        except AttributeError:
            pass

        opened = None
        try:
            opened = urllib2.urlopen(request)
            headers = opened.info()

            expiration = False
            try:
                match = re_max_age.match(headers['cache-control'])
                if match:
                    entry.expires = time() + int(match.group(1))
                    expiration = True
            except (KeyError, ValueError):
                pass

            if not expiration:
                try:
                    date = parseRFC822Time(headers['date'])
                    expires = parseRFC822Time(headers['expires'])
                    entry.expires = time() + (expires - date)
                    expiration = True
                except KeyError:
                    pass

            validation = False
            try: 
                entry.validator = 'If-None-Match', headers['etag']
                validation = True
            except KeyError:
                pass

            if not validation:
                try:
                    entry.validator='If-Modified-Since', headers['last-modified']
                except KeyError:
                    pass

            return opened
        except urllib2.HTTPError, error:
            if opened: 
                opened.close()
            if error.code == 304:
                return None
            else:
                raise error

    def build(self, key, name, opened, entry):
        try:
            return HTTPEntity(opened.read(), dict(opened.info()))
        finally:
            opened.close()

if __name__ == "__main__":
    hc = HTTPCache(filename="test.pk")
    t1 = time()
    print hc['http://ws.audioscrobbler.com/2.0/?api_key=1bf12731bd3b7a5a821d9455362896b4&method=user.getWeeklyChartList&user=exgratia']
    t2 = time()
    print t2 - t1

    print "(((((((("
    t1 = time()
    print hc['http://ws.audioscrobbler.com/2.0/?api_key=1bf12731bd3b7a5a821d9455362896b4&method=user.getWeeklyChartList&user=exgratia']
    t2 = time()
    print t2 - t1

    print hc.__dict__
    hc.close()

    hc = HTTPCache(filename="test.pk")
    t1 = time()
    print hc['http://ws.audioscrobbler.com/2.0/?api_key=1bf12731bd3b7a5a821d9455362896b4&method=user.getWeeklyChartList&user=exgratia']
    t2 = time()
    print t2 - t1


            


