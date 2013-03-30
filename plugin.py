# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import socket
import re
import sys
import operator
import threading

from supybot.commands import getText, addConverter, wrap, optional, commalist, context
import supybot.callbacks as callbacks
import supybot.conf as conf
import supybot.ircdb as ircdb
import supybot.log as supy_log

from supybot.utils.web import htmlToText
from datetime import datetime, timedelta
from time import time, mktime

from collections import defaultdict

import urllib
import urllib2
import urlparse
import xml.etree.cElementTree as ElementTree

#for multitagged
import csv

from dictdb import DictDB
import pymongo

from LastfmError import LastfmError
from localsettings import api_key

discogs_url_base = "http://discogs/com/"
api_url_base = "http://localhost:6081/2.0/?api_key=%s" % api_key
db = pymongo.Connection().anni.cache
legacy_userdb = DictDB(os.path.join(conf.supybot.directories.data(), 'users.pklz'), mode=0600)

log = supy_log.getPluginLogger('Lastfm')

# change mores output length temporarily, 0 = max output
class mores:
    def __init__(self, length):
        self.new_mores = length

    def __enter__(self):
        self.old_mores = conf.supybot.reply.mores.length()
        conf.supybot.reply.mores.length.setValue(self.new_mores)

    def __exit__(self, type, value, traceback):
        conf.supybot.reply.mores.length.setValue(self.old_mores)


# or_now_playing means: Look of this or try obtaining from
# caller's now playing.  Only works for artists atm
class or_now_playing(context):
    def __init__(self, spec):
        self.__parent = super(or_now_playing, self)
        self.__parent.__init__(spec)

    def __call__(self, irc, msg, args, state):
        try:
            self.__parent.__call__(irc, msg, args, state)
        except IndexError:
            if self.spec == 'lfm_artist':
                caller = find_account(irc, msg)
                track = caller.getRecentTracks(limit=1)[0]
                if not track.now_playing:
                    raise
                state.args.append(track.artist)
            else:
                raise


def overall_period():
    return {'lfm_period': 'overall',
            'start': datetime(2002, 1, 1),
            'end': datetime.now()}


#[3|6|12|o|w [weeks]|m [months]|d <days>]
#returns (period, days)
def getPeriod(irc, msg, args, state):
    now = datetime.now()
    if args[0] == '-3':
        state.args.append({'lfm_period': '3month',
                           'start': now - timedelta(90),
                           'end': now})
        args.pop(0)
    elif args[0] == '-6':
        state.args.append({'lfm_period': '6month',
                           'start': now - timedelta(180),
                           'end': now})
        args.pop(0)
    elif args[0] == '-12':
        state.args.append({'lfm_period': '12month',
                           'start': now - timedelta(365),
                           'end': now})
        args.pop(0)
    elif args[0] == '-w':
        try:
            weeks = int(args[1])
            state.args.append({'lfm_period': '%d weeks' % weeks,
                               'start': now - timedelta(7*weeks),
                               'end': now})
            del args[:2]
        except:
            state.args.append({'lfm_period': '1 week',
                               'start': now - timedelta(7),
                               'end': now})
            args.pop(0)
    elif args[0] == '-m':
        try:
            months = int(args[1])
            state.args.append({'lfm_period': '%d months' % months,
                               'start': now - timedelta(30*months),
                               'end': now})
            del args[:2]
        except:
            state.args.append({'lfm_period': '1 month',
                               'start': now - timedelta(30),
                               'end': now})
            args.pop(0)
    elif args[0] == '-o':
        state.args.append(overall_period())
        args.pop(0)
    elif args[0] == '-d':
        try:
            days = int(args[1])
            state.args.append({'lfm_period': '%d days' % days,
                               'start': now - timedelta(days),
                               'end': now})
            del args[:2]
        except:
            state.errorInvalid('lfm_period', ' '.join(args[:2]))
    else:
        try:
            days = int(re.match(r'-d(\d+)', args[0]).group(1))
            state.args.append({'lfm_period': '%d days' % days,
                               'start': now - timedelta(days),
                               'end': now})
            args.pop(0)
        except:     # no matches
            state.args.append(overall_period())
addConverter('lfm_period', getPeriod)


def artistConverter(irc, msg, args, state):
    getText(irc, msg, args, state)
    try:
        state.args[-1] = find_artist(state.args[-1])
    except Exception, e:
        state.error('%s' % e)
addConverter('lfm_artist', artistConverter)


def groupConverter(irc, msg, args, state):
    getText(irc, msg, args, state)
    state.args[-1] = Group(state.args[-1])
addConverter('lfm_group', groupConverter)


def tagConverter(irc, msg, args, state):
    from supybot.commands import getText
    getText(irc, msg, args, state)
    state.args[-1] = Tag(state.args[-1])
addConverter('lfm_tag', tagConverter)


def userConverter(irc, msg, args, state):
    if '#' in args[0]:
        state.errorInvalid('lfm_user', args[0])
    try:
        state.args.append(find_account(irc, msg, args[0]))
        args.pop(0)
    except Exception, e:
        state.error('%s' % e)
    #state.args.append(args.pop(0))
addConverter('lfm_user', userConverter)


def normalReply(irc, mod, msg, to=None):
    irc.reply(msg.encode('utf-8', 'ignore'), to=to)


def specialReply(irc, mod, msg, to=None):
    if(len(mod) >= 2 and len(mod[1]) > 2 and
        mod[1][0] == mod[1][1] and
        mod[1][0] in conf.supybot.reply.whenAddressedBy.chars()):
        irc.reply(msg.encode('utf-8', 'ignore'), private=True, notice=True)
    else:
        normalReply(irc, None, msg, to=to)


def command_name(msg):
    return ' '.join(msg.args[1:])


#msg is ircmsg, e is exception
def error_msg(msg, e):
    return "[%s]: %s" % (command_name(msg), str(e))


class Stats(object):
    def __init__(self, listeners=None,
                       playcount=None,
                       userplaycount=None,
                       tagcount=None,
                       count=None,
                       match=None,
                       rank=None,
                       weight=None,
                       attendance=None,
                       reviews=None,
                       score=None,
                       matches=None):
        self.listeners = int(listeners or 0)
        self.playcount = int(playcount or 0)
        self.userplaycount = int(userplaycount or 0)
        self.tagcount = int(tagcount or 0)
        self.count = int(count or 0)
        self.match = float(match or 0)
        self.rank = int(rank or 0)
        self.weight = float(weight or 0)
        self.attendance = int(attendance or 0)
        self.reviews = int(reviews or 0)
        self.score = float(score or 0)
        self.matches = int(matches or 0)


class Tag(object):
    def __init__(self, name=None, count=None, url=None, stats=None):
        self.name = name
        self.count = int(count or 0)
        self.url = url
        self.stats = stats

    def __cmp__(self, other):
        return cmp(self.name, other.name)

    def __repr__(self):
        return "<Tag: %s>" % self.name

    def getTopArtists(self):
        params = {'method': 'tag.getTopArtists', 'tag': self.name}
        data = fetch(api_url_base, params, None)

        return [Artist(artist_elem.find('name').text,
                       mbid=artist_elem.find('mbid').text,
                       url=artist_elem.find('url').text,
                       stats=Stats(rank=artist_elem.attrib['rank']))
                for artist_elem in data.findall('topartists/artist')]

    def getInfo(self):
        params = {'method': 'tag.getInfo', 'tag': self.name}
        data = fetch(api_url_base, params, None)
        wiki = data.find('tag/wiki')

        summary = wiki.find('summary').text or "none"
        content = wiki.find('content').text or "none"
        summary = summary.encode('ascii', 'ignore')
        content = content.encode('ascii', 'ignore')
        summary = htmlToText(summary).replace('\n', ' ')
        content = htmlToText(content).replace('\n', ' ')
        return {'summary': unicode(summary, 'utf-8', 'ignore'),
                'content': unicode(content, 'utf-8', 'ignore')}

    @staticmethod
    def getTopTags():
        params = {'method': 'tag.getTopTags'}
        data = fetch(api_url_base, params, None)

        return [Tag(name=tag_elem.find('name').text,
                    count=tag_elem.find('count').text,
                    url=tag_elem.find('url').text)
                for tag_elem in data.findall('toptags/tag')]

    @staticmethod
    def search(name, limit=None, page=None):
        params = {'method': 'tag.search',
                  'tag': name,
                  'limit': limit,
                  'page': page}
        data = fetch(api_url_base, params, None)

        return [Tag(name=tag_elem.find('name').text,
                    count=tag_elem.find('count').text,
                    url=tag_elem.find('url').text)
                for tag_elem in data.findall('results/tagmatches/tag')]


class Artist(object):
    def __init__(self, name=None,
                       mbid=None,
                       url=None,
                       stats=None,
                       tags=None,
                       bio=""):
        self.name = name
        self.mbid = mbid
        self.url = url
        self._stats = stats
        self._tags = tags
        self._bio = bio
        if stats:
            self.missing_stats = False
        else:
            self.missing_stats = True
        if tags:
            self.missing_tags = False
        else:
            self.missing_tags = True
        if bio:
            self.missing_bio = False
        else:
            self.missing_bio = True

    def __repr__(self):
        return "<Artist: %s>" % self.name

    @property
    def tags(self):
        if not self._tags and self.missing_tags:
            self._tags = self.getTopTags()
            self.missing_tags = False
        return self._tags

    @tags.setter
    def tags(self, value):
        self._tags = value
        self.missing_tags = False

    @property
    def stats(self):
        if not self._stats and self.missing_stats:
            print "fetching stats"
            a = self.getInfo()
            a.missing_stats = False
            self._stats = a.stats
            self.missing_stats = False
        return self._stats

    @stats.setter
    def stats(self, value):
        self._stats = value
        self.missing_stats = False

    @property
    def bio(self):
        if not self._bio and self.missing_bio:
            b = self.getBio()
            b.missing_bio = False
            self._bio = b.bio
            self.missing_bio = False
        return self._bio

    @bio.setter
    def bio(self, value):
        self._bio = value
        self.missing_bio = False

    def getSimilar(self, limit=None):
        params = {'method': 'artist.getSimilar',
                  'artist': self.name,
                  'limit': limit}
        data = fetch(api_url_base, params, None)

        return [Artist(artist_elem.find('name').text,
                       mbid=artist_elem.find('mbid').text,
                       url=artist_elem.find('url').text,
                       stats=Stats(match=artist_elem.find('match').text))
                for artist_elem in data.findall('similarartists/artist')]

    def getTopTags(self):
        params = {'method': 'artist.getTopTags', 'artist': self.name}
        data = fetch(api_url_base, params, None)

        return [Tag(name=tag_elem.find('name').text,
                    count=tag_elem.find('count').text,
                    url=tag_elem.find('url'))
                for tag_elem in data.findall('toptags/tag')]

    def getInfo(self, username=None, lang=None, autocorrect=1):
        params = {'method': 'artist.getInfo',
                  'artist': self.name,
                  'mbid': self.mbid,
                  'username': username,
                  'lang': lang,
                  'autocorrect': autocorrect}
        data = fetch(api_url_base, params, None)
        artist_elem = data.find('artist')

        def safe_find_text(el, tag):
            try:
                return el.find(tag).text
            except AttributeError:
                return None

        a = Artist(artist_elem.find('name').text,
                      mbid=artist_elem.find('mbid').text,
                      url=artist_elem.find('url').text,
                      stats=Stats(listeners=artist_elem.find('stats/listeners').text,
                                  playcount=artist_elem.find('stats/playcount').text,
                                  userplaycount=safe_find_text(artist_elem, 'stats/userplaycount')),
                      bio="")
        a.missing_stats = False
        return a

    def getBio(self, username=None, lang=None, autocorrect=1):
        params = {'method': 'artist.getInfo',
                  'artist': self.name,
                  'mbid': self.mbid,
                  'username': username,
                  'lang': lang,
                  'autocorrect': autocorrect}
        data = fetch(api_url_base, params, None)
        artist_elem = data.find('artist')

        bio = artist_elem.find('bio/summary').text or ""
        bio = bio.encode('ascii', 'ignore')
        bio = htmlToText(bio).replace('\n', ' ')
        bio = unicode(bio, 'utf-8', 'ignore')

        a = Artist(artist_elem.find('name').text, bio=bio)
        a.missing_bio = False
        return a

    def getTopAlbums(self):
        params = {'method': 'artist.getTopAlbums', 'artist': self.name}
        data = fetch(api_url_base, params, None)

        artist = data.find('topalbums').attrib['artist']
        return [Album(album_elem.find('name').text,
                      artist=artist,
                      mbid=album_elem.find('mbid').text,
                      url=album_elem.find('url').text,
                      stats=Stats(playcount=album_elem.find('playcount').text,
                                  rank=album_elem.attrib['rank']))
                for album_elem in data.findall('topalbums/album')]

    @staticmethod
    def search(name, limit=None, page=None):
        params = {'method': 'artist.search',
                  'artist': name,
                  'limit': limit,
                  'page': page}
        data = fetch(api_url_base, params, None)

        return [Artist(name=elem.find('name').text,
                       mbid=elem.find('mbid').text,
                       url=elem.find('url').text)
                for elem in data.findall('results/artistmatches/artist')]


class Track(object):
    def __init__(self, name=None,
                       artist=None,
                       album=None,
                       mbid=None,
                       url=None,
                       stats=None,
                       played_on=None,
                       now_playing=None,
                       duration=0):
        self.name = name
        self.artist = artist
        self.album = album
        self.mbid = mbid
        self.url = url
        self.stats = stats
        self.played_on = played_on
        self.now_playing = now_playing
        self.duration = duration

    def __repr__(self):
        return "<Track: %s, Artist: %s, Album: %s>" % (self.name, self.artist, self.album)

    def getInfo(self, autocorrect=1):
        params = {'method': 'track.getInfo', 'autocorrect': autocorrect}
        if self.mbid:
            params['mbid'] = self.mbid
        else:
            params['artist'] = self.artist.name
            params['track'] = self.name
        data = fetch(api_url_base, params, None)

        track_elem = data.find('track')

        return Track(track_elem.find('name').text,
                     mbid=track_elem.find('mbid').text,
                     url=track_elem.find('url').text,
                     stats=Stats(listeners=track_elem.find('listeners').text,
                                 playcount=track_elem.find('playcount').text),
                     duration=int(track_elem.find('duration').text or 0))

    def getTopTags(self, autocorrect=1):
        params = {'method': 'track.getTopTags',
                  'artist': self.artist.name,
                  'track': self.name,
                  'autocorrect': autocorrect}
        data = fetch(api_url_base, params, None)

        return [Tag(name=tag_elem.find('name').text,
                    count=tag_elem.find('count').text,
                    url=tag_elem.find('url'))
                for tag_elem in data.findall('toptags/tag')]


# tracks is a user's recent tracks, tracks[0] should be now playing
def now_playing_position(tracks):
    def make_time_tag(start_time, duration_ms):
        cur = datetime.now() - start_time
        cur_m = cur.days / (24 * 60) + cur.seconds / 60
        cur_s = cur.seconds - (cur.seconds / 60 * 60)

        cur_ms = cur_m * 60000 + cur_s * 1000
        if cur_ms > duration_ms:
            return ""

        minutes = duration_ms / 60000
        duration_ms = duration_ms - minutes * 60000
        seconds = duration_ms / 1000

        return "[%d:%02d/%d:%02d]" % (cur_m, cur_s, minutes, seconds)

    time_tag = ""
    try:
        last_track = Track(name=tracks[1].name, artist=tracks[1].artist).getInfo()
        start_time = tracks[1].played_on + timedelta(milliseconds=last_track.duration)

        now_track = Track(name=tracks[0].name, artist=tracks[0].artist).getInfo()
        time_tag = make_time_tag(start_time, now_track.duration)
    except LastfmError, e:
        print "now_playing_position: %s" % str(e)
        pass

    return time_tag


def now_playing_tags(track):
    try:
        track_tags = track.getTopTags()
        track_tags = filter_tags(track_tags)
    except Exception, e:
        log.exception("!wp track.getTopTags: %s" % e)
        log.info("track: %s" % track)
        track_tags = []
    try:
        artist_tags = track.artist.getTopTags()
        artist_tags = filter_tags(artist_tags)
    except Exception, e:
        log.exception("!wp track.artist.getTopTags(): %s" % e)
        log.info("track: %s  artist: %s" % (track, track.artist))
        artist_tags = []

    tags = track_tags or artist_tags
    if artist_tags:
        if track_tags:
            tags = [t for t in track_tags if t in artist_tags]
        tags = tags or artist_tags

    return tags



class Album(object):
    def __init__(self, name=None, artist=None, mbid=None, url=None, stats=None):
        self.name = name
        self.artist = artist
        self.mbid = mbid
        self.url = url
        self.stats = stats or Stats()

    def __repr__(self):
        return "<Album: %s>" % self.name


class Library(object):
    def __init__(self, user=None, artists=None, page=1, total_pages=0):
        self.user = user
        self._artists = artists
        self.page = page
        self.total_pages = total_pages


def library_getArtists(user, limit=50, page=None):
    params = {'method': 'library.getArtists',
              'user': user.name,
              'limit': limit,
              'page': page}
    data = fetch(api_url_base, params, None)

    return [Artist(artist_elem.find('name').text,
                   mbid=artist_elem.find('mbid').text,
                   url=artist_elem.find('url').text,
                   stats=Stats(rank=artist_elem.attrib['rank'],
                               playcount=artist_elem.find('playcount').text))
            for artist_elem in data.findall('artists/artist')]


def library_getAllArtists(user):
    params = {'method': 'library.getArtists', 'user': user.name, 'page': 1}
    artists = []
    data = fetch(api_url_base, params, None)
    have_pages = data.find('artists').attrib['total_pages'] + 1
    while have_pages:
        artists.append([Artist(artist_elem.find('name').text,
                               mbid=artist_elem.find('mbid').text,
                               url=artist_elem.find('url').text,
                               stats=Stats(rank=artist_elem.attrib['rank'],
                                           playcount=artist_elem.find('playcount').text))
                        for artist_elem in data.findall('artists/artist')])
        if have_pages:
            data = fetch(api_url_base, params, None)

    return artists


class User(object):
    def __init__(self, name=None, url=None, stats=None, join_date=None):
        self.name = name
        self.url = url
        self.stats = stats
        self.join_date = join_date
        self.library = Library(user=self)

    def __repr__(self):
        return "<User: %s>" % self.name

    def getInfo(self):
        params = {'method': 'user.getInfo', 'user': self.name}
        data = fetch(api_url_base, params, None)

        user_elem = data.find('user')
        return User(user_elem.find('name').text,
                    url=user_elem.find('url').text,
                    stats=Stats(playcount=user_elem.find('playcount').text),
                    join_date=datetime.fromtimestamp(float(user_elem.find('registered').attrib['unixtime'])))

    def getTopTags(self, start=None, end=None):
        return topTagsFromChart(self, start=start, end=end)

    def getTopArtists(self, period='overall'):
        params = {'method': 'user.getTopArtists', 'user': self.name, 'period': period}
        data = fetch(api_url_base, params, None)

        return [Artist(artist_elem.find('name').text,
                       mbid=artist_elem.find('mbid').text,
                       url=artist_elem.find('url').text,
                       stats=Stats(rank=artist_elem.attrib['rank'],
                                   playcount=artist_elem.find('playcount').text))
                for artist_elem in data.findall('topartists/artist')]

    def getRecentTracks(self, start=None, end=None, limit=None, page=None):
        params = {'method': 'user.getRecentTracks',
                  'user': self.name,
                  'from': start,
                  'to': end,
                  'limit': limit,
                  'page': page}
        data = fetch(api_url_base, params, None)
        tracks = data.findall('recenttracks/track')

        return [Track(name=track_elem.find('name').text,
                      artist=Artist(name = track_elem.find('artist').text),
                      played_on=datetime.fromtimestamp(
                            float(hasattr(track_elem.find('date'), 'attrib') and
                                  track_elem.find('date').attrib['uts'] or time())),
                      now_playing=hasattr(track_elem.attrib, 'get') and
                                      track_elem.attrib.get('nowplaying'))
                for track_elem in tracks]

    def getNeighbours(self, limit=None):
        params = {'method': 'user.getNeighbours', 'user': self.name, 'limit': limit}
        data = fetch(api_url_base, params, None)

        return [User(name=user_elem.find('name').text,
                     url=user_elem.find('url').text,
                     stats=Stats(match=user_elem.find('match').text))
                for user_elem in data.findall('neighbours/user')]


class Group(object):
    def __init__(self, name=None, url=None):
        self.name = name
        self.url = url

    def __repr__(self):
        return "<Group: %s>" % self.name

    def getTopTags(self, start=None, end=None):
        return topTagsFromChart(self, start=start, end=end)


class Tasteometer(object):
    def __init__(self, left=None, right=None, artists=None, stats=None):
        self.left = left
        self.right = right
        self.artists = artists
        self.stats = stats

    def score_name(self):
        n = self.stats.score * 100 or 0
        if n < 10:
            return "Very Low"
        if n < 30:
            return "Low"
        if n < 50:
            return "Medium"
        if n < 70:
            return "High"
        if n < 90:
            return "Very High"
        return "Super"

    def __repr__(self):
        return "<Tasteometer: %s, %s %f>" % (self.left, self.right, self.score)


class WeeklyChart(object):
    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end

    def __repr__(self):
        return "<WeeklyChart: from %s to %s>" % (self.start, self.end)


class WeeklyArtistChart(object):
    def __init__(self, start=None, end=None, artists=None):
        self.start = start
        self.end = end
        self.artists = artists

    def __repr__(self):
        return "<WeeklyArtistChart: from %s to %s>" % (self.start, self.end)


def chart_range(start, end, chart_list):
    cl_start = chart_list[0]
    cl_start_diff = abs(start - cl_start.start)
    cl_end = chart_list[-1]
    cl_end_diff = abs(end - cl_end.end)

    for chart in chart_list:
        diff = abs(start - chart.start)
        if(diff < cl_start_diff):
            cl_start = chart
            cl_start_diff = diff

        diff = abs(end - chart.end)
        if(diff < cl_end_diff):
            cl_end = chart
            cl_end_diff = diff
#    print "Start wanted %s, using %s"%(start, cl_start)
#    print "End wanted %s, using %s"%(end, cl_end)

    return (cl_start, cl_end)


def build_url(url, extra_params):
    (scheme, netloc, path, params, query, fragment) = urlparse.urlparse(url)
    path = path.replace(' ', '+')

    # Add any additional query parameters to the query
    if extra_params and len(extra_params) > 0:
        keys = extra_params.keys()
        keys.sort()
        extra_query = urllib.urlencode(
                        [(k, unicode(extra_params[k]).encode('utf-8'))
                         for k in keys if extra_params[k] is not None])
        # Add it to the existing query
        if query:
            query += '&' + extra_query
        else:
            query = extra_query

    return urlparse.urlunparse((scheme, netloc, path, params, query, fragment))


def check_xml(xml):
    data = ElementTree.XML(xml.encode('utf-8'))

    if data.get('status') != "ok":
        print data
        code = int(data.find('error').get('code'))
        message = data.findtext('error')
        raise LastfmError(code=code, message=message)
    return data


def fetch(url_base, key, args):
    args = args or dict()
    url = build_url(url_base, dict(key.items() + args.items()))
    print url

    for retries in range(2, -1, -1):
        try:
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            response = opener.open(url)
            url_data = response.read()
            url_data = url_data.decode('utf-8')

            break
        except urllib2.HTTPError, e:
            url_data = e.read()
        except socket.error, e:
            errno, errstr = sys.exc_info()[:2]
            print "socket error %s: %s" % (errno, errstr)
            if not retries:
                raise LastfmError(message="%s" % e)
            else:
                print "** retry **"
        except Exception, e:     # <urlopen error timed out>
            if not retries:
                raise LastfmError(message="%s" % e)
            else:
                print "** retry **"

    return check_xml(url_data)


def getWeeklyChartList(target):
    valid_types = {User: 'user', Group: 'group', Tag: 'tag'}
    our_type = valid_types.get(type(target))
    params = {'method': '%s.getWeeklyChartList' % our_type, our_type: target.name}
    data = fetch(api_url_base, params, None)

    return [WeeklyChart(start=datetime.fromtimestamp(float(chart_elem.get('from'))),
                        end=datetime.fromtimestamp(float(chart_elem.get('to'))))
            for chart_elem in data.findall('weeklychartlist/chart')]


def getWeeklyArtistChart(target, start=None, end=None):
    valid_types = {User: 'user', Group: 'group', Tag: 'tag'}
    our_type = valid_types.get(type(target))

    charts = getWeeklyChartList(target)
    (start_chart, end_chart) = chart_range(start, end, charts)

    valid_charts = [c for c in charts if c.start >= start_chart.start and c.end <= end_chart.end]
    print valid_charts

    artist_charts = []
    for c in valid_charts:
        params = {'method': '%s.getWeeklyArtistChart' % our_type,
                  our_type: target.name,
                  'from': int(mktime(c.start.timetuple())),
                  'to': int(mktime(c.end.timetuple()))}
        try:
            data = fetch(api_url_base, params, None)

            artist_charts.append(
                WeeklyArtistChart(
                    start=c.start,
                    end=c.end,
                    artists=[Artist(artist_elem.find('name').text,
                                    stats=Stats(playcount=artist_elem.find('playcount').text))
                             for artist_elem in data.findall('weeklyartistchart/artist')]))
        except LastfmError, e:
            log.info( "skipping from %s to %s: %s" % ( params['from'], params['to'], str(e) ) )


    return artist_charts


def compileArtists(charts, with_tags=False):
    artists = defaultdict()
    for chart in charts:
        for a in chart.artists:
            if a.name in artists:
                artists[a.name].stats.playcount += a.stats.playcount
            else:
                if with_tags:
                    a.tags = a.getTopTags()
                artists[a.name] = a
    return artists.values()


def topTagsFromChart(target, start=None, end=None):
    charts = getWeeklyArtistChart(target, start=start, end=end)

    max_tag_count = 4
    all_tags = defaultdict(lambda: 0)
    tag_weights = defaultdict(lambda: 0)
    total_playcount = 0
    artist_top_tags = {}
    for chart in charts:
        artist_count = 0
        for artist in chart.artists:
            artist_count += 1
            total_playcount += artist.stats.playcount
            tag_count = 0
            artist_top_tags[artist.name] = find_artist(artist.name).tags

            for tag in artist_top_tags[artist.name]:
                if tag_count >= max_tag_count:
                    break
                all_tags[tag.name] += 1
                tag_count += 1

            artist_pp = artist.stats.playcount / float(len(chart.artists))
            cumulative_pp = total_playcount / float(len(chart.artists))
            if (cumulative_pp > 0.75 or artist_pp < 0.01) and artist_count > 10:
                break

        for artist in chart.artists[:artist_count]:
            artist_pp = artist.stats.playcount / float(len(chart.artists))
            tf = 1 / float(max_tag_count)
            tag_count = 0
            weighted_tfidfs = {}
            if artist.name not in artist_top_tags.keys():
                artist_top_tags[artist.name] = find_artist(artist.name).tags
                print "shouldn be here! (%s)" % artist.name
            for tag in artist_top_tags[artist.name]:
                if tag_count >= max_tag_count:
                    break

                df = all_tags[tag.name] / float(artist_count)
                tfidf = tf / df
                weighted_tfidf = float(max_tag_count - tag_count) * tfidf
                weighted_tfidfs[tag.name] = weighted_tfidf
                tag_count += 1

            sum_weighted_tfidfs = sum(weighted_tfidfs.values())
            for tag in weighted_tfidfs:
                tag_weights[tag] += weighted_tfidfs[tag] / sum_weighted_tfidfs * artist_pp

    tag_weights_sum = sum(tag_weights.values())
    tag_weights = tag_weights.items()
    tag_weights.sort(key=lambda x: x[1], reverse=True)
    for i in xrange(len(tag_weights)):
        tag, weight = tag_weights[i]
        tag_weights[i] = (tag, weight, i + 1)

    return [Tag(name=tag,
                stats=Stats(rank=rank,
                            count=int(round(1000 * weight / tag_weights_sum))),
                 count=int(round(1000 * weight / tag_weights_sum)))
            for (tag, weight, rank) in tag_weights]


#last.fm api is broken for limit/matches
#limit defaults to 5, maximum 10.  matches will never be > 10
def taste_compare(left, right, limit=None):
    valid_types = {User: 'user', Artist: 'artists'}
    params = {'method': 'tasteometer.compare',
              'type1': valid_types.get(type(left)),
              'type2': valid_types.get(type(right)),
              'value1': left.name,
              'value2': right.name,
              'limit': limit}
    data = fetch(api_url_base, params, None)

    artists = data.find('comparison/result/artists')
    return Tasteometer(left=left, right=right,
                       artists=[find_artist(a.find('name').text)
                                for a in artists.findall('artist')],
                       stats=Stats(score=data.find('comparison/result/score').text,
                                   matches=artists.attrib['matches']))


class CachePerf(object):
    def __init__(self):
        self.start_time = time()
        self.valid = False

    def end(self):
        self.end_time = time()
        self.valid = True

    def results(self):
        if not self.valid:
            self.end()
        return defaultdict(int, {'time': self.end_time - self.start_time})


immigrant_song = ("We come from the land of the ice and snow",
                  "From the midnight sun where the hot springs blow",
                  "Hammer of the gods will drive our ships to new land",
                  "To fight the horde, sing and cry: Valhalla, I am coming!"
                  "On we sweep with threshing oar, Our only goal will be the western shore"
                  "Ah, ah")


def get_banned_tags():
    db = pymongo.Connection().anni.banned_tags
    return db.find()


def ban_tag(tag_name):
    db = pymongo.Connection().anni.banned_tags
    spec = {'tag': tag_name.lower()}
    doc = {'tag': tag_name.lower(), '$inc': {'count': 1},
           'banned_on': datetime.utcnow(), 'banned_by': 'tdb'}
    db.update(spec, doc, upsert=True, multi=False)


def unban_tag(tag_name):
    db = pymongo.Connection().anni.banned_tags
    spec = {'tag': tag_name.lower()}
    tag = db.find_one(spec, {"_id": 1})
    if tag:
        db.remove(tag)


# tag_list is list of Tag
def filter_tags(tag_list):
    banned = [t['tag'] for t in get_banned_tags()]
    return [t for t in tag_list if t.name.lower() not in banned and t.count > 5]


def find_from_nick(network, nick):
    acc = pymongo.Connection().anni.account
    #acc.ensure_index('nick')
    item = acc.find_one({'nick': nick.replace('.', '_').lower(), 'network': network})
    if item:
        print "find_from_nick: found %s for user %s on %s" % (item['account'], nick, network)
        return item
    print "find_from_nick: %s not found" % nick
    return None


def db_key_clean(nick):
    return nick.replace('.', '_').lower()

def hostmask_clean(mask):
    return reduce(lambda acc, x: acc.replace(x, '_'),
                  ['!','@','.','~'],
                  mask).lower().split('_',1)[1]

def find_account(irc, msg, user=None):
    account_coll = pymongo.Connection().anni.account

    def try_legacy(nick):
        acc = legacy_userdb.get(nick.lower(), None)
        if not acc:
            print "legacy not found user %s" % user
            return None
        print "legacy using %s for %s" % (acc, user)
        account_coll.update({'nick': nick.replace('.', '_').lower(), 'network': irc.network},
                            {'nick': [nick.replace('.', '_').lower(), ],
                             'network': irc.network,
                             'account': acc},
                            upsert=True, multi=False)
        print "update %s on %s with %s: %s" % (nick, irc.network, acc, pymongo.Connection().anni.error())
        return acc

    if user and user != msg.nick:
        try:
            print "find_account user: %s" % user
            #host = irc.state.nickToHostmask(user).replace('.', '_').lower().split('@', 1)[1]
            host = hostmask_clean(irc.state.nickToHostmask(user))
        except:
            #user isnt an irc user we've seen, may not be a irc user at all
            host = None
        caller_self = False
    else:
        try:
            host = hostmask_clean(irc.state.nickToHostmask(user))
        except:
            host = None

        #host = msg.host.replace('.', '_').lower()
        user = msg.nick
        caller_self = True

    #look up caller host
    ##host = msg.host.replace('.','_').lower()
    if host:
        item = account_coll.find_one({'host': host, 'network': irc.network})
        if item:
            if caller_self:
                nick = user.replace('.', '_').lower()
                if nick not in item['nick']:
                    other = find_from_nick(irc.network, nick)
                    if other:
                        print "%s @ %s conflicts with %s @ %s" % (nick, host, other['nick'], other.get('host'))
                        return User(item['account'])
                    print item['nick']
                    item['nick'].append(nick)
                    account_coll.update({'host': host, 'network': irc.network}, item, upsert=True, multi=False)
                    print "update %s with nick %s" % (host, nick)

            print "found %s for host %s on %s" % (item['account'], host, irc.network)
            return User(item['account'])
        print "host %s on %s not found" % (host, irc.network)

    #look up caller nick
    item = find_from_nick(irc.network, user)
    if item:
        if caller_self:
            item['host'] = host
            account_coll.update({'nick': user.replace('.', '_').lower(), 'network': irc.network},
                                item, upsert=True, multi=False)
            print "update %s with host %s" % (item, host)
        return User(item['account'])

    #try legacy user db
    acc = legacy_userdb.get(user.lower(), None)
    if not acc:
        print "legacy not found user %s" % user
        #if user == msg.nick:
        print "legacy fallback using %s" % user
        return User(user)
        return None
    print "legacy found %s for %s" % (acc, user)
    if caller_self:
        account_coll.update({'nick': user.replace('.', '_').lower(),
                             'network': irc.network},
                            {'nick': [user.replace('.', '_').lower()],
                             'network': irc.network, 'host': host, 'account': acc},
                            upsert=True, multi=False)
    return User(acc)


def doc_to_artist(doc):
    if doc.get('stats'):
        stats = Stats(listeners=doc.get('stats').get('listeners'),
                      playcount=doc.get('stats').get('playcount'))
    else:
        stats = None

    if doc.get('tags'):
        tags = [Tag(name=t['name'], count=t.get('count')) for t in doc.get('tags')]
    else:
        tags = []

    artist = Artist(doc['name'],
                    mbid=doc.get('mbid'),
                    url=doc.get('url'),
                    stats=stats,
                    tags=tags,
                    bio=doc.get('bio'))
    return artist


def artist_to_doc(artist):
    stats = {'listeners': artist.stats.listeners, 'playcount': artist.stats.playcount}
    if not artist.missing_tags:
        tags = [{'name': t.name, 'count': t.count} for t in artist.tags]
    else:
        tags = None

    return {'name': artist.name,
            'mbid': artist.mbid,
            'url': artist.url,
            'bio': artist.bio,
            'stats': stats,
            'tags': tags}


def find_artist(name, expired_ok=False):
    artist_coll = pymongo.Connection().anni.artist
    search = {'key': name.lower()}
    if not expired_ok:
        search['expiration_date'] = {"$gte": datetime.utcnow()}
    item = artist_coll.find_one(search)

    if item:
        return doc_to_artist(item)

    print "find_artist new %s" % name
    item = Artist(name).getInfo()
    artist_doc = artist_to_doc(item)
    artist_doc['key'] = name.lower()
    artist_doc['creation_date'] = datetime.utcnow()
    artist_doc['expiration_date'] = datetime.utcnow() + timedelta(days=4)
    print artist_doc['key']
    print "new artist, expires %s" % artist_doc['expiration_date']
    artist_coll.update({'key': artist_doc['key']}, artist_doc, upsert=True, multi=False)
    print pymongo.Connection().anni.error()

    return item


class Lastfm(callbacks.Plugin):
    """interface to last.fm.  See !list Lastfm for a list of commands."""
    threaded = True

    users = DictDB(os.path.join(conf.supybot.directories.data(), 'users.pklz'), mode=0600)

    reply = normalReply

    def error(self, irc, msg):
        out = "%s" % msg
        irc.reply(out.encode('utf-8'), notice=True, private=True)

    def __init__(self, irc):
        self.__parent = super(Lastfm, self)
        self.__parent.__init__(irc)
        self.log = supy_log.getPluginLogger('Lastfm')

        # say if ! in chars and is !! in strings,
        # use specialReply to reply !! with private notice
        for c in conf.supybot.reply.whenAddressedBy.chars():
            if c + c in conf.supybot.reply.whenAddressedBy.strings():
                self.reply = specialReply
                break

    def doNotice(self, irc, msg):
        msg.command = "PRIVMSG"
        self.__parent.__call__(irc, msg)

    def nick_to_user(self, first, second=None):
        #print "selecting from %s, %s"%(first, second)
        user = first or second
        #print "selected: %s"%user
        user = user.encode('utf-8')
        acc = self.users.get(user.lower(), user)
        #print "acc: %s"%acc
        return User(acc)

    viking_line = 0

    def trelleborg(self, irc, msg, args):
        self.reply(irc, msg.args, immigrant_song[self.viking_line % len(immigrant_song)])
        self.viking_line += 1

#*******************************************************************************
#*******************************************************************************
#*******************************************************************************

    def mongo(self, irc, msg, args, user):
        print find_account(irc, msg, user)
        #self.reply(irc, msg.args, out)
    mongo = wrap(mongo, [optional('lfm_user')])

    def artist(self, irc, msg, args, artist):
        """[artist]
        Info of artist or your currently playing artist
        """
        artist = artist.getInfo()
        try:
            out = "[%s.artist]: %d listeners. tagged: %s" % (
                    artist.name,
                    artist.stats.listeners,
                    ', '.join(["%s" % (t.name) for t in artist.tags[:4]]))
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    artist = wrap(artist, [or_now_playing('lfm_artist')])

    def albums(self, irc, msg, args, artist):
        """[artist]
        Returns albums for artist or your currently playing artist
        """
        try:
            albums = artist.getTopAlbums()
            out = "[%s.albums]: %s" % (
                    artist.name,
                    ', '.join(["%s [%d]" % (a.name, a.stats.playcount) for a in albums]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    albums = wrap(albums, [or_now_playing('lfm_artist')])

    def similar(self, irc, msg, args, artist):
        """[artist]
        Returns artists similar to artist or your currently playing artist
        """
        try:
            similar = artist.getSimilar(limit=8)
            out = "[%s.similar]: %s" % (
                    artist.name,
                    ', '.join(["%s (%d)" % (s.name, s.stats.match * 100) for s in similar]) or 'nothing')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    similar = wrap(similar, [or_now_playing('lfm_artist')])

    def recent(self, irc, msg, args, name):
        """[user]
        Returns recent tracks for [user]
        """
        def diff_hrs(dt1, dt2):
            diff = dt1 - dt2
            return (diff.days * 24) + (diff.seconds / 3600.0)

        try:
            account = find_account(irc, msg, name)
            tracks = account.getRecentTracks()
            now = datetime.now()
            out = "[%s.recent]: %s" % (account.name, ', '.join(["%s - %s (%d hrs)" %  \
                           (t.artist.name, t.name, -diff_hrs(now, t.played_on)) for t in tracks]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    recent = wrap(recent, [optional('something')])

    def unbantag(self, irc, msg, args, tag):
        """<tag>
        unban <tag>
        """
        try:
            if ircdb.checkCapability(msg.prefix, 'owner'):
                unban_tag(tag)
                self.reply(irc, msg.args, "'%s' removed from banned tags" % (tag))
            else:
                self.reply(irc, msg.args, "No permissions for you peon!")
        except Exception, e:
            self.reply(irc, msg.args, str(e))
    unbantag = wrap(unbantag, ['text'])

    def bantag(self, irc, msg, args, tag):
        """<tag>
        ban <tag> from appearing it tag lists
        """
        try:
            if ircdb.checkCapability(msg.prefix, 'owner'):
                ban_tag(tag)
                self.reply(irc, msg.args, "'%s' added to banned tags" % (tag))
            else:
                self.reply(irc, msg.args, "No permissions for you peon!")
        except Exception, e:
            self.reply(irc, msg.args, str(e))
    bantag = wrap(bantag, ['text'])

    def bannedtags(self, irc, msg, args):
        """
        get all banned tags
        """
        tags = get_banned_tags()
        out = "[banned.tags]: %s" % (', '.join(["%s" % t['tag'] for t in tags]))
        self.reply(irc, msg.args, out)
    bannedtags = wrap(bannedtags)

    def tagrank(self, irc, msg, args, tag):
        """<tag>
        popularity of tag
        """
        try:
            tag = Tag.search("\'%s\'" % tag, limit=1)[0]
            top_tag = Tag.getTopTags()[0]
        except LastfmError, e:
            self.reply(irc, msg.args, error_msg(msg, e))
            return
        except IndexError:
            self.reply(irc, msg.args, "no tags found")
            return

        rank = float(tag.count) / float(top_tag.count) * 100.0

        self.reply(irc, msg.args,
                "[%s.tag_rank]: %.2f  (%d / %d)" % (tag.name, rank, tag.count, top_tag.count))
    tagrank = wrap(tagrank, ['text'])

    def tag(self, irc, msg, args, tag):
        """<tag>
        Returns description of tag
        """
        try:
            tag = Tag.search("\"%s\"" % tag, limit=5)[0]
        except LastfmError, e:
            self.reply(irc, msg.args, error_msg(msg, e))
            return
        except IndexError:
            self.reply(irc, msg.args, "no tags found")
            return

        try:
            wiki = tag.getInfo()
            out = "[tag.wiki]: %s" % (wiki['content'])
        except:
            out = "no data"
        self.reply(irc, msg.args, out)
    tag = wrap(tag, ['text'])

    def tags(self, irc, msg, args, artist):
        """[artist]
        Returns top tags for artist or your currently playing artist
        """
        perf = CachePerf()
        try:
            tags = ', '.join(['%s (%s)' % (t.name, t.count) for t in filter_tags(artist.tags)])
            out = '[%s.tags]: %s' % (artist.name, tags)
        except LastfmError, e:
            out = error_msg(msg, e)
        with mores(250):
            self.reply(irc, msg.args, out)
        out = "tags: took %.6fs"
        log.info(out % (perf.results()['time']))
    tags = wrap(tags, [or_now_playing('lfm_artist')])

    def tagged(self, irc, msg, args, tag):
        """<tag>
        Returns top artists for <tag>
        """
        try:
            artists = tag.getTopArtists()
            out = u"[%s.artists]: %s" % (
                    tag.name,
                    ', '.join(["%s" % (a.name) for a in artists]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        with mores(250):
            self.reply(irc, msg.args, out)
    tagged = wrap(tagged, ['lfm_tag'])

    def multitagged(self, irc, msg, args, text):
        """<tag>[, <tag>...]
        Returns artists with tags
        """
        tags = [t for t in csv.reader([text], skipinitialspace=True)][0]
        print tags

        # Look up top artists and their similar artists
        def crawl(tag_list):
            def lookup(artist):
                return [find_artist(a.name, expired_ok=True) for a in artist]

            try:
                for t in tag_list:
                    a = lookup(Tag(name=t).getTopArtists())
                    [lookup(a2.getSimilar()) for a2 in a]
            except Exception, e:
                print "crawl: %s" % e
        #threading.Thread(target=crawl, args=(tags,)).start()

        coll = pymongo.Connection().anni.artist
        items = coll.find({'tags.name': {'$all': tags}})
        artists = [doc_to_artist(i) for i in items]

        def tag(ar, ta):
            return filter(lambda t: t.name == ta, ar.tags)[0]

        def gettag(artist, tag):
            return filter(lambda t: t.name == tag, artist.tags)[0] or None

        def do_sort(artists, tag):
            def keyfun(ar):
                return gettag(ar, tag).count or 0
            return sorted(artists, key=keyfun, reverse=True)

        for t in reversed(tags):
            artists = filter(lambda a: gettag(a, t).count > 10, artists)
            artists = do_sort(artists, t)

        for a in artists[:5]:
            print "%s: %s" % (a.name,
                              ', '.join(["%s (%s)" % (t.name, t.count)
                                         for t in [tag(a, at) for at in tags]]))

        out = "[artists]: %s" % ', '.join(["%s (%s)" %
            (a.name, ', '.join(["%s" % (t.name) for t in a.tags[:3]])) for a in artists[:5]])
        self.reply(irc, msg.args, out)
    mt = wrap(multitagged, ['text'])

#******************************** expensive start
#********************************
#********************************
    def gtags_thread(self, irc, msg, args, period, group):
        perf = CachePerf()

        try:
            tags = topTagsFromChart(group, period['start'], period['end'])
            tags = filter_tags(tags)
            out = "[%s.tags %s]: %s" % (
                    group.name,
                    period['lfm_period'],
                    ', '.join(["%s" % (t.name) for t in tags]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

        try:
            charts = getWeeklyChartList(group)
            (start_chart, end_chart) = chart_range(period['start'], period['end'], charts)
            out = "gtags: took %.2fs -- wanted %s to %s, using %s to %s"
            log.info(out % (perf.results()['time'],
                            period['start'].strftime("%m-%d-%y"),
                            period['end'].strftime("%m-%d-%y"),
                            start_chart.start.strftime("%m-%d-%y"),
                            end_chart.end.strftime("%m-%d-%y")))
        except:
            pass

    def gartists_thread(self, irc, msg, args, period, group):
        perf = CachePerf()

        try:
            charts = getWeeklyArtistChart(group, period['start'], period['end'])
            artists = compileArtists(charts)

            out = "[%s.artists %s]: %s" % (group.name, period['lfm_period'],\
                    ', '.join(["%s [%s]" % (a.name, a.stats.playcount)
                               for a in sorted(artists, key=lambda x: x.stats.playcount, reverse=True)]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

        try:
            charts = getWeeklyChartList(group)
            (start_chart, end_chart) = chart_range(period['start'], period['end'], charts)
            out = "gartists: took %.2fs -- wanted %s to %s, using %s to %s"
            log.info(out % (perf.results()['time'],
                            period['start'].strftime("%m-%d-%y"),
                            period['end'].strftime("%m-%d-%y"),
                            start_chart.start.strftime("%m-%d-%y"),
                            end_chart.end.strftime("%m-%d-%y")))
        except:
            pass

    def akin_thread(self, irc, msg, args, name):
        """[user]
        Returns users most comparable to you
        """
        caller = find_account(irc, msg, name)
        nicks = list(irc.state.channels[msg.args[0]].users)
        results = []

        for n in nicks:
            other = find_account(irc, msg, n)
            if not other or other.name == caller.name:
                continue

            try:
                results.append([taste_compare(caller, other, limit=10), n])
            except Exception:
                continue

        results = [r for r in sorted(results, key=lambda x: x[0].stats.score, reverse=True) if r[0].stats.score > .5]
        if len(results):
            out = "[%s.akin]: %s" % (name or msg.nick, ', '.join(  \
                        ["%s (%.2f%%)" % (r[1], r[0].stats.score * 100) for r in results]))
        else:
            out = "[%s.akin]: i weep for your loneliness" % (name or msg.nick)

        self.reply(irc, msg.args, out)

    def heard_artist_thread(self, irc, msg, args, artist):
        """<artist>
        Returns users who have heard artist
        """
        out = "[%s.heard]:" % artist.name

        nicks = list(irc.state.channels[msg.args[0]].users)
        have_heard = []
        for n in nicks:
            account = find_account(irc, msg, n)
            if not account:
                continue

            try:
                taste = taste_compare(account, artist)
                if taste.stats.score == 1:
                    try:
                        art_stat = artist.getInfo(username=account.name)
                        have_heard.append((n, art_stat.stats.userplaycount))
                    except LastfmError:
                        have_heard.append((n, 0))
            except LastfmError:
                pass

        if not len(have_heard):
            out = "%s none of that" % out
        else:
            out = "%s %s" % (out, ", ".join(["%s (%d)" % (x[0], x[1]) \
                                for x in sorted(have_heard, key=lambda x: x[1], reverse=True)]))
        self.reply(irc, msg.args, out)

    def utagged_thread(self, irc, msg, args, period, name, tag):
        perf = CachePerf()
        period = period or overall_period()
        account = find_account(irc, msg, name)

        try:
            artists = compileArtists(getWeeklyArtistChart(account, period['start'], period['end']))

            tagged_artists = []
            for a in artists:
                tags = a.getTopTags()
                for t in tags:
                    if t == tag:
                        w = a.stats.playcount * t.count
                        tagged_artists.append((a, tags, w))
                        break

            ta = sorted(tagged_artists, key=lambda x: x[2], reverse=True)

            out = "[%s.artists %s]:" % (account.name, period['lfm_period'])
            if len(ta):
                out = "%s %s" % (out, ', '.join(["%s (%d @ %d%%)" % (n[0].name, n[0].stats.playcount,
                                                                     n[2] / n[0].stats.playcount)
                                                  for n in ta[:5]]))
            else:
                out = "%s %s" % (out, "nothing")
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

        try:
            charts = getWeeklyChartList(account)
            (start_chart, end_chart) = chart_range(period['start'], period['end'], charts)
            out = "heardtag: took %.2fs -- wanted %s to %s, using %s to %s"
            log.info(out % (perf.results()['time'],
                            period['start'].strftime("%m-%d-%y"),
                            period['end'].strftime("%m-%d-%y"),
                            start_chart.start.strftime("%m-%d-%y"),
                            end_chart.end.strftime("%m-%d-%y")))
        except:
            pass

    def mytags_thread(self, irc, msg, args, period, name):
        perf = CachePerf()
        period = period or overall_period()
        account = find_account(irc, msg, name)

        try:
            tags = account.getTopTags(period['start'], period['end'])
            tags = filter_tags(tags)
            out = "[%s.tags %s]: %s" % (account.name,
                                            period['lfm_period'],
                                            ', '.join(["%s" % (t.name) for t in tags]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

        try:
            charts = getWeeklyChartList(account)
            (start_chart, end_chart) = chart_range(period['start'], period['end'], charts)
            out = "mytags: took %.2fs -- wanted %s to %s, using %s to %s"
            log.info(out % (perf.results()['time'],
                            period['start'].strftime("%m-%d-%y"),
                            period['end'].strftime("%m-%d-%y"),
                            start_chart.start.strftime("%m-%d-%y"),
                            end_chart.end.strftime("%m-%d-%y")))
        except:
            pass

    def myartists_thread(self, irc, msg, args, period, name):
        perf = CachePerf()
        period = period or overall_period()
        fast_periods = ('overall', '7day', '3month', '6month', '12month')
        account = find_account(irc, msg, name)

        try:
            if period['lfm_period'] in fast_periods:
                artists = account.getTopArtists(period['lfm_period'])
            else:
                charts = getWeeklyArtistChart(account, period['start'], period['end'])
                artists = compileArtists(charts)

            out = "[%s.artists %s]: %s" % (account.name, period['lfm_period'],\
                    ', '.join(["%s [%s]" % (a.name, a.stats.playcount)
                               for a in sorted(artists, key=lambda x: x.stats.playcount, reverse=True)]) or 'none')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

        try:
            out = "myartists: took %.2fs" % perf.results()['time']
            if period['lfm_period'] not in fast_periods:
                charts = getWeeklyChartList(account)
                (start_chart, end_chart) = chart_range(period['start'], period['end'], charts)
                out += " -- wanted %s to %s, using %s to %s" % \
                            (period['start'].strftime("%m-%d-%y"),
                             period['end'].strftime("%m-%d-%y"),
                             start_chart.start.strftime("%m-%d-%y"),
                             end_chart.end.strftime("%m-%d-%y"))
            log.info(out)
        except:
            pass

    def myrecs_thread(self, irc, msg, args, period, name):
        perf = CachePerf()
        period = period or overall_period()

        account = find_account(irc, msg, name)

        try:
            neighbours = account.getNeighbours(limit=10)
            charts = getWeeklyArtistChart(account, period['start'], period['end'])
            my_artists = compileArtists(charts)

            global_artists = defaultdict()
            for n in neighbours:
                charts = getWeeklyArtistChart(n, period['start'], period['end'])
                artists = compileArtists(charts)

                for a in artists:
                    if a.name in my_artists:
                        continue
                    if a.name in global_artists.values():
                        global_artists[a.name].stats.weight += a.stats.playcount / float(len(artists)) * n.stats.match
                        global_artists[a.name].stats.count += 1
                    else:
                        global_artists[a.name] = a
                        global_artists[a.name].stats.weight = a.stats.playcount / float(len(artists)) * n.stats.match
                        global_artists[a.name].stats.count = 1

            for k, v in global_artists.items():
                global_artists[k].stats.weight /= float(global_artists[k].stats.count)

            hdr = "[%s.recs]:" % account.name
            out = "%s %s" % (hdr,
                             ', '.join(["%s [%.2f]" % (a.name, a.stats.weight)
                                        for a in sorted(global_artists.values(),
                                                        key=lambda x: x.stats.weight,
                                                        reverse=True)]))
            print out
            out = "%s %s" % (hdr,
                             ', '.join(["%s" % (a.name)
                                        for a in sorted(global_artists.values(),
                                                        key=lambda x: x.stats.weight,
                                                        reverse=True)]))
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

        try:
            charts = getWeeklyChartList(account)
            (start_chart, end_chart) = chart_range(period['start'], period['end'], charts)
            out = "myrecs: took %.2fs -- wanted %s to %s, using %s to %s"
            log.info(out % (perf.results()['time'],
                            period['start'].strftime("%m-%d-%y"),
                            period['end'].strftime("%m-%d-%y"),
                            start_chart.start.strftime("%m-%d-%y"),
                            end_chart.end.strftime("%m-%d-%y")))
        except:
            pass

    def eclectic_thread(self, irc, msg, args, name, num_top=20, num_sim=5):
        account = find_account(irc, msg, name)
        try:
            top20 = account.getTopArtists()[:num_top]
            artists = {}

            for a in top20:
                sim = a.getSimilar(limit=num_sim)
                for s in sim:
                    if s.name in artists:
                        artists[s.name] += 1
                    else:
                        artists[s.name] = 1
            out = "[%s.eclectic]: %d/%d" % (account.name, len(artists), num_top * num_sim)
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)

    def eclectic(self, irc, msg, args, user):
        """<user>
        Stupid number that means nothing
        """
        threading.Thread(target=self.eclectic_thread, args=(irc, msg, args, user)).start()
    eclectic = wrap(eclectic, [optional('something', default="")])

    def super_eclectic(self, irc, msg, args, user):
        """<user>
        Stupid number that means nothing
        """
        threading.Thread(target=self.eclectic_thread, args=(irc, msg, args, user, 50, 20)).start()
    supereclectic = wrap(super_eclectic, [optional('something')])

    def akin(self, irc, msg, args, user):
        """[user]
        Returns users most comparable to you
        """
        threading.Thread(target=self.akin_thread, args=(irc, msg, args, user)).start()
    akin = wrap(akin, [optional('something')])

    def gtags(self, irc, msg, args, period, group):
        """[o|w [weeks]|3|6|12|m [months]|d <days>] <group>
        Returns tags for group over period
        """
        irc.reply("'%s' may take a while" % command_name(msg), private=True, notice=True)
        threading.Thread(target=self.gtags_thread, args=(irc, msg, args, period, group)).start()
    gtags = wrap(gtags, [optional('lfm_period'), 'lfm_group'])

    def gartists(self, irc, msg, args, period, group):
        """[o|w [weeks]|3|6|12|m [months]|d <days>] <group>
        Returns top artists for group over period
        """
        irc.reply("'%s' may take a while" % command_name(msg), private=True, notice=True)
        threading.Thread(target=self.gartists_thread, args=(irc, msg, args, period, group)).start()
    gartists = wrap(gartists, [optional('lfm_period'), 'lfm_group'])

    def mytags(self, irc, msg, args, period, user):
        """[o|w [weeks]|3|6|12|m [months]|d <days>] [user]
        Returns tags for user over period
        """
        irc.reply("'%s' may take a while" % command_name(msg), private=True, notice=True)
        threading.Thread(target=self.mytags_thread, args=(irc, msg, args, period, user)).start()
    mytags = wrap(mytags, [optional('lfm_period'), optional('something')])

    def myartists(self, irc, msg, args, period, user):
        """[o|w [weeks]|3|6|12|m [months]|d <days>] [user]
        Returns top artists for [user] over period
        """
        threading.Thread(target=self.myartists_thread, args=(irc, msg, args, period, user)).start()
    myartists = wrap(myartists, [optional('lfm_period'), optional('something')])

    def myrecs(self, irc, msg, args, period, user):
        """[user]
        Returns some recommendations for [user]
        """
        irc.reply("'%s' may take a while" % command_name(msg), private=True, notice=True)
        threading.Thread(target=self.myrecs_thread, args=(irc, msg, args, period, user)).start()
    myrecs = wrap(myrecs, [optional('lfm_period'), optional('something')])

    def heardtag(self, irc, msg, args, period, user, tag):
        """[o|w [weeks]|3|6|12|m [months]|d <days>] <user> <tag>
        Returns top artists user has heard with tag.
        """
        irc.reply("'%s' may take a while" % command_name(msg), private=True, notice=True)
        threading.Thread(target=self.utagged_thread, args=(irc, msg, args, period, user, tag)).start()
    heardtag = wrap(heardtag, [optional('lfm_period'), 'something', 'lfm_tag'])

    def heardartist(self, irc, msg, args, artist):
        """<artist>
        Returns users that have heard artist.
        """
        irc.reply("'%s' may take a while" % command_name(msg), private=True, notice=True)
        threading.Thread(target=self.heard_artist_thread, args=(irc, msg, args, artist)).start()
    heardartist = wrap(heardartist, [or_now_playing('lfm_artist')])

    def whatsplaying_thread(self, irc, msg, args):
        """Returns what is playing"""
        perf = CachePerf()
        channel_state = irc.state.channels[msg.args[0]]
        #for i in channel_state.users:
        #    log.info("!wp %s: %s -> %s" % (msg.args[0], i, irc.state.nickToHostmask(i)))
        nicks = list(channel_state.users)
        hits = 0
        for n in nicks:
            account = find_account(irc, msg, n)
            if not account:
                continue

            try:
                track = account.getRecentTracks(limit=1)

                timelimit = datetime.now() - timedelta( minutes=10 )
                now_playing = True
                if track[0].now_playing:
                    time_tag = now_playing_position(track)

                elif track[0].played_on > timelimit:
                    minutes_ago = int( (track[0].played_on - timelimit).seconds / 60 )
                    time_tag = "[~%s minute ago]" % minutes_ago
                else:
                    now_playing = False

                if now_playing:
                    tags = now_playing_tags(track[0])

                    tag_str = ""
                    if tags:
                        tag_str = "(%s)" % ', '.join([t.name for t in tags[:3]])
                    out = "[%s.playing]: %s - %s  %s %s" % (n, track[0].artist.name, \
                                track[0].name, tag_str, time_tag)
                    self.reply(irc, msg.args, out)
                    hits += 1
            except IndexError:      # Nothing playing
                continue
            except Exception:
                continue

        out = "whatsplaying %s: took %.2fs -- %s hits of %s users"
        log.info(out % (msg.args[0], perf.results()['time'], hits, len(nicks)))

    def whatsplaying(self, irc, msg, args):
        """Returns what is playing"""
        threading.Thread(target=self.whatsplaying_thread, args=(irc, msg, args)).start()
    whatsplaying = wrap(whatsplaying)
    wp = wrap(whatsplaying)

#******************************** expensive end

    def np2(self, irc, msg, args, users, channel):
        """[user, ...] or [channel]]
        Returns now playing for [users], or you in [channel]
        """
        use_nick = False
        users = users or list((msg.nick, ))

        if channel != msg.channel:
            new_users = list((msg.nick, ))
            if users != new_users:
                self.error(irc, "Ignoring supplied users (%s)" % ', '.join(users))
            users = new_users
            use_nick = True

        for user in users:
            try:
                account = find_account(irc, msg, user)
                track = account.getRecentTracks(limit=1)

                if use_nick:
                    out = "[%s.playing]:" % msg.nick
                else:
                    out = "[%s.playing]:" % account.name

                if track and track[0]:
                    print "played on: %s" % track[0].played_on

                    if track[0].now_playing:
                        time_tag = now_playing_position(track)
                    else:
                        last_play = datetime.now() - track[0].played_on

                        # seconds
                        if last_play.days == 0 and last_play.seconds < 60:
                            time_tag = "[%s seconds ago]" % last_play.seconds
                        # minutes
                        elif last_play.days == 0 and last_play.seconds < 3600:
                            time_tag = "[%s minutes ago]" % int(last_play.seconds / 60 )
                        # hours
                        elif last_play.days == 0:
                            time_tag = "[~%s hours ago]" % int(last_play.seconds / 3600 )

                        else:
                            time_tag = "[~%s days ago]" % last_play.days

                    tags = now_playing_tags(track[0])

                    tag_str = ""
                    if tags:
                        tag_str = "(%s)" % ', '.join([t.name for t in tags[:3]])
                    result = "%s - %s  %s %s" % (track[0].artist.name, track[0].name, tag_str, time_tag)
                else:
                    result = "nothing"
                out = "%s %s" % (out, result)
            except LastfmError, e:
                out = error_msg(msg, e)
            self.reply(irc, msg.args, out, to=channel)
    np = wrap(np2, [optional(commalist('nick')), optional('callerInGivenChannel')])

    def account(self, irc, msg, args, name):
        """<account>
        Associates Last.FM <account> with your host.
        """
        account_coll = pymongo.Connection().anni.account

        host = hostmask_clean(irc.state.nickToHostmask(msg.nick))
        #host = db_key_clean(msg.host)
        nick = db_key_clean(msg.nick)
        item = account_coll.find_one({'host': host, 'network': irc.network})
        if item:
            if not item['nick']:
                item['nick'] = [nick]
            elif nick not in item['nick']:
                item['nick'].append(nick)
        else:
            item = {'host': host, 'network': irc.network, 'nick': [nick]}
        item['account'] = name

        account_coll.update({'host': host, 'network': irc.network}, item, upsert=True, multi=False)
        self.error(irc, "%s@%s set account to %s" % (nick, host, name))
    account = wrap(account, ['something'])

    def user(self, irc, msg, args, name):
        """[user]
        get profile for user.
        """
        account_coll = pymongo.Connection().anni.account
        account = find_account(irc, msg, name)

        nick = name or msg.nick
        n, h = db_key_clean(nick), db_key_clean(msg.host)
        if name:
            nicks = account_coll.find({'network': irc.network, 'nick': n}, {'_id': 0})
        else:
            nicks = account_coll.find({'network': irc.network, 'host': h}, {'_id': 0}) or \
                    account_coll.find({'network': irc.network, 'nick': n}, {'_id': 0})

        nicks = []
        for k, v in self.users.items():
            if v.lower() == account.name:
                nicks.append(k)
        try:
            u = account.getInfo()
            out = "[%s.user]: %s plays, joined %s." % (u.name, u.stats.playcount, u.join_date)
            if len(nicks) > 0:
                out = "%s aka %s" % (out, ', '.join(nicks))
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    profile = wrap(user, [optional('something')])
    user = wrap(user, [optional('something')])

    def bio(self, irc, msg, args, artist):
        """[artist]
        Return biography for artist or your currently playing artist
        """
        try:
            out = "[%s.bio]: %s" % (artist.name, artist.bio)
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    bio = wrap(bio, [or_now_playing('lfm_artist')])

    def compare(self, irc, msg, args, user1, user2):
        """<user1> [user2]
        Compares user1 to user2 or requester
        """
        ignore_tags = ["electronic", "experimental", "electronica", "seen live"]
        if user2:
            left, right = find_account(irc, msg, user1), find_account(irc, msg, user2)
        else:
            left, right = find_account(irc, msg), find_account(irc, msg, user1)

        try:
            taste = taste_compare(left, right, limit=10)
            tags = defaultdict(lambda: 0)
            for a in taste.artists:
                for t in filter_tags(a.tags)[:5]:
                    if t.name not in ignore_tags:
                        tags[t.name] += 1
            tags = sorted(tags.iteritems(), key=operator.itemgetter(1), reverse=True)

            out = "[%s.compare.%s]: %s (%.2f%%) %s feat. %s" % \
                    (left.name, right.name, \
                     taste.score_name(), taste.stats.score * 100, \
                     ', '.join(['%s' % t[0] for t in tags[:3] if tags]), \
                     ', '.join(["%s" % a.name for a in taste.artists]) or 'nothing')
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    compare = wrap(compare, ['something', optional('something')])

    def heard(self, irc, msg, args, account, artist):
        """<user> [artist]
        Has user listened to artist or your currently playing artist
        """
        try:
            taste = taste_compare(account, artist)
            if len(taste.artists) == 0:
                found = "none of %s" % artist.name
            elif len(taste.artists) > 1:
                found = ', '.join([t.name for t in taste.artists])
            else:
                found = taste.artists[0].name
            out = "[%s.heard]: %s" % (account.name, found)
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    heard = wrap(heard, ['lfm_user', or_now_playing('lfm_artist')])

    def neighbours(self, irc, msg, args, user):
        """[user]
        Returns neighbours of [user]
        """
        account = find_account(irc, msg, user)
        try:
            n = account.getNeighbours()
            neighbours = ', '.join(["%s [%.2f]" % (u.name, u.stats.match * 100)
                                    for u in n]) or 'none'
            out = "[%s.neighbours]: %s" % (account.name, neighbours)
        except LastfmError, e:
            out = error_msg(msg, e)
        self.reply(irc, msg.args, out)
    neighbours = wrap(neighbours, [optional('something')])

Class = Lastfm


# vim:set shiftwidth=4 softtabstop=4 expandtab textwidth=79:
