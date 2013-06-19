import time
import pycurl
import urllib
import json
import oauth2 as oauth
from verify import *
import redis

API_ENDPOINT_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
USER_AGENT = 'TwitterStream 1.0'

api = verify(API_CREDENTIALS)
r = redis.Redis()

POST_PARAMS = {'include_entities': 0,
                'stall_warning': 'true',
                'track': 'godot'}


class TwitterStream:
    def __init__(self):
        self.oauth_token = oauth.Token(key=API_CREDENTIALS['access_token_key'], secret = API_CREDENTIALS['access_token_secret'])
        self.oauth_consumer = oauth.Consumer(key=API_CREDENTIALS['consumer_key'], secret = API_CREDENTIALS['consumer_secret'])
        self.conn = None
        self.buffer = ''
        self.setup_connection()

    def setup_connection(self):
        """Create persistent http connection
        """
        if self.conn:
            self.conn.close()
            self.buffer = ''
        self.conn = pycurl.Curl()
        self.conn.setopt(pycurl.URL, API_ENDPOINT_URL)
        self.conn.setopt(pycurl.USERAGENT, USER_AGENT)
        self.conn.setopt(pycurl.ENCODING, 'deflate, gzip')
        self.conn.setopt(pycurl.POST, 1)
        self.conn.setopt(pycurl.POSTFIELDS, urllib.urlencode(POST_PARAMS))
        self.conn.setopt(pycurl.HTTPHEADER, ['Host: stream.twitter.com',
                                            'Authorization: {}'.format(self.get_oauth_header())])
        # self.handle_tweet is the method that is called
        self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)

    def get_oauth_header(self):
        """ Create and return oauth get_oauth_header
        """
        params = {'oauth_version': '1.0',
                    'oauth_nonce': oauth.generate_nonce(),
                    'oauth_timestamp': int(time.time())}
        req = oauth.Request(method="POST", parameters=params, url='{}?{}'.format(API_ENDPOINT_URL, urllib.urlencode(POST_PARAMS)))
        req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), self.oauth_consumer, self.oauth_token)
        return req.to_header()['Authorization'].encode('utf-8')

    def start(self):
        backoff_network_error = 0.25
        backoff_http_error = 5
        backoff_rate_limit = 60
        while True:
            self.setup_connection()
            try:
                self.conn.perform()
            except:
                # Network error
                print 'Network error: {}'.format(self.conn.errstr())
                print 'Waiting {} seconds before trying again'.format(backoff_network_error)
                time.sleep(backoff_network_error)
                backoff_network_error = min(backoff_network_error + 1, 16)
                continue
            sc = self.conn.getinfo(pycurl.HTTP_CODE)
            if sc == 420:
                # Rate limit use exponential backoff
                print 'Rate limit, waiting {} seconds'.format(backoff_rate_limit)
                time.sleep(backoff_rate_limit)
                backoff_rate_limit *= 2
            else:
                #HTTP error use exponential backoff
                print "HTTP error {}, {}".format(sc, self.conn.errstr())
                print "Waiting {} seconds".format(backoff_http_error)
                time.sleep(backoff_http_error)
                backoff_http_error = min(backoff_http_error * 2, 320)

    def post_update(self, user):
        api.PostUpdate("@" + user + " " + r.rpop('godot')[:(138 - len(user))])
        print "Posted to " + user

    def handle_tweet(self, data):
        try:
            self.buffer += data
            if data.endswith('\r\n') and self.buffer.strip():
                message = json.loads(self.buffer)
                self.buffer = ''
                msg = ''
                if message.get('limit'):
                    print "Rate limiting caused us to miss {} tweets".format(message['limit'].get('track'))
                elif message.get('disconnect'):
                    raise Exception('Got disconnect: {}'.format(message['disconnect'].get('reason')))
                elif message.get('warning'):
                    print 'Got warning {}'.format(message['warning'].get('message'))
                else:
                    print "****** {} -- {}".format(message.get('user')['screen_name'] , message.get('text'))
                    twitter_user_screen_name = message.get('user')['screen_name']
                    self.post_update(twitter_user_screen_name)

        except (UnicodeEncodeError):
            print "Unicode Error"
