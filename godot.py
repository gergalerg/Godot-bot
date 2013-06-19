import twitter
import redis
from verify import *

api = verify(API_CREDENTIALS)
r = redis.Redis()

def make_text():
    file = []
    with open('godot.txt', 'rb') as f:
        file = f.read().splitlines()

    newfile = [word for word in file if not word.isupper()] 
    final = [line[:140] for line in newfile if line]

def make_redis_db_for_godot():
    for i in final:
            r.lpush('godot', i)

# populate responded to in redis
def post_update():
    # Post to twitter in response to search
    # This is an example for one. I have to automate.
    x = api.GetSearch('godot')
    #for i in x:
    #    r.sadd('users', i.user.screen_name)
    for i in x:
        if r.sismember('users', i.user.screen_name):
            print "already responded to: " + i.user.screen_name
        else:
            api.PostUpdate("@" + i.user.screen_name + " " + r.rpop('godot')) 
            r.sadd('users', i.user.screen_name)
            print "Posted to " + i.user.screen_name

if __name__ == "__main__":
    post_update()
