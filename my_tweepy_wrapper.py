import tweepy, json, time, datetime, gzip, sys

tweets = []
initial_time = time.time()

def get_list_members_ids(api, list_owner, list_name):
    members = []
    for page in tweepy.Cursor(api.list_members, list_owner, list_name).items():
        members.append(page)
    print str(len(members))
    return [ m.id_str for m in members ]

def streaming_timeline_users(auth, list_ids):
    listener = StdOutListener()
    stream = tweepy.Stream(auth, listener)
    stream.filter(follow=list_ids)

def get_last_2000_tweets(api, list_ids):
    loops = 15
    WAIT_MINS = 5
    for id_str in list_ids:
        print 'Checking the timeline of %s' % (id_str)
        new_tweets = []
        i = 0
        repeat = True
        while repeat:
            try:
                for i in range(loops):
                    if len(new_tweets) > 0:
                        new_tweets.extend(api.user_timeline(user_id=id_str, max_id=new_tweets[-1].id_str, count=200,tweet_mode='extended'))
                    else:
                        new_tweets.extend(api.user_timeline(user_id=id_str, count=200,tweet_mode='extended'))
                repeat = False
            except tweepy.error.TweepError as e:
                repeat = True
                print '(%s) Time limit exceeded. Waiting %s mins' % (time.ctime(), WAIT_MINS)
                print '\t', e
                sys.stdout.flush()
                try:
                    if e.args[0][0]['code'] == 88:
                        print i
                        i -= 1
                        time.sleep(WAIT_MINS * 60)
                    else:
                        repeat = False
                except:
                    repeat = False
        print '%s tweets have been recovered from %s timeline' % (len(new_tweets), id_str)
        print len(new_tweets)
        file_name = './parlamentarios_usa/tweets-%s.txt.gz' % (id_str)
        print 'Writing file:', file_name
        with gzip.open(file_name, 'w') as f:
            for tweet in new_tweets:
                f.write(json.dumps(tweet._json) + '\n')
        print 'Writing finished'

class StdOutListener(tweepy.StreamListener):

    def on_data(self, raw_data):
        global tweets, initial_time
        elapsed_time = time.time () - initial_time #elapsed secons
        #save the status every 30 mins
        if elapsed_time >= 60 * 30:
            now = datetime.datetime.now()
            file_name = './parlamentarios_usa/tweets-%s-%s-%s-%s-%s.txt.gz' % (now.month, now.day, now.hour, now.minute, now.second)
            print 'Writing file:', file_name
            with gzip.open(file_name, 'w') as f:
                for tweet in tweets:
                    f.write(json.dumps(tweet) + '\n')
            print 'Writing finished'
            tweets = []
            initial_time = time.time()

        try:
            data = json.loads(raw_data)
            tweets.append(data)
        except:
            now = datetime.datetime.now()
            print '(%s %s:%s)Invalid json data: %s' % (now.day, now.hour, now.minute, raw_data)

        return True

    def on_error(self, status_code):
        now = datetime.datetime.now()
        print '(%s %s:%s)Got an error with status code: %s' % (now.day, now.hour, now.minute, status_code)
        #sleep 5 mins if an error occurs
        time.sleep(5 * 60)
        return True # To continue listening

    def on_timeout(self):
        print 'Timeout...'
        return True # To continue listening


if __name__ == '__main__':
    print 'Starting...'
    CONFIG_FILEPATH = './conf/'
    config_twitter = json.load(open(CONFIG_FILEPATH + 'conf.json', 'r'))
    CONSUMER_KEY = config_twitter['CONSUMER_KEY']
    CONSUMER_SECRET = config_twitter['CONSUMER_SECRET']
    USER_TOKEN = config_twitter['USER_TOKEN']
    USER_SECRET = config_twitter['USER_SECRET']
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(USER_TOKEN, USER_SECRET)
    api = tweepy.API(auth)
    twitter_ids = get_list_members_ids(api, "cspan", "members-of-congress")
    #twitter_ids = get_list_members_ids(api, "twittergov", "uk-mps")
    #twitter_ids = get_list_members_ids(api, 'Congreso_Es', 'diputados-xii-legislatura')
    twitter_ids = ['138203134']
    get_last_2000_tweets(api, twitter_ids )
    #streaming_timeline_users(auth, twitter_ids)
