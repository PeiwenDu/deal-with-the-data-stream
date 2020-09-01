import tweepy, json, random

class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        self.streamlength = 0
        self.sampletweets =[]

    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

        # returning non-False reconnects the stream, with backoff.
    def on_data(self, raw_data):
        tweet = json.loads(raw_data)
        # print(raw_data)
        # print(tweet["entities"]["hashtags"])
        if tweet["entities"]["hashtags"] !=[]:
            self.streamlength +=1
            hastags = tweet["entities"]["hashtags"]
            if len(self.sampletweets) < 100:
                self.sampletweets.append(hastags)
            else:
                if self.streamlength == 100:
                    self.sampletweets.append(hastags)
                else:
                    i = random.randint(0,self.streamlength-1)
                    if i in range(100):
                        self.sampletweets.append((hastags))
                        j = random.randint(0,99)
                        self.sampletweets.pop(j)
            print("The number of tweets with tags from the begining: "+str(self.streamlength))
            tagswithnum = dict()
            for hashtags in self.sampletweets:
                for hashtag in hashtags:
                    if hashtag["text"] not in tagswithnum.keys():
                        tagswithnum[hashtag["text"]] = len(hashtag["indices"])
                    else:
                        tagswithnum[hashtag["text"]] += len(hashtag["indices"])
            nums =[]
            for tag in tagswithnum:
                if tagswithnum[tag] not in nums:
                    nums.append(tagswithnum[tag])
            nums.sort(reverse = True)
            maxnumwithtag = dict()
            for num in nums[:3]:
                for tag in tagswithnum:
                    if tagswithnum[tag] == num:
                        if num not in maxnumwithtag.keys():
                            maxnumwithtag[num] = [tag]
                        else:
                            maxnumwithtag[num].append(tag)
            for num in maxnumwithtag:
                maxnumwithtag[num].sort()
            for num in maxnumwithtag:
                for tag in maxnumwithtag[num]:
                    print(tag+" : "+ str(num))
            print("\n")




consumer_token = "IVYeWbbnmIBHjHq8Z3Bpky8mF"
consumer_secret ="FLRFPWnWlyVutPJyBYwxaRLumBIH17Mn3JQR84k4Wj0O9U0DnZ"
key = "1120198773639008256-QsODjduq49VAJirIy3JLWjobW0eWcH"
secret = "TwEZ2jsUSvCmpoiUSsaIII8UxkNn6wT1ZEsDJHe2lwWJN"
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(key, secret)
api = tweepy.API(auth)
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.filter(track=['hashtags'])
# myStream.sample()

