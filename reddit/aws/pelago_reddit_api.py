import json
import praw
import boto3

s3 = boto3.resource('s3')
bucket = 'aws-glue-kgalife-test'

### get config file
obj = s3.Object(bucket, 'pelago/config.json')
config = json.load(obj.get()['Body'])

client_id = config['client_id']
client_secret = config['client_secret']
username = config['username']
password = config['password']
user_agent = config['user_agent']

### Reddit Calller
reddit = praw.Reddit(
    client_id = client_id,
    client_secret = client_secret,
    user_agent = user_agent,
    username = username,
    password = password
    )

subreddit = reddit.subreddit("stardewvalley")

### output each submission as JSON file in S3
for x in subreddit.hot(limit=100):
    data = {
            "id" : x.id,
            "created": x.created,
            "url" : x.url,
            "selftext" : x.selftext,
            "upvote_ration" : x.upvote_ratio,
            "author" : str(x.author),
            "author_premium" : x.author_premium,
            "over_18" : x.over_18,
            "treatment_tags" : x.treatment_tags
        }
    s3.Object(bucket, f'pelago/output/{x.id}_{x.created}.json').put(Body=json.dumps(data))