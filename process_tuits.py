import json, gzip, glob, csv
save_path = "./clean_tweets/"
for filename in glob.iglob('./parlamentarios_timeline/*.txt.gz'):
    with gzip.open(filename, 'rb') as f:
        clean_tuits = []
        file_content = f.read()
        file_content = file_content.split('\n')
        print(len(file_content))
        if len(file_content) > 1:
            del file_content[-1]
            username = json.loads(file_content[-1])['user']['screen_name']
            for tuit in file_content:
                tuit = json.loads(tuit)
                tuit_clean = {}
                tuit_clean['created_at'] = tuit['created_at']
                tuit_clean['text'] = tuit['text']
                if 'retweeted_status' in tuit:
                    tuit_clean['is_rt'] = True
                else:
                    tuit_clean['is_rt'] = False
                clean_tuits.append(tuit_clean)
            with open(save_path + username + '.csv','w') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',')
                for tuit in clean_tuits:
                    spamwriter.writerow([tuit['text'].encode('utf-8'), tuit['created_at'], tuit['is_rt']])
