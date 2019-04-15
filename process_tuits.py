import json, gzip, glob, csv
#save_path = "./parlamentarios_timeline/parlamentarios_csv/"
save_path = "./parlamentarios_uk/untouched_csvs/"
available_tuits = 0
clean_tuits = {}
#for filename in glob.iglob('./parlamentarios_timeline/*.txt.gz'):
for filename in glob.iglob('./parlamentarios_uk/*.txt.gz'):    
    with gzip.open(filename, 'rb') as f:
        file_content = f.read()
        file_content = file_content.split('\n')
        print(len(file_content))
        if len(file_content) > 1:
            del file_content[-1]
            username = json.loads(file_content[-1])['user']['screen_name']
            clean_tuits[username] = []
            for tuit in file_content:
                tuit = json.loads(tuit)
                tuit_clean = {}
                if not 'created_at' in tuit and not 'full_text' in tuit:
                    print tuit
                else:
                    tuit_clean['created_at'] = tuit['created_at']
                    #print tuit['text']
                    #print  tuit['text']
                    #print tuit
                    tuit_clean['text'] = tuit['full_text'].replace("&amp;", "").replace(",", " ").replace('\n', ' ')
                    #print tuit_clean['text'] 
                    #print(tuit_clean['text'])
                    #break
                    tuit_clean['id'] = tuit['id']
                    tuit_clean['user'] = tuit['user']['screen_name']
                    if str(tuit_clean['id']) == "826227011546972160":
                        print tuit_clean
                        print tuit['full_text']
                        print tuit
                    if 'retweeted_status' in tuit:
                        tuit_clean['is_rt'] = True
                    else:
                        tuit_clean['is_rt'] = False
                    if not tuit_clean['is_rt']:
                        available_tuits = available_tuits + 1
                        clean_tuits[username].append(tuit_clean)
for username in clean_tuits:
    with open(save_path + username + '.csv','w') as csvfile:
        spamwriter = csv.writer(csvfile)
        for tuit in clean_tuits[username]:
            spamwriter.writerow([tuit['text'].encode('utf-8'), tuit['created_at'], tuit['user'], tuit['id'], tuit['is_rt'], 'https://twitter.com/' + tuit['user'] + '/status/' + str(tuit['id'])])

