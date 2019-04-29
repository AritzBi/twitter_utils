

def load_list_twitters_ids(filename):
    tweet_ids = []
    with open(filename, "r") as fd:
        for line in fd:
            tweet_ids.append(line.strip())
    return tweet_ids

if __name__ == '__main__':
    load_list_twitters_ids("./dataset_twitterids/democratic-candidate-timelines.txt")