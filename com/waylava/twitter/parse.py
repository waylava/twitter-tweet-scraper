import string
import bs4

printable = set(string.printable)


def has_class(class_name):
    return lambda class_: class_ and class_name in class_.split()


only_tweet_tags = bs4.SoupStrainer('div', class_=has_class('tweet'), **{'data-tweet-id': True})


class Parser(object):
    @staticmethod
    def parse_tweet_tag(tag, expand_emojis=True):
        tweet_id = tag['data-tweet-id']
        permalink = tag['data-permalink-path']
        screen_name = tag['data-screen-name']
        name = tag['data-name']
        user_id = tag['data-user-id']

        content_div = tag.find('div', class_=has_class('content'))
        tweet_body_tag = content_div.find('p', class_=has_class('tweet-text'))
        geo_location_span = content_div.find('span', class_=has_class('Tweet-geo'))
        if geo_location_span:
            location = geo_location_span['title']
        else:
            location = None

        if tweet_body_tag is None:
            # Might be a censored tweet, skip
            return

        if expand_emojis:
            for emoji_tag in tweet_body_tag.find_all(class_='Emoji'):
                emoji_tag.insert(0, emoji_tag['alt'])

        lang = tweet_body_tag['lang']
        tweet_text = tweet_body_tag.text

        urls = [
            a['data-expanded-url']
            for a in tweet_body_tag.find_all('a', class_=has_class('twitter-timeline-link'))
            if 'data-expanded-url' in a.attrs
            ]

        mentions = [
            a.text
            for a in tweet_body_tag.find_all('a', class_=has_class('twitter-atreply'))
            ]

        timestamp = int(content_div.find(**{'data-time-ms': True})['data-time-ms']) / 1000.

        footer_div = content_div.find('div', class_=has_class('stream-item-footer'))

        def get_stats(stats_type):
            span = footer_div.find('span', class_=has_class("ProfileTweet-action--%s" % stats_type))
            spanspan = span.find('span', class_=has_class("ProfileTweet-actionCount"))
            return int(spanspan['data-tweet-stat-count'])

        footer_div.find('span', class_="ProfileTweet-action--retweet")
        retweet_count = get_stats('retweet')
        favorite_count = get_stats('favorite')
        tweet_text = filter(lambda x: x in printable, tweet_text)
        tweet = dict(tweet_id=tweet_id,
                     permalink=permalink,
                     screen_name=screen_name,
                     name=name,
                     user_id=user_id,
                     lang=lang,
                     tweet_text=tweet_text,
                     urls=urls,
                     mentions=mentions,
                     retweet_count=retweet_count,
                     favorite_count=favorite_count,
                     timestamp=timestamp, location=location)
        return tweet

    def parse_search_results(self, html):
        soup_tweets = bs4.BeautifulSoup(html, 'html.parser', parse_only=only_tweet_tags, from_encoding='utf-8')
        for tag in soup_tweets:
            tweet = self.parse_tweet_tag(tag)
            if tweet is not None:
                yield tweet
