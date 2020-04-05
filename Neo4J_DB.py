from neo4j import GraphDatabase
import Constants

uri = ""
user = ""
password = ""

class Neo4j_DB_Util(object):

    def __init__(self):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        with self._driver.session() as session:
            for city, country in Constants.Cities_Countries.items():
                log = session.write_transaction(self._create_city, city, Constants.Cities_Population[city])
                print(log)
                log = session.write_transaction(self._create_country, country, Constants.Countries_Corona_Cases[country])
                print(log)
                log = session.write_transaction(self._connect_city_country, city, country)
                print(log)
                #log = session.write_transaction(self._create_unknown_locationon, 'unknown', 0)
                #print(log)

    def close(self):
        self._driver.close()

    def Insert_User(self, user):
        with self._driver.session() as session:
            log = session.write_transaction(self._create_user, user)
            print(log)

    def Insert_Mentioned_User(self, user_id, screen_name):
        with self._driver.session() as session:
            log = session.write_transaction(self._create_mentioned_user, user_id, screen_name)
            print(log)

    def Insert_Tweet(self, tweet):
        with self._driver.session() as session:
            log = session.write_transaction(self._create_tweet, tweet)
            print(log)

    def Connect_User_Tweet(self):
        with self._driver.session() as session:
            log = session.write_transaction(self._connect_user_tweet)
            print(log)

    def Connect_Leader_Country(self, user_id, country):
        with self._driver.session() as session:
            log = session.write_transaction(self._connect_leader_country, user_id, country)
            print(log)

    def Insert_Hashtag(self, hashtags_list, user_id, tweet_id):
        with self._driver.session() as session:
            for hashtag in hashtags_list:
                log = session.write_transaction(self._create_hashtag, hashtag)
                print(log)
                # log = session.write_transaction(self._connect_hashtags_with_users, hashtag, user_id)
                # print(log)
                log = session.write_transaction(self._connect_hashtags_with_tweets, hashtag, tweet_id)
                print(log)


    def Connect_User_City(self, user_id, city):
        with self._driver.session() as session:
            log = session.write_transaction(self._connect_user_city, user_id, city)
            print(log)



    def Connect_Mentioned_User_Tweet(self, user_id, tweet_id):
        with self._driver.session() as session:
            log = session.write_transaction(self._connect_mentioned_user_tweet, user_id, tweet_id)
            print(log)

    @staticmethod
    def _create_user(tx, user):
        result = tx.run("Merge (a:User "
                        "{ user_id: $id, screen_name: $screen_name, followers: $followers,"
                        " favourites_count: $favourites_count, following_count: $following_count,"
                        " verified: $verified, location: $location }) "
                        "RETURN a.id+ ', from node ' + id(a)",
                        id=user["id"], screen_name=user["screen_name"], followers=user["followers"],
                        favourites_count=user["favourites_count"], following_count=user["following_count"],
                        verified=user["verified"], location=user["location"])
        return result.single()[0]

    @staticmethod
    def _create_mentioned_user(tx, user_id, screen_name):
        result = tx.run("Merge (a:Mentioned_User "
                        "{ user_id: $id, screen_name: $screen_name}) "
                        "RETURN a.id+ ', from node ' + id(a)",
                        id=user_id, screen_name=screen_name)
        return result.single()[0]

    @staticmethod
    def _create_tweet(tx, tweet):
        result = tx.run("Merge (t:Tweet "
                        "{ tweet_id: $tweet_id, tweeted_by: $tweeted_by, text: $text, creation_time: $creation_time, retweets: $retweets,"
                        " favourites: $favourites, sentiment: $sentiment}) "
                        "RETURN t.id +', from node ' + id(t)", tweet_id=tweet['id'], tweeted_by=tweet["tweeted_by"], text=tweet["text"],
                        creation_time=tweet["creation_time"], retweets=tweet["retweet_count"],
                        favourites=tweet["favorite_count"], sentiment=tweet["sentiment"])
        return result.single()[0]

    @staticmethod
    def _create_hashtag(tx, hashtag):
        result = tx.run("Merge (h:Hashtag "
                        "{ hashtag: $hashtag }) "
                        "RETURN h.id +', from node ' + id(h)", hashtag=hashtag)
        return result.single()[0]

    @staticmethod
    def _connect_user_tweet(tx):
        tx.run("MATCH (u:User) MATCH (t:Tweet {tweeted_by:u.user_id}) MERGE (u)-[ht:HAS_TWEETED]->(t)")
        return 'matched user with tweet'

    @staticmethod
    def _connect_hashtags_with_users(tx, hashtag, user_id):
        tx.run("MATCH (u:User { user_id: $user_id }),(h:Hashtag { hashtag: $hashtag }) MERGE (u)-[r:Used_Hashtag]->(h)",
               user_id=user_id, hashtag=hashtag)
        return 'user with user_id: ' + str(user_id) + ' Used_Hashtag ' + str(hashtag)

    @staticmethod
    def _connect_hashtags_with_tweets(tx, hashtag, tweet_id):
        tx.run("MATCH (t:Tweet { tweet_id: $tweet_id }),(h:Hashtag { hashtag: $hashtag }) MERGE (t)-[r:Includes_Hashtag]->(h)",
               tweet_id=tweet_id, hashtag=hashtag)
        return 'tweet id: ' + str(tweet_id) + ' Includes_Hashtag ' + str(hashtag)

    @staticmethod
    def _create_city(tx, city, population):
        result = tx.run("Merge (c:City "
                        "{ city: $city, population: $population }) "
                        "RETURN c.id +', from node ' + id(c)", city=city, population=population)
        return result.single()[0]

    @staticmethod
    def _create_country(tx, country, corona_cases):
        result = tx.run("Merge (c:Country "
                        "{ country: $country, corona_cases: $corona_cases }) "
                        "RETURN c.id +', from node ' + id(c)", country=country, corona_cases=corona_cases)
        return result.single()[0]

    @staticmethod
    def _connect_city_country(tx, city, country):
        tx.run("MATCH (city:City { city: $city }),(country:Country { country: $country }) MERGE (city)-[r:Located_In]->(country)",
               city=city, country=country)
        return 'city: ' + str(city) + ' is Located_In ' + str(country)

    @staticmethod
    def _connect_user_city(tx, user_id, city):
        tx.run("MATCH (user:User { user_id: $user_id }),(city:City { city: $city }) MERGE (user)-[r:Lives_In]->(city)",
               city=city, user_id=user_id)
        return 'User id: ' + str(user_id) + ' Lives_In ' + str(city)

    @staticmethod
    def _connect_leader_country(tx, user_id, country):
        tx.run("MATCH (user:User { user_id: $user_id }),(country:Country { country: $country }) MERGE (user)-[r:Leader_Of]->(country)",
               country=country, user_id=user_id)
        return 'User id: ' + str(user_id) + ' is Leader_Of ' + str(country)

    @staticmethod
    def _create_unknown_locationon(tx, city, population):
        result = tx.run("Merge (c:City "
                        "{ city: $city, population: $population }) "
                        "RETURN c.id +', from node ' + id(c)", city=city, population=population)
        return result.single()[0]

    @staticmethod
    def _connect_mentioned_user_tweet(tx, user_id, tweet_id):
        tx.run("MATCH (mentioned_user:Mentioned_User { user_id: $user_id }),(tweet:Tweet { tweet_id: $tweet_id }) MERGE (mentioned_user)-[r:Mentinoed_In]->(tweet)",
               user_id=user_id, tweet_id=tweet_id)
        return 'User id: ' + str(user_id) + ' Mentioned_In ' + str(tweet_id)

    #def _connect_user_tweet(tx, tweet_id, user_id):


    # MERGE(c1: City {name: line.from_city})
    # MERGE(p1: Person {name: line.from_name, number: line.from_number, gender: line.from_gender})
    # MERGE(p1) - [: FROM]->(c1)


# MATCH (charlie:Person { name: 'Charlie Sheen' }),(wallStreet:Movie { title: 'Wall Street' })
# MERGE (charlie)-[r:ACTED_IN]->(wallStreet)


