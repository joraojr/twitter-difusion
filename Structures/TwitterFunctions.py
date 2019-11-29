import tweepy
from Logging import Log
from Database import Neo4jConnection, TwitterBD

# ==Twitter development account======================================
class TwitterEntry:
    consumer_key_twitter = None
    consumer_secret_twitter = None
    access_token_twitter = None
    access_token_secret_twitter = None
    auth_twitter = None
    api_twitter = None
    key_word = None


# ===============================================================================================================
# ==CONNECTION TWITTER
# ===============================================================================================================

def Conect_Twitter(consumer_key, consumer_secret, access_token, access_token_secret):
    TwitterEntry.consumer_key_twitter = consumer_key
    TwitterEntry.consumer_secret_twitter = consumer_secret
    TwitterEntry.access_token_twitter = access_token
    TwitterEntry.access_token_secret_twitter = access_token_secret
    TwitterEntry.auth_twitter = tweepy.OAuthHandler(consumer_key,consumer_secret)
    TwitterEntry.auth_twitter.set_access_token(access_token,access_token_secret)
    TwitterEntry.api_twitter = tweepy.API(TwitterEntry.auth_twitter,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# ===============================================================================================================
# ==BASIC TWITTER FUNCTIONS
# ===============================================================================================================

def getUserInfo(id):
    if (TwitterEntry.api_twitter != None):
        try:
            return TwitterEntry.api_twitter.get_user(id=id)
        except:
            return None

def send_mensage(text): #from account user
    if(TwitterEntry.api_twitter != None):
        try:
            TwitterEntry.api_twitter.update_status(text)
        except:
            return None

def get_followers(followed):
    ids = []
    for page in tweepy.Cursor(TwitterEntry.api_twitter.followers_ids,screen_name=followed).pages():
        ids.extend(page)
    return ids

def follow_back(id):
    try:
        TwitterEntry.api_twitter.create_friendship(id=id)
    except:
        return None

def delete_Tweet(id_str):
    try:
        TwitterEntry.api_twitter.destroy_status(id_str)
    except:
        return None

def send_mensage_Selected_Users():
    selected_users = TwitterBD.getAll_Selected_Users()
    try:
        for selected_user in selected_users:
            if(TwitterBD.count_Selected_User_MENTIONS(selected_user['id_str']) == 0):
                send_mensage("Olá, @"+selected_user['screen_name']+". Somos um projeto que auxilia com divulgação de pessoas desaparecidas. Pela sua influência no Twitter, selecionamos você para participar do projeto. Podemos contar com sua sua ajuda na divulgação? Siga-nos de volta! Mais informações em nossa conta")
                follow_back(selected_user['screen_name'])
                tweets = TwitterEntry.api_twitter.user_timeline(count=1)#save tweet and mention
                tweet = TwitterBD.Tweet(tweets[0].id_str,tweets[0].text,tweets[0].favorite_count,'Text',tweets[0].retweet_count,str(tweets[0].created_at))
                Neo4jConnection.Save(tweet)
                mention = TwitterBD.MENTIONS(selected_user,tweet)
                Neo4jConnection.Save(mention)
    except Exception:
        Log.logging.exception("Erro ao mandar mensagem para os seguidores")

def follow_all_followers(followed):
    followers = get_followers(followed)
    for follower in followers:
        follow_back(str(follower))



# ===============================================================================================================
# == VERIFICATIONS
# ===============================================================================================================


def verify_follow():  # verify is user foolow us back  use after delet old Selected_Users
    try:
        selected_users = TwitterBD.getAll_Selected_Users()
        followers = get_followers("PjDesaparecidos")
        for selected_user in selected_users:
            if (selected_user['follower'] == "0"):
                for follower in followers:
                    if (str(follower) == selected_user['id_str']):  # if selected user follow us
                        TwitterBD.set_Selected_User(selected_user['id_str'], 'follower','1')  # change status of selected user
                        Log.logging.info("follow_back:"+ str(selected_user['screen_name']))
    except Exception:
        Log.logging.exception("Erro fazer a verificação dos seguidores")


def verify_retweets_Dissemination():
    recommendations = TwitterBD.getAll_Dissemination()
    for recommendation in recommendations:
        verify_retweets(recommendation)

def verify_retweets(recommendation):#follow recommendations and save on DB
    if (recommendation != None):
        try:
            retweets = TwitterEntry.api_twitter.retweets(recommendation['id_str'])
            for retweet in retweets:
                selected_user = TwitterBD.find_Selected_User(retweet.user.id_str)
                if (selected_user != None):#if selected user
                    print(retweet.id_str)
                    data = TwitterBD.find_Dissemination_RETWEETED_Selected_User(recommendation['id_str'],selected_user['id_str'])

                    count = len(data)
                    if(count == 0):
                        rel = TwitterBD.RETWEETED(selected_user, recommendation,retweet.id_str)
                        Neo4jConnection.Save(rel)
                        Log.logging.info("retweet_selected_user:"+str(selected_user))

                else:   #if common user
                    simple_user = TwitterBD.find_Simple_User(retweet.user.id_str)
                    if(simple_user == None):
                        simple_user = TwitterBD.Simple_User(retweet.user.id_str, retweet.user.screen_name) ## Tentar pegar os dados de localização pelo id
                        Neo4jConnection.Save(simple_user)
                        rel = TwitterBD.RETWEETED_SU(simple_user, recommendation,retweet.id_str) ##Mudar aqui
                        Neo4jConnection.Save(rel)

                    else:
                        data = TwitterBD.find_Dissemination_RETWEETED_Selected_User(recommendation['id_str'], simple_user['id_str'])
                        count = len(data)
                        if (count == 0):
                            rel = TwitterBD.RETWEETED_SU(selected_user, recommendation, retweet.id_str)
                            Neo4jConnection.Save(rel)

                    Log.logging.info("retweet_selected_user:" + str(selected_user))

        except Exception:
            Log.logging.exception("Erro fazer a verificação dos retweets")

# ===============================================================================================================
# == Missing persons dissemination
# ===============================================================================================================
def send_missingPerson_Selected_Users():
    selected_users = TwitterBD.getAll_Selected_Users()
    for selected_user in selected_users:
        send_missingPerson_Selected_User(selected_user['id_str'])

def send_missingPerson_Selected_User(id_str):
    try:
        selected_user = TwitterBD.find_Selected_User(id_str)
        missing_persons =  TwitterBD.getAll_Missing_Persons() ##Colocar para pegas as pessoas desaparecidas
        #if(selected_user['follower']=='true' and TwitterBD.count_User_RECOMMENDER(selected_user['id_str'])==0):# if selected user follow back
        if(TwitterBD.count_User_DISSEMINATE(selected_user['id_str'])==0):# if selected user follow back
            for person in missing_persons:
                if (person != None):
                    messenge = 'Olá @'+selected_user['screen_name']+". Testando o envio de msng . Id:" + person["name"]
                    Log.logging.info("Difusion:{User:"+selected_user['id_str']+","+messenge+"}")
                    send_mensage(messenge)
                    tweets = TwitterEntry.api_twitter.user_timeline(count=1)  # save tweet and mention
                    diffusion = TwitterBD.find_Dissemination(tweets[0].id_str)
                    if(diffusion == None):
                        diffusion = TwitterBD.Dissemination(tweets[0].id_str, tweets[0].text, str(tweets[0].created_at),person["mp_id"])
                        Neo4jConnection.Save(diffusion)
                        mention = TwitterBD.DISSEMINATE(selected_user, diffusion)
                        Neo4jConnection.Save(mention)
                        mention.clear()
                        mention = TwitterBD.MENTIONS(person, diffusion)
                        Neo4jConnection.Save(mention)

                break

    except Exception:
        Log.logging.exception("Erro ao enviar mensagem com a pessoa desaparecida")