from Database import Neo4jConnection
from py2neo import Node, Relationship, RelationshipMatcher, NodeMatcher
from Logging import Log



#variavel global python para padronizar os relacionamentos e nós



#=======================================================================================================================
#   Nodes and Relationships
#=======================================================================================================================

#-----------------------------------------------------------------------------------------------------------------------
#User
#-----------------------------------------------------------------------------------------------------------------------
# def Selected_User(screen_name,id_str,followers_count,friends_count,usr_location,usr_lang,statuses_count,follower):#create tipe user
#     if (len(id_str) > 0):
#         usuario = Node("Selected_User", screen_name=screen_name,id_str=id_str,followers_count=int(followers_count),
#                        friends_count=friends_count, usr_location=usr_location,usr_lang=usr_lang,
#                        statuses_count=statuses_count, follower = follower)
#         Neo4jConnection.Save(usuario)
#     else:
#         return None

# # -----------------------------------------------------------------------------------------------------------------------
# # Selected_User
# # -----------------------------------------------------------------------------------------------------------------------
def Selected_User(id_str, screen_name, selected_at, follower):  # create tipe selected user
    try:
        if (len(id_str) > 0):
                usuario = Node("Selected_User", id_str=id_str, screen_name=screen_name, selected_at=selected_at, follower = follower)
                Neo4jConnection.Save(usuario)
    except Exception:
        Log.logging.exception("Error ao inserir Usuario")


# -----------------------------------------------------------------------------------------------------------------------
# Simple_User
# -----------------------------------------------------------------------------------------------------------------------
def Simple_User(id_str, screen_name):  # create tipe simple user
    if (len(id_str) > 0):
        usuario = Node("Simple_User", id_str=id_str, screen_name=screen_name)
        return usuario
    else:
        return None

# -----------------------------------------------------------------------------------------------------------------------
# Simple_User
# -----------------------------------------------------------------------------------------------------------------------

def MissingPerson(id,mp0):
    try:
        mp = find_MissingPerson(id)
        if (mp == None):
            mp = Node("MissingPerson")
            mp.setdefault("mp_id", id)
            for property in mp0:
                mp.setdefault(property, mp0[property])
            Neo4jConnection.Save(mp)
        else:
            for property in mp0:
                mp.setdefault(property, mp0[property])
            Neo4jConnection.Neo4jEntry.graph_neo4j.push(mp)

    except Exception:
        return None


#-----------------------------------------------------------------------------------------------------------------------
#Tweet
#-----------------------------------------------------------------------------------------------------------------------
def Tweet(id_str,text,favorite_count,media,retweet_count, created_at):#create tipe tweet
    if (len(id_str) > 0) and (len(text) != 0):
        tweet = Node("Tweet", id_str=id_str,text=text,favorite_count=favorite_count,media=media, retweet_count=retweet_count, created_at=created_at)
        return tweet
    else:
        return None

# -----------------------------------------------------------------------------------------------------------------------
# Dissemination
# -----------------------------------------------------------------------------------------------------------------------
def Dissemination(id_str, text, created_at, dissemination):  # create tipe recommendation
    if (len(id_str) > 0) and (len(text) != 0):
        tweet = Node("Dissemination", id_str=id_str, text=text, created_at=created_at, dissemination = dissemination)
        return tweet
    else:
        return None

#-----------------------------------------------------------------------------------------------------------------------
#Relationship
#-----------------------------------------------------------------------------------------------------------------------

def MENTIONS(user,tweet):
    if (user != None) and (tweet != None):
        relacionamento = Relationship(tweet,"MENTIONS",user)
        return relacionamento
    else:
        return None

def RETWEETS(tweet1,tweet2):
    if (tweet1 != None) and (tweet2 != None):
        relacionamento = Relationship(tweet1,"RETWEETS",tweet2)
        return relacionamento
    else:
        return None

def RETWEETED(selected_user,recommendation, retweet):
    if (recommendation != None) and (selected_user != None):
        relacionamento = Relationship(recommendation,"RETWEETED",selected_user,)
        relacionamento.setdefault("id_str",retweet)
        return relacionamento
    else:
        return None

def RETWEETED_SU(simple_user,recommendation,retweet):
    if (recommendation != None) and (simple_user != None):
        relacionamento = Relationship(recommendation,"RETWEETED_SU",simple_user)
        relacionamento.setdefault("id_str",retweet)
        return relacionamento
    else:
        return None

def DISSEMINATE(user, tweet):
    if (tweet != None) and (user != None):
        relacionamento = Relationship(tweet, "DISSEMINATE", user)
        return relacionamento
    else:
        return None

def FOLLOWS(user1,user2):
    if (user1 != None) and (user2 != None):
        relacionamento = Relationship(user1,"FOLLOWS",user2)
        return relacionamento
    else:
        return None

#=======================================================================================================================
#   Search on Data Base
#=======================================================================================================================

#-----------------------------------------------------------------------------------------------------------------------
#Duplicates
#-----------------------------------------------------------------------------------------------------------------------

def delete_Duplicates_Users():
    try:
        query = " MATCH (n:Selected_User) WITH n.id_str as id , collect(n) as ids WHERE size(ids) > 1 FOREACH (n in tail(nodes) | DETACH DELETE n)"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
    except:
        #print ("Can not changed.")
        None


#-----------------------------------------------------------------------------------------------------------------------
#User
#-----------------------------------------------------------------------------------------------------------------------
def find_User(id_str):#find single user
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            user = selector.match("User", id_str=id_str).first()
            return user
        except:
            #print ("Can not find.")
            return None

def getAll_Users():
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            users = selector.match("User")
            return users
        except ValueError:
                    # print ("Can not find tweet.")
            return None

### Missing Person
def find_MissingPerson(id):

    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            mp = selector.match("MissingPerson", mp_id=id).first()
            return mp
        except:
            #print ("Can not find.")
            return None


def getAll_Missing_Persons():
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            mp = selector.match("MissingPerson")
            return mp
        except ValueError:
                    # print ("Can not find tweet.")
            return None

def getAll_Users_limit(limit, skip):
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            users = selector.match("User").skip(skip).limit(limit)
            count = 0
            for user in users:
                count = count + 1
            if (count == 0):
                return None
            return users
        except ValueError:
            return None

def set_User(id_str,set_variable, value):  # change a value of variable by user id
    try:
        query = " MATCH (user:User) WHERE user.id_str = '"+id_str+"' SET user."+set_variable+" = '"+value+"'"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
    except:
        #print ("Can not changed.")
        None


def get_Users_by_Followers_count_DESC_ONLYscreen_name(num_users):  # get user order by desc number od followers, only screen_name
    try:
        query = " MATCH (user:User) RETURN user.screen_name AS screen_name, user.id_str AS id_str ORDER BY user.followers_count DESC LIMIT "+str(num_users)
        data = Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
        #for d in data:
            #d = d['screen_name']
            #print d
        return data
    except:
        return None

def get_Users_by_Tweets_count_DESC_ONLYscreen_name(num_users):  # get user order by desc number od followers, only screen_name
    try:
        query = " MATCH (user:User) RETURN user.screen_name AS screen_name, user.id_str AS id_str ORDER BY user.statuses_count DESC LIMIT "+str(num_users)
        data = Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
        #for d in data:
            #d = d['screen_name']
        return data
    except:
        return None

def get_Users_by_Tweets_keyword_DESC_ONLYscreen_name(num_users):  # get user order by desc number od followers, only screen_name
    try:
        query = " MATCH (user:User)-[post:POSTS]->(tweet:Tweet) WITH user,count(tweet) as rels, collect(tweet) as num WHERE rels>1 RETURN user.id_str as id_str,user.screen_name as screen_name,rels as count ORDER BY rels DESC LIMIT "+str(num_users)
        data = Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
        # for d in data:
        # d = d['screen_name']
        return data
    except:
        return None


# -----------------------------------------------------------------------------------------------------------------------
# Selected User
# -----------------------------------------------------------------------------------------------------------------------
def find_Selected_User(id_str):  # find single user
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            user = selector.match("Selected_User", id_str=id_str).first()
            return user
        except:
            # print ("Can not find.")
            return None

def find_Simple_User(id_str):  # find single user
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            user = selector.match("Simple_User", id_str=id_str).first()
            return user
        except:
            # print ("Can not find.")
            return None

def getAll_Selected_Users():
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            selected_users = selector.match("Selected_User")
            return selected_users
        except ValueError:
            # print ("Can not find tweet.")
            return None

def count_Selected_User_MENTIONS(id_str):
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            user = selector.match("Selected_User", id_str=id_str).first()
            count = 0
            if(user!=None):
                relationship = RelationshipMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
                rels = relationship.match(r_type="MENTIONS",nodes=(None,user))
                for rel in rels:
                    if(rel != None):
                        count = count + 1
                return count
        except Exception as e:
            print(e)
            return None

def set_Selected_User(id_str, set_variable, value):  # change a value of variable by tweet id
    try:
        query = " MATCH (su:Selected_User) WHERE su.id_str = '" + id_str + "' SET su." + set_variable + " = '" + value + "'"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
    except:
        # print ("Can not changed.")
        None

#-----------------------------------------------------------------------------------------------------------------------
#Tweet
#-----------------------------------------------------------------------------------------------------------------------
def find_Tweet(id_str):#find single tweet
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            tweet = selector.match("Tweet").where(id_str=id_str).first()
            #print tweet['text']
            return tweet
        except ValueError:
            #print ("Can not find user.")
            return None

def getAll_Tweets():
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            tweet = selector.match("Tweet")
            return tweet
        except ValueError:
            # print ("Can not find tweet.")
            return None

def getAll_Tweets_limit(limit, skip):
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            tweet = selector.match("Tweet").skip(skip).limit(limit)
            count = 0
            for t in tweet:
                count = count + 1
            if (count == 0):
                return None
            return tweet
        except ValueError:
            # print ("Can not find tweet.")
            return None

def find_Tweets_Text(text):  # find single tweet
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            tweet = selector.match("Tweet").where("_.text=~ '.*" + text + ".*'")
            return tweet
        except ValueError:
            # print ("Can not find tweet.")
            return None

def set_Tweet(id_str, set_variable, value):  # change a value of variable by tweet id
    try:
        query = " MATCH (tweet:Tweet) WHERE tweet.id_str = '" + id_str + "' SET tweet." + set_variable + " = '" + value + "'"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
    except:
        # print ("Can not changed.")
        None

# -----------------------------------------------------------------------------------------------------------------------
# Dissemination
# -----------------------------------------------------------------------------------------------------------------------

def getAll_Dissemination():
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        # print("Conect neo4j before with Conect_Neo4j() function.")
        # print ("Can not find tweet.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            recommendations = selector.match("Dissemination")
            return recommendations
        except ValueError:
            # print ("Can not find tweet.")
            return None


def find_Dissemination_RETWEETED_Selected_User(tweet_id_str,user_id_str):
    #try:
        query = "MATCH (df:Dissemination)-[r:RETWEETED]->(su:Selected_User) WHERE su.id_str = '"+ user_id_str +"' AND df.id_str = '"+ tweet_id_str +"' RETURN su as Selected_User,r as RETWEETED,df as Recommendation"
        data = Neo4jConnection.Neo4jEntry.graph_neo4j.run(query).data()
        return data
   # except:
        # print ("Can not changed.")
        None

def find_Dissemination_RETWEETED_Simple_User(tweet_id_str,user_id_str):
    #try:
        query = "MATCH (df:Dissemination)-[r:RETWEETED]->(su:Simple_User) WHERE su.id_str = '"+ user_id_str +"' AND df.id_str = '"+ tweet_id_str +"' RETURN su as Selected_User,r as RETWEETED,df as Recommendation"
        data = Neo4jConnection.Neo4jEntry.graph_neo4j.run(query).data()
        return data
   # except:
        # print ("Can not changed.")
        None

def find_Dissemination(id_str):#find single tweet
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            diffusion = selector.match("Dissemination").where(id_str=id_str).first()
            #print tweet['text']
            return diffusion
        except ValueError:
            #print ("Can not find user.")
            return None

#-----------------------------------------------------------------------------------------------------------------------
#Relationship
#-----------------------------------------------------------------------------------------------------------------------

def delete_User(id_str):
    try:
        query = " MATCH (user:User)-[post:POSTS]->(tweet:Tweet) WHERE user.id_str = '" + id_str + "' DELETE post,tweet,user"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
    except:
        # print ("Can not changed.")
        None

def delete_Selected_User(id_str):  # change a value of variable by tweet id, before delete_Simple_User
    #try:
        print(id_str)
        query = " MATCH (tweet:Tweet)-[mentions:MENTIONS]->(selected_user:Selected_User) WHERE selected_user.id_str = '" + id_str + "' DELETE mentions , tweet"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)

        query = " MATCH (recommendation:Recommendation)-[retweeted:RETWEETED]->(su:Selected_User) WHERE su.id_str = '" + id_str + "' return recommendation.id_str as id_str"
        data = Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)

        for recommender in data:
            #print recommender['id_str']
            delete_Simple_Users_by_Recommendation(recommender['id_str'])

        query = " MATCH (recommendation:Recommendation)-[rsu:RETWEETED]->(su:Selected_User) WHERE su.id_str = '" + id_str + "' DELETE rsu  "
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)

        query = " MATCH (recommendation:Recommendation)-[recommender:RECOMMENDER]->(su:Selected_User) WHERE su.id_str = '" + id_str + "' DELETE recommender, recommendation"
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)

        selected_user =  find_Selected_User(id_str)
        Neo4jConnection.Neo4jEntry.graph_neo4j.delete(selected_user)
    #except:
        # print ("Can not changed.")
        None

def delete_Simple_Users_by_Recommendation(id_str):
    try:
        query = " MATCH (recommendation:Recommendation)-[rsu:RETWEETED_SU]->(su:Simple_User) WHERE recommendation.id_str = '" + id_str + "' DELETE rsu , su "
        Neo4jConnection.Neo4jEntry.graph_neo4j.run(query)
    except:
        # print ("Can not changed.")
        None



def count_User_DISSEMINATE(id_str):
    if (Neo4jConnection.Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            selector = NodeMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j)
            user = selector.match("Selected_User", id_str=id_str).first()
            count = 0
            if(user!=None):
                for rel in RelationshipMatcher(Neo4jConnection.Neo4jEntry.graph_neo4j).match(nodes=(None,user),r_type="DISSEMINATE"):
                    if(rel != None):
                        count = count + 1
                        break
            return count
        except Exception:
           Log.logging.exception("Erro ao contar")


