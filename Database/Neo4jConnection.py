from py2neo import Graph
from Logging import Log
#FOR TEST QUERYS, coment try and except

#==Neo4J acess======================================================

class Neo4jEntry:
    user_neo4j = None
    password_neo4j = None
    url_neo4j = None
    graph_neo4j = None
    schema_neo4j = None


#=======================================================================================================================
#   Basic Operations
#=======================================================================================================================

def Conect_Neo4j(user, password, url):#conect
    Neo4jEntry.user_neo4j = user
    Neo4jEntry.password_neo4j = password
    Neo4jEntry.url_neo4j = url
    Neo4jEntry.graph_neo4j = Graph(Neo4jEntry.url_neo4j, auth=(Neo4jEntry.user_neo4j, Neo4jEntry.password_neo4j))
    Neo4jEntry.schema_neo4j = Neo4jEntry.graph_neo4j
    return Neo4jEntry.graph_neo4j

def Delete_BD():#delete DB
    try:
        Neo4jEntry.graph_neo4j.delete_all()
    except Exception:
        Log.logging.exception("Error Conect neo4j before")


def Save(object):#save object
    if (Neo4jEntry.graph_neo4j == None):
        #print("Conect neo4j before with Conect_Neo4j() function.")
        return None
    else:
        try:
            Neo4jEntry.graph_neo4j.create(object)
        except:
            #print ("Can not Save.")
            return None

