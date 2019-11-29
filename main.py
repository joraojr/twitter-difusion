from Database import Neo4jConnection, TwitterBD
from Structures import TwitterFunctions, ReadFiles
from Logging import Log
import configparser
import time



#SEE CONFIG_EXAMPLE.INI FILE TO ADD TWITTER, NEO4J KEYS, ETC ( MV config_example.ini TO config.ini)
config = configparser.ConfigParser()

#==Config File==============================================================================

try:
    config.read('config.ini')
except Exception:
    Log.logging.exception("Arquivo de configuração não encontrado, sistema não pode continuar")
    exit()

recommender = True
delete_bd = True

#==Tweeter Conection==============================================================================

consumer_key = config["TWITTER 1"]["CONSUMER_KEY"]
consumer_secret = config["TWITTER 1"]["CONSUMER_KEY_SECRET"]
access_token = config["TWITTER 1"]["ACCESS_TOKEN"]
access_token_secret = config["TWITTER 1"]["ACCESS_TOKEN_SECRET"]

#==Neo4j Conection================================================================================
user_neo4j = config["NEO4J"]["USER"]
password_neo4j = config["NEO4J"]["PASSWORD"]
url_neo4j = config["NEO4J"]["DATABASE"]
endereco_neo4j = config["NEO4J"]["DATABASE"]

#==Conections===================================================================================
Log.logging.info("Conexão com o BD")
Neo4jConnection.Conect_Neo4j(user_neo4j,password_neo4j,url_neo4j)
TwitterFunctions.Conect_Twitter(consumer_key,consumer_secret,access_token,access_token_secret)

if(delete_bd):
    Log.logging.warning("Deletando BD")
    Neo4jConnection.Delete_BD()
    #TwitterBD.Selected_User("teste_desaparec","77829661",0, "teste","teste","teste","teste","0")
    Log.logging.info("Carregando arquivos de perfis do twitter")
    ReadFiles.importProfiles(config["FILES"]["PROFILES_TWITTER"])
    Log.logging.info("Arquivo de perfis do twitter carregado")

    Log.logging.info("Carregando arquivos de pessoas desaparecidas")
    ReadFiles.importMissingPersons(config["FILES"]["MISSING_PEOPLES"])
    Log.logging.info("Arquivo de pessoas desaparecidas carregado")

#match(c:User) return c.usr_location  as location, count(c.usr_location) as frequency order by frequency DESC


#==Recommender====================================================================================

time_of_loop = 60*2 #  minutes

TwitterFunctions.send_mensage_Selected_Users()#manda mensagem para eles confirmarem aceitando e segue

Log.logging.info("Iniciando o sistema de difusão")


#Colocar envio e verificação de desaparecidos aquis

#TwitterFunctions.send_missingPerson_Selected_Users()#recomenda para os usuarios

Log.logging.info("Loop para verificação de retweet")

count = 0

# while count != 1 :
#     time.sleep(time_of_loop)
#
#     TwitterFunctions.verify_retweets_Dissemination()#verifica e salva as recomendacoes que foram retweetadas
#
#     TwitterFunctions.verify_follow()#verifica quem seguiu de volta
#
#
#     count += 1
#

Log.logging.info("Fim do sistema")