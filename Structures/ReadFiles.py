import csv
import Logging as Log
from Database import TwitterBD

def importProfiles(file):
    with open(file,'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';', quoting=csv.QUOTE_NONE)
        for row in spamreader:
            try:
                TwitterBD.Selected_User(screen_name=row[0],id_str=row[1],follower="0",selected_at="ini_proj")
            except Exception:
                Log.logging.exception("Erro ao inserir: " + row)
        try:
            TwitterBD.delete_Duplicates_Users()
        except Exception:
            Log.logging.fatal("Erro ao deletar duplicatas")

def importMissingPersons(file):
    with open(file,'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ',quotechar='"')
        id_old = "1"
        mp = {}
        for row in spamreader:
            id = row[0].replace("<http://www.desaparecidos.ufjf.br/desaparecidos/","")
            id = id.replace(">","")
            property = row[1].replace("<http://www.desaparecidos.com.br/rdf/","")
            property = property.replace("<http://xmlns.com/foaf/0.1/","")
            property = property.replace("<http://dbpedia.org/property/","")
            property = property.replace("<http://www.w3.org/1999/02/22-rdf-syntax-ns#","")
            property = property.replace(">","")
            val = row[2].lower()
            if(id == id_old):
                mp[property] = val
            else:
                TwitterBD.MissingPerson(id_old,mp)
                id_old = id
                mp.clear()
                mp[property] = val

        TwitterBD.MissingPerson(id,mp)

