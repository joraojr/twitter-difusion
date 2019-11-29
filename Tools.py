import time
import calendar
import os
import datetime
import subprocess

def getDays_of_Month(year,month):
    return calendar.monthrange(year, month)

def get_complete_date_time():
    os.environ['TZ'] = 'Europe/Greenland'
    time.tzset()
    return time.strftime("%H:%M:%S-%d/%m/%Y")

def return_life_time(created_at_datatimeTYPE):
    os.environ['TZ'] = 'Europe/Greenland'
    time.tzset()
    now = datetime.datetime.now()
    life_time = now - created_at_datatimeTYPE
    return life_time

def lifeTime_Days(life_days,day,month,year,hour,minute,second):
    os.environ['TZ'] = 'Europe/Greenland'
    time.tzset()
    now = datetime.datetime.now()
    born = datetime.datetime(year=year,month=month,day=day,hour=hour,minute=minute,second=second)
    interval = datetime.timedelta(days=life_days)
    end = born + interval
    if (end >= now):#is alive
        return True
    else:#is dead
        return False

def lifeTime_Minutes(life_Minutes,day,month,year,hour,minute,second):
    os.environ['TZ'] = 'Europe/Greenland'#set location
    time.tzset()#save set
    now = datetime.datetime.now()#get data now
    born = datetime.datetime(year=year,month=month,day=day,hour=hour,minute=minute,second=second)#create data for comparition
    interval = datetime.timedelta(minutes=life_Minutes)# create a interval
    end = born + interval#born + time life
    if (end >= now):
        return True
    else:
        return False

def write_file(file_name,vector_info):# salve in file, end of file
    file = open(file_name, 'a')
    for info in vector_info:
        file.write(str(info))
    file.close()

class ShortUrl():

    @staticmethod
    def GetShortUrl(url):
        try:
            if(len(url)>0):
                results = subprocess.check_output(
                    """curl https://www.googleapis.com/urlshortener/v1/url?key=AIzaSyA4twtd9t1yz61JYRKTceZvCpmgMdrqk3s   -H 'Content-Type: application/json'   -d '{"longUrl": "%s"}'""" % (
                        str(url)), shell=True)
                #print(results)
                results = str(results)
                #results = json.loads(str(results))
                a = results[results.find('"id": "')+7:results.find('longUrl')-6]
                #print(a)
                a = a.replace('http://', '')
                a = a.replace('https://', '')
                return a
            else:
                return None
        except:
            return None
