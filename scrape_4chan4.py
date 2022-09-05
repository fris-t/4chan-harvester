import json
import pandas as pd
import numpy as np
from urllib.request import Request, urlopen
###
import mysql.connector
#from mysql.connector import Error
###
import sqlalchemy
import pymysql
import ssl
import requests
from time import sleep
from bs4 import BeautifulSoup as soup
import datetime
import re 
from sqlalchemy.dialects.oracle import VARCHAR2
from sqlalchemy import update


############################
### OPEN DATABASE CONNECTION
############################

engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost')
#engine.execute("CREATE DATABASE fourchan") 
############################
### CREATING FUNCTIONS
############################

##STARTPOINT
def startpoint():
    #open currentthreads
    context = ssl._create_unverified_context()
    threads_api= urlopen('http://a.4cdn.org/pol/threads.json', context=context).read().decode('utf-8')
    data=json.loads(threads_api)
    active_threads=pd.DataFrame()
    for page in data:
        active_threads=active_threads.append(page['threads'], ignore_index=True)
    print(active_threads)

    try:
        #sql connection
        engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost/fourchan')
        active_threads.to_sql(name='actives', con=engine, index=False, if_exists='replace')
        print("database filled")
    except:
        print("didn't work")
print("startpoint function created")

##newbatch
def newbatch():
    context = ssl._create_unverified_context()
    threads_api= urlopen('http://a.4cdn.org/pol/threads.json', context=context).read().decode('utf-8')
    data=json.loads(threads_api)
    newbatch_1=pd.DataFrame()
    for page in data:
        newbatch_1=newbatch_1.append(page['threads'], ignore_index=True)
    print(newbatch_1)

    try:
        engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost:3306/fourchan')
        newbatch_1.to_sql(name='newbatch',
        con=engine,
        index=False,
        if_exists='replace')
        print("database filled")
    except:
        raise
        print("didn't work")
print("newbatch function created")

##archive threads
def archive_threads(thread_number):
    # establish database connection, for what it is worth
    db=mysql.connector.connect(
        host="localhost", 
        user='root',
        passwd='mysqlpass',
        database='fourchan'
    )
    #cursor
    mycursor=db.cursor()
    sql = "DELETE FROM actives WHERE no = " + str(thread_number)
    mycursor.execute(sql)
    db.commit()
print("archive threads function created")

##the protocol
def protocol():

    #load in data
    engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost:3306/fourchan')
    actives = pd.read_sql_table("actives", engine)
    print("current active " + str(len(actives)))
    news= pd.read_sql_table("newbatch", engine)
    len(news)

    activesno = actives['no']
    newsno = news['no'][2:]

    ###### ARCHIVE TRHEADS ######
    archivedthreads=pd.DataFrame()
    for i in list(activesno):
        if i not in list(newsno):
            print("thread " + str(i) + " is archived")
            archivedthreads=archivedthreads.append(actives.loc[actives['no'] == i], ignore_index=True)
            archive_threads(i)
    archivedthreads.to_sql(name='archived', con=engine, index=False, if_exists='append')

    ###### NEW TRHEADS ######
    newthreads=pd.DataFrame()
    for i in list(newsno): 
        print(i)
        if i not in list(activesno):
            print("thread " + str(i) + " is new")
            newthreads=newthreads.append(news.loc[news['no'] == i], ignore_index=True)
            threadcrawler(i)
        else:
            thread_in_news = news.loc[news['no'] == i]
            thread_in_actives = actives.loc[actives['no'] == i]
            if int(thread_in_news['last_modified']) > int(thread_in_actives['last_modified']):
                newreplies=int(thread_in_news['replies']) - int(thread_in_actives['replies']) # this gives you how many new replies to scrape
                #print("newreplies: " + str(newreplies))
                print("thread " + str(i) + " has been recently updated with " + str(newreplies) + " new replies")
                replycrawler(i, newreplies)

                ###HIER PROBLEEM!!! --> aantal posts in de actives database moet worden ge-update
                engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost:3306/fourchan')
                stmnt= "UPDATE actives SET replies = " + str(int(thread_in_news['replies'])) + " WHERE no = " + str(i)
                #print(stmnt)
                engine.execute(stmnt) 
            else:
                newreplies=int(thread_in_news['replies']) - int(thread_in_actives['replies']) 
                print("thread " + str(i) + " has been stable with " + str(newreplies) + " new replies")
    #append new threads to database
    newthreads.to_sql(name='actives', con=engine, index=False, if_exists='append')
print("The protocol function created")

##the full threadcrawler
def threadcrawler(someinput):
    thread_no=someinput
    link= 'http://boards.4chan.org/pol/thread/' +str(thread_no)

    ###Make Request to url####
    headers={'User-Agent': 'Mozilla/5.0 (Macinstosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36(KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    req = Request(link, headers=headers)
    context = ssl._create_unverified_context()

    ###Get HTML####
    print("grabbing html of thread " + str(thread_no))
    try:
        uClient= urlopen(req, context=context)
        page_html= uClient.read()
        page_html=str(page_html).replace("<wbr>", "")
        page_html=str(page_html).replace("<br>", " ")
        uClient.close()

        ###Parse HTML with BeautifulSoup####
        page_soup= soup(page_html, "html.parser")

        ###get number of replies###
        reply_containers=page_soup.find_all("div", {"class", "postContainer replyContainer"})
        replies=len(reply_containers)
        reply_id=thread_no

        ###find the original post###
        post_container=page_soup.find_all("div", {"class", "postContainer opContainer"})

        #Post Date 
        date_post=post_container[0].find_all("span", {"class", "dateTime"})
        unix_post=date_post[0]["data-utc"]
        datetime_post=datetime.datetime.fromtimestamp(int(unix_post)).strftime('%Y-%m-%d %H:%M:%S')

        #Geo-polical Identification POST
        flag_post=post_container[0].find_all("span", {"class", "flag"})
        if len(flag_post) < 1:
            geo_id_post='non identifiable'
            flag_id_post='non identifiable'
        else:
            geo_id_post=flag_post[0]['title']
            flag_id_post=flag_post[0]['class'][1]

        #Screen name 
        name=post_container[0].find_all("span", {"class", "name"})
        screen_name_poster=name[0].text

        #Poster ID
        try:
            pos_id=post_container[0].find_all("span", {"class", "hand"})
            poster_id=pos_id[0].text
        except:
            poster_id=str('no_posterid')

        #Thread Subject
        subject=post_container[0].find_all("span", {"class", "subject"})
        subject_text=subject[0].text
        if len(subject_text) < 1:
            post_subject= str('no subject')
        else:
            post_subject=subject_text

        #Image
        image=post_container[0].find_all("a", {'class', "fileThumb"})
        if len(image) < 1:
            image_file= 0
        else:
            image_file='https:'+ image[0]['href']

        image_filename= 'thread_' + str(thread_no) + '_' + str(reply_id)+'.jpg'
        image_url=image_file

        try:
            result = requests.get(image_url, stream=True)
            if result.status_code == 200:
                image = result.raw.read()
                open(image_filename,"wb").write(image)
            print(str(image_filename) + ' downloaded and saved')
        except:
            pass
            #print('no image to download')
            #print('') 

        #Quotes in post
        quotes= str('no_quotes')

        #Text in post
        post=post_container[0].find("blockquote")
        post_text = post.get_text()
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', post_text)
        if len(urls) <1:
            urls=["no_urls"]
        else:
            urls=str(urls)

        #INFO TO DATAFRAME 
        postdata=[thread_no, post_subject, replies, reply_id, int(unix_post), datetime_post, geo_id_post, flag_id_post, screen_name_poster, poster_id, image_url, str(quotes), [post_text], urls]
        thread_dataframe=pd.DataFrame([postdata], columns=["threadno", "thread_subject", "nr_replies", "replyno", "unix_date", "real_date", "geo_id", "flag_id", "screen_name", "poster_id", "image_url", "quoting", "post_text", "urls"])

        #INFO TO SQL
        table_name= 'thread_' + str(thread_no) 
        #columnnames= "(threadno INTEGER(15), thread_subject INTEGER(15), nr_replies INTEGER(10), replyno INTEGER(15), unix_date INTEGER(15), real_date VARCHAR(32), geo_id VARCHAR(32), flag_id VARCHAR(32), screen_name VARCHAR(50),  poster_id VARCHAR(50),  image_url VARCHAR(250),  quoting VARCHAR(750),  post_text VARCHAR(1550), urls VARCHAR(1550))"
        #newsqltable(table_name, columnnames)
        engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost:3306/fourchan')
        try:
            thread_dataframe.to_sql(name=table_name,
            con=engine,
            index=False,
            if_exists='append')
            print("table correctly filled")
        except:
            print( "PROBLEM WITH " + str(table_name) + " sleeping now for 10 seconds")
            sleep(10)
            print( "TRYING AGAIN " + str(table_name))
            thread_dataframe.to_sql(name=table_name,
            con=engine,
            index=False,
            if_exists='append')
            print("table correctly filled")

        #replies
        if len(reply_containers) < 1:
            print("no replies")
        else:
            for reply_container in reply_containers:
                reply_no=reply_container.div["id"][2:]
                reply_id=reply_no

                date_reply=reply_container.find_all("span", {"class", "dateTime"})
                unix_reply=date_reply[0]["data-utc"]
                datetime_reply=datetime.datetime.fromtimestamp(int(unix_reply)).strftime('%Y-%m-%d %H:%M:%S')

                #Geo-polical Identification
                flag=reply_container.find_all("span", {"class", "flag"})
                if len(flag) < 1:
                    geo_id='non identifiable'
                    flag_id='non identifiable'
                else:
                    geo_id=flag[0]['title']
                    flag_id=flag[0]['class'][1]

                #Screen name
                name=reply_container.find_all("span", {"class", "name"})
                screen_name=name[0].text

                #Replier  id
                rep_id=reply_container.find_all("span", {"class", "hand"})
                replier_id=rep_id[0].text

                #Quotes
                quote_links=reply_container.find_all("a", {"class", "quotelink"})
                if len(quote_links) < 1:
                    quotes=['no_quotes']
                else:
                    quotes=list()
                    for quote in quote_links:
                        quotes.append(quote.text[2:])

                #Check for images and store URL
                image=reply_container.find_all("a", {'class', "fileThumb"})
                if len(image) < 1:
                    image_file= 0
                else:
                    image_file=image[0]['href']

                image_filename= 'thread_' + str(thread_no) + '_' + str(reply_id)+'.jpg'
                image_url='https:'+ str(image_file)

                #try:
                    #result = requests.get(image_url, stream=True)
                    #if result.status_code == 200:
                        #image = result.raw.read()
                        #open(image_filename,"wb").write(image)
                        #print(str(image_filename) + ' downloaded and saved')
                #except:
                    #pass
                    #print('no image to download')
                    #print('') 

                #Text in post
                post=reply_container.find("blockquote")
                post_text = post.get_text()
                urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', post_text)
                if len(urls) <1:
                    urls=["no_urls"]
                else:
                    urls=str(urls)

                #REPLIES TO DATAFRAME
                replydata={"threadno":thread_no, 
                "thread_subject":post_subject, 
                "nr_replies":replies, 
                "replyno":reply_id, 
                "unix_date":int(unix_reply), 
                "real_date":datetime_reply, 
                "geo_id":geo_id, 
                "flag_id": flag_id, 
                "screen_name": screen_name, 
                "poster_id": replier_id, 
                "image_url": image_url, 
                "quoting": str(quotes), 
                "post_text":post_text, 
                "urls":str(urls)}
                len(replydata)
                reply_dataframe=pd.DataFrame([replydata])
                engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost:3306/fourchan')
                reply_dataframe.to_sql(name=table_name, con=engine, index=False, if_exists='append')
            print("table for thread " + str(thread_no) + " updated")
    except:
        print("Error 404 -- thread not active anymore")
print("The threadcrawler function created")

##the replycrawler
def replycrawler(someinput, number_of_new_replies):
    thread_no=someinput
    link= 'http://boards.4chan.org/pol/thread/' +str(thread_no)

    ###Make Request to url####
    headers={'User-Agent': 'Mozilla/5.0 (Macinstosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36(KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    req = Request(link, headers=headers)
    context = ssl._create_unverified_context()

    ###Get HTML####
    print("grabing html of thread " + str(thread_no))
    try:
        uClient= urlopen(req, context=context)
        page_html= uClient.read()
        page_html=str(page_html).replace("<wbr>", "")
        page_html=str(page_html).replace("<br>", " ")
        uClient.close()

        ###Parse HTML with BeautifulSoup####
        page_soup= soup(page_html, "html.parser")

        ###get number of replies###
        reply_containers=page_soup.find_all("div", {"class", "postContainer replyContainer"})
        replies=len(reply_containers)

        #Thread Subject
        post_container=page_soup.find_all("div", {"class", "postContainer opContainer"})
        subject=post_container[0].find_all("span", {"class", "subject"})
        subject_text=subject[0].text
        if len(subject_text) < 1:
            post_subject= str('no subject')
        else:
            post_subject=subject_text

        for reply_container in reply_containers[-number_of_new_replies:]:
            reply_no=reply_container.div["id"][2:]
            reply_id=reply_no

            date_reply=reply_container.find_all("span", {"class", "dateTime"})
            unix_reply=date_reply[0]["data-utc"]
            datetime_reply=datetime.datetime.fromtimestamp(int(unix_reply)).strftime('%Y-%m-%d %H:%M:%S')

            #Geo-polical Identification
            flag=reply_container.find_all("span", {"class", "flag"})
            if len(flag) < 1:
                geo_id='non identifiable'
                flag_id='non identifiable'
            else:
                geo_id=flag[0]['title']
                flag_id=flag[0]['class'][1]

            #Screen name
            name=reply_container.find_all("span", {"class", "name"})
            screen_name=name[0].text

            #Replier  id
            rep_id=reply_container.find_all("span", {"class", "hand"})
            replier_id=rep_id[0].text

            #Quotes
            quote_links=reply_container.find_all("a", {"class", "quotelink"})
            if len(quote_links) < 1:
                quotes=['no_quotes']
            else:
                quotes=list()
                for quote in quote_links:
                    quotes.append(quote.text[2:])

            #Check for images and store URL
            image=reply_container.find_all("a", {'class', "fileThumb"})
            if len(image) < 1:
                image_file= 0
            else:
                image_file=image[0]['href']

            image_filename= 'thread_' + str(thread_no) + '_' + str(reply_id)+'.jpg'
            image_url='https:'+ str(image_file)

            #try:
                #result = requests.get(image_url, stream=True)
                #if result.status_code == 200:
                    #image = result.raw.read()
                    #open(image_filename,"wb").write(image)
                    #print(str(image_filename) + ' downloaded and saved')
            #except:
                #pass
                #print('no image to download')
                #print('') 

            #Text in post
            post=reply_container.find("blockquote")
            post_text = post.get_text()
            urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', post_text)
            if len(urls) <1:
                urls=["no_urls"]
            else:
                urls=str(urls)

            #REPLIES TO DATAFRAME
            replydata={"threadno":thread_no, 
            "thread_subject":post_subject, 
            "nr_replies":replies, 
            "replyno":reply_id, 
            "unix_date":int(unix_reply), 
            "real_date":datetime_reply, 
            "geo_id":geo_id, 
            "flag_id": flag_id, 
            "screen_name": screen_name, 
            "poster_id": replier_id, 
            "image_url": image_url, 
            "quoting": str(quotes), 
            "post_text":post_text, 
            "urls":str(urls)}
            table_name= 'thread_' + str(thread_no) 
            reply_dataframe=pd.DataFrame([replydata])
            engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost:3306/fourchan')
            reply_dataframe.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        print("table for thread " + str(thread_no) + " updated")
    except:
        print("Error 404 -- thread not active anymore")
print("The replycrawler function created")


############################
### CREATING FUNCTIONS
############################

print(" #### starting scraping ####")
startpoint()
engine= sqlalchemy.create_engine('mysql+pymysql://root:mysqlpass@localhost/fourchan')
actives = pd.read_sql_table("actives", engine)
activesno = actives['no']
for i in list(activesno):
    threadcrawler(i)
print("start point scraped -- pausing for 1,5 minutes")
sleep(90)
batchid=1
while 1 > 0:
    print(" #### scraping batch nr " + str(batchid) + " ####")
    newbatch()
    sleep(10)
    protocol()
    print(" #### batch nr " + str(batchid) + " scraped! Pausing for 1.5 minutes ####")
    sleep(90)
    batchid=batchid+1

