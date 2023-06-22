import pymongo
import pandas as pd
import json
import mysql.connector
import pdb

# client = pymongo.MongoClient('mongodb://localhost:27017')
# database = client['Youtube_Scrapping']

def insert_data(collection_name,dataframe,database_name):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    # if database_name != None:
    database = client[database_name]
    collection = database[collection_name]
    data_dict = dataframe.to_dict(orient='records')
    collection.insert_many(data_dict)
    # else:
    client.close()


def fetch_database():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    database_names = client.list_database_names()
    client.close()
    return database_names


def from_mongo(options):
    channel_df=[]
    playlist_df=[]
    video_df=[]
    comment_df=[]

    for db in options:
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database_names = client.list_database_names()
        database = client[db]
        collection_names = database.list_collection_names()

        for collection_name in collection_names:
            if collection_name== 'channel':
                collections = database[collection_name]
                for i in collections.find():
                    channel_df.append(i)
            
            elif collection_name== 'playlist':
                collections = database[collection_name]
                for i in collections.find():
                    playlist_df.append(i)
            
            elif collection_name== 'video':
                collections = database[collection_name]
                for i in collections.find():
                    video_df.append(i)
            
            elif collection_name== 'comment':
                collections = database[collection_name]
                for i in collections.find():
                    comment_df.append(i)

                
    return channel_df, playlist_df, video_df, comment_df
        
def into_sql(data):
    conn = mysql.connector.connect(host='localhost',
                                user='root',
                                password='Terralogic@123',
                                database='yogeshdb')    
    cursor = conn.cursor()
    # cursor.execute("SELECT * FROM yogeshdb.channels")
    channel_df = data[0]
    playlist_df = data[1]
    video_df = data[2]
    comment_df = data[3]
    full_df = [channel_df, playlist_df, video_df, comment_df]
    variable_names = ['channel_df', 'playlist_df', 'video_df', 'comment_df']
    for i in range(0,len(full_df)):
        if variable_names[i] == 'channel_df':
            insert_query = "INSERT INTO channels (channel_title, channel_id, channel_description, channel_viewcount, channel_privacystatus, channel_published) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (channel_df[0]['channel_title'], channel_df[0]['channel_id'], channel_df[0]['channel_description'], channel_df[0]['channel_viewcount'], channel_df[0]['channel_privacystatus'], channel_df[0]['channel_published'])
            cursor.execute(insert_query, values)
            conn.commit()
            
        elif variable_names[i] == 'playlist_df':
            insert_query = "INSERT INTO playlists (name, channel_id, playlist_id, total_videos, published) VALUES (%s, %s, %s, %s, %s)"
            values = (playlist_df[0]['name'], playlist_df[0]['channel_id'], playlist_df[0]['playlist_id'], playlist_df[0]['total_videos'], playlist_df[0]['published'])
            cursor.execute(insert_query, values)
            conn.commit()
        
        elif variable_names[i] == 'video_df':
            for j in range(0,len(video_df)):
                insert_query = "INSERT INTO videos (video_id, playlist_id, video_title, video_description, published, view_count, like_count, favourite_count, comment_count, thumbnail, caption) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (video_df[j]['video_id'], video_df[j]['playlist_id'], video_df[j]['video_title'], video_df[j]['video_description'], video_df[j]['published'],video_df[j]['view_count'], video_df[j]['like_count'], video_df[j]['favourite_count'], video_df[j]['comment_count'], video_df[j]['thumbnail'], video_df[j]['caption'])
                cursor.execute(insert_query, values)
                conn.commit()

        elif variable_names[i] == 'comment_df':
            for k in range(0,len(comment_df)):
                insert_query = "INSERT INTO comments (video_id, comment_text, authr_name, published_date, comment_id) VALUES (%s, %s, %s, %s, %s)"
                values = (comment_df[k]['video_id'], comment_df[k]['comment_text'], comment_df[k]['authr_name'], comment_df[k]['published_date'], comment_df[k]['comment_id'])
                cursor.execute(insert_query, values)
                conn.commit()

    cursor.close()
    conn.close()
    return 1
