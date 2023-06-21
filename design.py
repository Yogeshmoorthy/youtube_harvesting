from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import re
import json
from googleapiclient.errors import HttpError

api_key='AIzaSyAAgRZSfn5osPVOWWpYkFC1Asp_y1-lsco'


##Fetching Channel Data
def get_channel(api_key, channel_name):
    channel={}
    youtube=build('youtube','v3',developerKey=api_key)
    request = youtube.channels().list(
        part="snippet,statistics,status,contentDetails,contentOwnerDetails",
        forUsername=channel_name)
    response=request.execute()
    try:
        channel['channel_title']=response['items'][0]['snippet']['localized']['title']
        channel['channel_id']=response['items'][0]['id']
        channel['channel_description']=response['items'][0]['snippet']['description']
        channel['channel_viewcount']=response['items'][0]['statistics']['viewCount']
        channel['channel_subscriberscount']=response['items'][0]['statistics']['subscriberCount']
        channel['channel_videocount']=response['items'][0]['statistics']['videoCount']
        channel['channel_privacystatus']=response['items'][0]['status']['privacyStatus']
        channel['channel_published']=response['items'][0]['snippet']['publishedAt']
    except KeyError as e:
        print('No Channel Found')

    df=pd.DataFrame(channel,index=[1],columns=[i for i in channel.keys()])
    
    return df

# channel_df = get_channel(api_key,channel_name='')


# channel_df
# id=channel_df.iloc[0,1]
# print(id)


##Fetching Playlist 
def get_playlist(id,api_key):
    youtube=build('youtube','v3',developerKey=api_key)
    request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=id)
    response=request.execute()
    total_playlist=response['pageInfo']['totalResults']
    playlist_df = pd.DataFrame(columns=['name','channel_id','playlist_id','total_videos','published'])
    
    for i in response['items']:
        play_df={}
        play_df['name']=i['snippet']['localized']['title']
        play_df['channel_id']=i['snippet']['channelId']
        play_df['playlist_id']=i['id']
        play_df['total_videos']=i['contentDetails']['itemCount']
        play_df['published']=i['snippet']['publishedAt']
        playlist_df=playlist_df.append(play_df,ignore_index=True)
   
    for page in range(5, total_playlist, 5):
        if 'nextPageToken' in response.keys():
            request = youtube.playlists().list(
                            part="snippet,contentDetails",
                            channelId=id,
                            pageToken=response['nextPageToken'])
            response=request.execute()
            for i in response['items']:
                play_df={}
                play_df['name']=i['snippet']['localized']['title']
                play_df['channel_id']=i['snippet']['channelId']
                play_df['playlist_id']=i['id']
                play_df['total_videos']=i['contentDetails']['itemCount']
                play_df['published']=i['snippet']['publishedAt']
#                 playlist_df=playlist_df.append(play_df,ignore_index=True)
                playlist_df=playlist_df.append(play_df,ignore_index=True)
        else:
            print('No Next Page')
        
    return playlist_df
    
# playlist_df=get_playlist(id,api_key)

##Fetching Videos 
def get_video(playlist_df):
#     global response
    video_data={}
    video_df = pd.DataFrame(columns=['video_id','playlist_id','video_title','video_description','published','view_count','like_count','fovourite_count','comment_count','thumbnail','caption'])
    for pid in playlist_df['playlist_id']:
        youtube=build('youtube','v3',developerKey=api_key)
        request = youtube.playlistItems().list(part="snippet,status,contentDetails",playlistId=pid)
        response=request.execute()
        try:
            for i in range(0,5):
                vid=response['items'][i]['snippet']['resourceId']['videoId']
                vid_request = youtube.videos().list(part="snippet,contentDetails,statistics",id=vid)
                vid_response = vid_request.execute()
                video_data['video_id']=vid
                video_data['playlist_id']=pid
                try:
                    video_data['video_title']=vid_response['items'][0]['snippet']['title']
                except KeyError:
                    video_data['video_title']= "NaN"
                try:
                    video_data['video_description']=vid_response['items'][0]['snippet']['description']
                except KeyError:
                    video_data['video_description'] = 'NaN'
                try:
                    video_data['published']=vid_response['items'][0]['snippet']['publishedAt']
                except KeyError:
                    video_data['published']='NaN'
                try:
                    video_data['view_count']=vid_response['items'][0]['statistics']['viewCount']
                except KeyError:
                    video_data['view_count']='NaN'
                try:
                    video_data['like_count']=vid_response['items'][0]['statistics']['likeCount']
                except KeyError:
                    video_data['like_count']='NaN'
                try:
                    video_data['fovourite_count']=vid_response['items'][0]['statistics']['favoriteCount']
                except KeyError:
                    video_data['fovourite_count']='NaN'
                try:
                    video_data['comment_count']=vid_response['items'][0]['statistics']['commentCount']
                except KeyError:
                    video_data['comment_count']='NaN'
                try:
                    video_data['thumbnail']=vid_response['items'][0]['snippet']['thumbnails']['default']['url']
                except KeyError:
                    video_data['thumbnail']='NaN'
                try:
                    video_data['caption']=vid_response['items'][0]['contentDetails']['caption']
                except KeyError:
                    video_data['caption']='NaN'
                video_df=video_df.append(video_data,ignore_index=True)
#                 video_df = pd.concat([video_df,video_data])
                
        except IndexError as e:
            print('Completed Full iteration')
            
        while 'nextPageToken' in response:
            request = youtube.playlistItems().list(part="snippet,status,contentDetails",playlistId='PLG8IrydigQfd0Y_EN96Sgni8L37apMMR5',pageToken=response['nextPageToken'])
            response = request.execute()
            try:
                for i in range(0,5):
                    vid=response['items'][i]['snippet']['resourceId']['videoId']
                    vid_request = youtube.videos().list(part="snippet,contentDetails,statistics",id=vid)
                    vid_response = vid_request.execute()
                    video_data['video_id']=vid
                    video_data['playlist_id']=pid
                    try:
                        video_data['video_title']=vid_response['items'][0]['snippet']['title']
                    except KeyError:
                        video_data['video_title']= "NaN"
                    try:
                        video_data['video_description']=vid_response['items'][0]['snippet']['description']
                    except KeyError:
                        video_data['video_description'] = 'NaN'
                    try:
                        video_data['published']=vid_response['items'][0]['snippet']['publishedAt']
                    except KeyError:
                        video_data['published']='NaN'
                    try:
                        video_data['view_count']=vid_response['items'][0]['statistics']['viewCount']
                    except KeyError:
                        video_data['view_count']='NaN'
                    try:
                        video_data['like_count']=vid_response['items'][0]['statistics']['likeCount']
                    except KeyError:
                        video_data['like_count']='NaN'
                    try:
                        video_data['fovourite_count']=vid_response['items'][0]['statistics']['favoriteCount']
                    except KeyError:
                        video_data['fovourite_count']='NaN'
                    try:
                        video_data['comment_count']=vid_response['items'][0]['statistics']['commentCount']
                    except KeyError:
                        video_data['comment_count']='NaN'
                    try:
                        video_data['thumbnail']=vid_response['items'][0]['snippet']['thumbnails']['default']['url']
                    except KeyError:
                        video_data['thumbnail']='NaN'
                    try:
                        video_data['caption']=vid_response['items'][0]['contentDetails']['caption']
                    except KeyError:
                        video_data['caption']='NaN'
                    video_df=video_df.append(video_data,ignore_index=True)
                    
            except IndexError as e:
                print('Completed Full iteration')
    
    return video_df
# video_df = get_video(playlist=playlist_df)
   
##Fetching Comments
def get_comment(video_df):
    comment_data={}
    comment_df = pd.DataFrame(columns=['video_id','comment_text','authr_name','published_date','comment_id'])
    
    for vid in video_df['video_id']:
        try:
            youtube=build('youtube','v3',developerKey=api_key)
            request = youtube.commentThreads().list(part="snippet,replies",videoId=vid)
            response = request.execute()
            try:
                for i in range(0,20):
                    comment_data['video_id'] = vid
                    try:
                        comment_data['comment_text'] = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay']
                    except KeyError:
                        comment_data['comment_text']='NaN'
                    try:
                        comment_data['authr_name'] = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    except KeyError:
                        comment_data['authr_name']='NaN'   
                    try:
                        comment_data['published_date'] = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                    except KeyError:
                        comment_data['published_date'] = 'NaN'
                    try:
                        comment_data['comment_id'] = response['items'][i]['id']
                    except KeyError:
                        comment_data['comment_id'] = 'NaN'
                    
                    comment_df=comment_df.append(comment_data,ignore_index=True)
            except IndexError as e:
                print('Completed Full iteration')

            while 'nextPageToken' in response:
                request = youtube.commentThreads().list(part="snippet,replies",videoId=vid,pageToken=response['nextPageToken'])
                response = request.execute()
                try:
                    for j in range(0,20):
                        comment_data['video_id'] = vid
                        try:
                            comment_data['comment_text'] = response['items'][j]['snippet']['topLevelComment']['snippet']['textDisplay']
                        except KeyError:
                            comment_data['comment_text']='NaN'
                        try:
                            comment_data['authr_name'] = response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName']
                        except KeyError:
                            comment_data['authr_name']='NaN'   
                        try:
                            comment_data['published_date'] = response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt']
                        except KeyError:
                            comment_data['published_date'] = 'NaN'
                        try:
                            comment_data['comment_id'] = response['items'][j]['id']
                        except KeyError:
                            comment_data['comment_id'] = 'NaN'
                        comment_df=comment_df.append(comment_data,ignore_index=True)
                except IndexError as e:
                    print('Completed Full iteration')      
        
        except HttpError as e:
            error_message = e.content.decode("utf-8")
            pattern = r'\"reason\"\:\s*\"([^>]*?)\"'
            reason = re.findall(pattern,error_message)

            if reason[0] == "commentsDisabled":
                print("Comments are disabled for this video.")
            else:
                print("An error occurred:", error_message)
    return comment_df

# comments_df = get_comment(video_df)

