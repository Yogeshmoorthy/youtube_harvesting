import streamlit as st
import pandas as pd
import numpy as np
import design
import database


api_key='AIzaSyAAgRZSfn5osPVOWWpYkFC1Asp_y1-lsco'

channel_name=''
channel_id = ''
global channel_df

st.title('Scrape the YOUTUBE Data')
channel_name = st.text_input('Enter the Youtube channel',placeholder='youtube')

if "button_clicked" not in st.session_state:
    st.session_state.button_clicked = False

button_clicked = st.button("Click me")
if button_clicked:
    # Update session state
    st.session_state.button_clicked = True

if st.session_state.button_clicked:
    if channel_name != '':
        channel_df = design.get_channel(api_key=api_key, channel_name = channel_name)
        st.header('CHANNEL DATA')
        st.write(channel_df)
        try:
            channel_id = channel_df.iloc[0,1]
        except IndexError:
            st.info("Sorry! Couldn't find the Channel you are looking for! Please try some other")

    if channel_id != '':
        playlist_df = design.get_playlist(api_key = api_key, id = channel_id)
        st.header('PLAYLIST DATA')
        st.write(playlist_df)

        video_df = design.get_video(playlist_df=playlist_df)
        st.header('VIDEO DATA')
        st.write(video_df)

        comments_df = design.get_comment(video_df=video_df)
        st.header('COMMENTS DATA')
        st.write(comments_df)

        clicked = st.button('Insert into MongoDB')
        if clicked:
            database.insert_data(database_name=channel_name, collection_name='channel', dataframe=channel_df)
            database.insert_data(database_name=channel_name, collection_name='playlist', dataframe=playlist_df)
            database.insert_data(database_name=channel_name,collection_name='video', dataframe=video_df)
            database.insert_data(database_name=channel_name,collection_name='comment', dataframe=comments_df)        
            st.success('Data inserted successfully')
        
# else:
#     st.write("Button is not clicked.")


