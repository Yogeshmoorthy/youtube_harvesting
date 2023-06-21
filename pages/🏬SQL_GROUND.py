import streamlit as st
import database

st.write('SQL PlayGround')

databases=database.fetch_database()

options = st.multiselect(
    'Select the Database you want to insert into SQL',
    databases)

# st.write('You selected:', options)

df={'channel_title':['MontyPython'],'channel_id':['UCGm3CO6LPcN-Y7HIuyE0Rew'],'channel_description':['For 7 years you YouTubers have been ripping us off, taking tens of thousands of our videos and putting them on YouTube.  Now the tables are turned']}

conn = st.experimental_connection("sql")
df = conn.query("select * from pet_owners")
st.dataframe(df)