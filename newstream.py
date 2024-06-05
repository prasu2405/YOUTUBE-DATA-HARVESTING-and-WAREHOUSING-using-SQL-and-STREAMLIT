#APi libraries
from googleapiclient.discovery import build
import json


import re

 #sql libraires
import mysql.connector
from googleapiclient.errors import HttpError
import pymysql
import pandas as pd
import datetime
from datetime import datetime


# [STREAMLIT libraries]
from streamlit_option_menu import option_menu
import streamlit as st
from sqlalchemy import create_engine
import plotly.express as px


api_key="AIzaSyDlnMV3QZ9YrP-9wmlTYE3g8416W7Mwh3w"
youtube=build("youtube","v3",developerKey=api_key)

#execution of channel data

def channel_details(channel_id):
    channel_data=[]

    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",   
    id=channel_id)
    response = request.execute()


    if 'items' in response and len(response['items']) > 0:
        item = response['items'][0]
        data = {
                'channel_id': channel_id,
                'channel_name' : item['snippet']['title'],
                'channel_des': item['snippet']['description'],
                'channel_pid': item['contentDetails']['relatedPlaylists']['uploads'],
                'channel_viewcount': item['statistics']['viewCount'],
                'channel_subcount': item['statistics']['subscriberCount'],
                'channel_vc': item['statistics']['videoCount']
                    }
        channel_data.append(data)
    return channel_data
    
def to_seconds(duration): #eg P1W2DT6H21M32S
    week = 0
    day  = 0
    hour = 0
    min  = 0
    sec  = 0

    duration = duration.lower()

    value = ''
    for c in duration:
        if c.isdigit():
            value += c
            continue

        elif c == 'p':
            pass
        elif c == 't':
            pass
        elif c == 'w':
            week = int(value) * 604800
        elif c == 'd':
            day = int(value)  * 86400
        elif c == 'h':
            hour = int(value) * 3600
        elif c == 'm':
            min = int(value)  * 60
        elif c == 's':
            sec = int(value)

        value = ''

    return week + day + hour + min + sec

def get_videos_ids(channel_id):
    video_ids=[]
    try:           
        response=youtube.channels().list(id=channel_id,part='contentDetails').execute()
                    
                    
        if 'items' in response:
            playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token = None

        while True  :
            response1=youtube.playlistItems().list(
                                                    part="snippet",
                                                    playlistId=playlist_id,
                                                    maxResults=50,
                                                    pageToken=next_page_token).execute()
            if 'items' in response1:
                for i in range(len(response1['items'])):
                    video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token = response1.get('nextPageToken')

                if next_page_token is None:
                    break
    except:
         pass
    return video_ids

        # FUNCTION TO GET VIDEO DETAILS


def get_video_info(video_ids):#video_ids is equal to playlists_id
    video_data=[]
    for video_id in video_ids:

        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()


        for item in response['items']:

            data= {
                'channel_name': item['snippet']['channelTitle'],
                'video_name': item['snippet']['title'],
                'video_id': item['id'],
                'video_desc': item['snippet']['description'],
                'pub_at':conversion(item['snippet']['publishedAt']),
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'dislike_count': int(item['statistics'].get('dislikeCount', 0)),
                'fav_count': int(item['statistics'].get('favoriteCount', 0)),
                'comm_count': int(item['statistics'].get('commentCount', 0)),
                'duration':int(to_seconds(item['contentDetails'].get('duration'))),
                'thumbnail_url': item['snippet']['thumbnails']['default']['url'],
                'caption_status': item['contentDetails'].get('caption', 'false')
            }

            video_data.append(data)

    return video_data




        #**GET COMMENT INFORMATION**

def conversion(input_string):
    pattern = r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z' # Define regular expression pattern
    match = re.match(pattern, input_string)                 # Match the pattern
    if match:
        year, month, day, hour, minute, second = match.groups()  # Extract matched groups
        dt_obj = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))# Convert to datetime object
        return dt_obj
    else:
        print("Invalid datetime string format.")
    return None

def get_comment_info(video_ids):
    comment_data=[]
    

    try:
        for video_com in video_ids:
            request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_com,
                    maxResults=50
                )
            response=request.execute()


            for item in response['items']:
                data={
                        'comment_Id': item['snippet']['topLevelComment']['id'],
                        'video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                        'comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment_publishedAt':conversion(item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    }
                comment_data.append(data)
    except:
         pass
    return comment_data


#GET CHANNEL DATA-SQL 
myconnection = pymysql.connect(host="localhost", user='root', password='1234', database='youtube_harvesting')
mycursor = myconnection.cursor()


mycursor.execute("create database if not exists youtube_harvesting")
mycursor.execute("use youtube_harvesting")

# create table of channel_Id

def channel_inform(channel_id):
    mycursor.execute("""create table if not exists youtube_channels(
        channel_id varchar(255) primary key,
        channel_name varchar(255),
        channel_des TEXT,
        channel_pid varchar(255),
        channel_viewcount BIGINT,
        channel_subcount BIGINT,
        channel_vc int)
        """)
    
    channel_data=channel_details(channel_id)
    channel_df=pd.DataFrame(channel_data)
    for index,row in channel_df.iterrows():
        sql= """insert ignore into youtube_channels(channel_id,channel_name,channel_des,channel_pid,channel_viewcount,channel_subcount,channel_vc)values(%s,%s,%s,%s,%s,%s,%s)"""
        val = (
                row['channel_id'], 
                row['channel_name'], 
                row['channel_des'], 
                row['channel_pid'], 
                row['channel_viewcount'], 
                row['channel_subcount'], 
                row['channel_vc']
                )
        mycursor.execute(sql,val)
        myconnection.commit()

                #GET VIDEO DETAILS_SQL

def get_video_inform(video_ids):
    mycursor.execute("use youtube_harvesting")
    mycursor.execute("""create table if not exists youtube_videos(
                                        channel_name varchar(255),
                                        video_name varchar(255),
                                        video_id VARCHAR(255) PRIMARY KEY,
                                        video_desc TEXT,
                                        pub_at DATETIME,
                                        view_count INT,
                                        like_count INT,
                                        dislike_count INT,
                                        fav_count INT,
                                        comm_count INT,
                                        duration VARCHAR(255),
                                        thumbnail_url TEXT,
                                        caption_status VARCHAR(255))""") 


    video_data=get_video_info(video_ids)
    video_df=pd.DataFrame(video_data)
    for index,row in video_df.iterrows():
            sql="""insert ignore into youtube_videos(channel_name,video_name,video_id,video_desc,pub_at,view_count,like_count,dislike_count,fav_count,comm_count,duration,thumbnail_url,caption_status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            val = (
                row['channel_name'], 
                row['video_name'], 
                row['video_id'], 
                row['video_desc'], 
                row['pub_at'], 
                row['view_count'], 
                row['like_count'], 
                row['dislike_count'], 
                row['fav_count'], 
                row['comm_count'], 
                row['duration'], 
                row['thumbnail_url'], 
                row['caption_status'])
            mycursor.execute(sql,val)
            myconnection.commit()

                #GET COMMENT DATA_SQL

def get_comment_inform(video_ids):
    mycursor.execute("use youtube_harvesting")
    mycursor.execute("""create table if not exists youtube_comments(comment_Id int primary key,
                                                video_id varchar(255),
                                                comment_text text,
                                                comment_author varchar(255),
                                                comment_publishedAt datetime)""")
    
    comment_data=get_comment_info(video_ids)
    comment_df=pd.DataFrame(comment_data)
    for index, row in comment_df.iterrows():
        sql="""insert ignore into youtube_comments(comment_Id,video_id,comment_text,comment_author,comment_publishedAt)values(%s,%s,%s,%s,%s)"""
        val=(row['comment_Id'],
            row['video_id'],
            row['comment_text'], 
            row['comment_author'],
            row['comment_publishedAt'])
        mycursor.execute(sql,val)
        myconnection.commit()










    
               







st.set_page_config(page_title="Youtube" , page_icon=":tada:", layout="wide")

#header section


st.subheader("Hi, i am prasanth :wave:")

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Data Zone","Analysis Zone","Query Zone"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "24px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}}) 
    #Home Tab
if selected == "Home":
    st.title(':Green[YOUTUBE DATA HARVESTING and WAREHOUSING using SQL and STREAMLIT]')
    st.markdown("## :violet[Domain] : Social Media")
    st.markdown("## :violet[Skills take away From This Project] : Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL")
    st.markdown("## :violet[Overall view] : Building a easiest way to access the details and retrieving data from YouTube API, storing and query the data using SQL as a Warehouse  and displaying the data in the Streamlit app.")
    
    #Data Zone
elif selected == "Data Zone":
    tab1,tab2 = st.tabs(["$\huge COLLECT $", "$\huge MIGRATE $"])
    
    #collect tab
    with tab1:
        st.markdown('## :Grey[Data collection zone]')
        st.write(
            '(**collects data** by using channel id and **stores it in the :Red[SQL] database**.)')
        channel_id = st.text_input('**Enter the channel_id**')
        st.write('''click below to retrieve and store data.''')
        Get_data = st.button('**execute**')

        if "Get_state" not in st.session_state:
            st.session_state.Get_state = False
        if Get_data or st.session_state.Get_state:
            st.session_state.Get_state = True
            
                        
            c_det=channel_details(channel_id)
            channel_data=pd.DataFrame(c_det)

            video_ids=get_videos_ids(channel_id)



            v_det=get_video_info(video_ids)
            video_data=pd.DataFrame(v_det)


            c_det=get_comment_info(video_ids)
            comment_data=pd.DataFrame(c_det)



    with tab2:
        
        st.markdown('## :blue[Data Migration zone]')
        st.write('''( **Migrates channel data to :green[MYSQL] database**)''')

        st.write('''Click below for **Data migration**.''')
        Migrate = st.button('**Migrate to MySQL**')
        if 'migrate_sql' not in st.session_state:
            st.session_state_migrate_sql = False
        if Migrate or st.session_state_migrate_sql:
            st.session_state_migrate_sql = True

        #myconnection = pymysql.connect(host="localhost",user='root',password='1234')

        #mycursor=myconnection.cursor()

            ch_data=channel_inform(channel_id)
            vi_data=get_video_inform(video_ids)
            comt_data=get_comment_inform(video_ids)

            Migrate=st.button('**Success**')




#Analysis Zone                
if selected == "Analysis Zone":
    st.header(':blue[Channel Data Analysis zone]')
    st.write(
        '''(Checks for available channels by clicking this checkbox)''')
    # Check available channel data
    Check_channel = st.checkbox('**Check available channel data for analysis**')
    if Check_channel:
        # Create database connection
        engine = create_engine("mysql+pymysql://root:1234@127.0.0.1:3306/youtube_harvesting")
                    # Execute SQL query to retrieve channel names
        query = "SELECT channel_name FROM youtube_channels;"
        results = pd.read_sql(query,engine)
            # Get channel names as a list
        channel_names_fromsql = list(results['channel_name'])
            # Create a DataFrame from the list and reset the index to start from 1
        sql_df = pd.DataFrame(channel_names_fromsql,columns=['Available channel data']).reset_index(drop=True)
            # Reset index to start from 1 instead of 0
        sql_df.drop_duplicates(inplace=True)
        sql_df.index += 1
            # Show dataframe
        st.dataframe(sql_df)





# QUERY ZONE
if selected == "Query Zone":
    st.subheader(':blue[Queries and Results ]')
    st.write('''(Queries were answered based on :orange[**Channel Data analysis**] )''')
    
    # Selectbox creation
    question_tosql = st.selectbox('Select your Question]',
                                ('1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                key='collection_question')
    
# Create a connection to SQL
    connect_for_question = pymysql.connect(host="127.0.0.1", user="root", passwd="1234", port=3306, db="youtube_harvesting")
    cursor = connect_for_question.cursor()
    engine = create_engine("mysql+pymysql://root:1234@127.0.0.1:3306/youtube_harvesting")

        # Q1
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute('''SELECT youtube_videos.channel_name,youtube_videos.video_name FROM youtube_videos ''')
        result_1 = cursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

        # Q2
    elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':

        col1, col2 = st.columns(2)
        with col1:
            cursor.execute("SELECT youtube_channels.channel_name, youtube_channels.channel_vc FROM youtube_channels ORDER BY channel_vc DESC;")
            result_2 = cursor.fetchall()
            df2 = pd.DataFrame(result_2, columns=['channel Name', 'Video Count']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)

        with col2:
            fig_vc = px.bar(df2, y='Video Count', x='channel Name', text_auto='.2s', title="Most number of videos", )
            fig_vc.update_traces(textfont_size=16, marker_color='#E6064A')
            fig_vc.update_layout(title_font_color='#1308C2 ', title_font=dict(size=25))
            st.plotly_chart(fig_vc, use_container_width=True)

        # Q3
    elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':

        col1, col2 = st.columns(2)
        with col1:
            cursor.execute(
                "SELECT youtube_videos.video_id, youtube_videos.view_count, youtube_videos.channel_name FROM youtube_videos ORDER BY youtube_videos.view_count DESC LIMIT 10;")
            result_3 = cursor.fetchall()
            df3 = pd.DataFrame(result_3, columns= ['video_name', 'view_count','channel_name']).reset_index(drop=True)
            df3.index += 1
            st.dataframe(df3)

        with col2:
            fig_topvc = px.bar(df3, y='view_count', x='video_name', text_auto='.2s', title="Top 10 most viewed videos")
            fig_topvc.update_traces(textfont_size=16, marker_color='#E6064A')
            fig_topvc.update_layout(title_font_color='#1308C2 ', title_font=dict(size=25))
            st.plotly_chart(fig_topvc, use_container_width=True)

        # Q4
    elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
            cursor.execute(
                "SELECT youtube_videos.video_name, youtube_videos.comm_count FROM youtube_videos;")
            result_4 = cursor.fetchall()
            df4 = pd.DataFrame(result_4, columns=['video_name', 'comm_count']).reset_index(drop=True)
            df4.index += 1
            st.dataframe(df4)

        # Q5
    elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            cursor.execute(
                "SELECT youtube_videos.channel_name, youtube_videos.video_id, youtube_videos.like_count FROM youtube_videos ORDER BY youtube_videos.like_count DESC;")
            result_5 = cursor.fetchall()
            df5 = pd.DataFrame(result_5, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
            df5.index += 1
            st.dataframe(df5)

        # Q6
    elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
            cursor.execute(
                "SELECT youtube_videos.channel_name, youtube_videos.video_id, youtube_videos.like_count FROM youtube_videos ORDER BY youtube_videos.like_count DESC;")
            result_6 = cursor.fetchall()
            df6 = pd.DataFrame(result_6, columns=['channel_name', 'video_name', 'like_count', ]).reset_index(drop=True)
            df6.index += 1
            st.dataframe(df6)

        # Q7
    elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

            col1, col2 = st.columns(2)
            with col1:
                cursor.execute("SELECT channel_name,channel_viewcount FROM youtube_channels ORDER BY channel_viewcount DESC;")
                result_7 = cursor.fetchall()
                df7 = pd.DataFrame(result_7, columns=['channel_name', 'channel_viewcount']).reset_index(drop=True)
                df7.index += 1
                st.dataframe(df7)

            with col2:
                fig_topview = px.bar(df7, y='channel_viewcount', x='channel_name', text_auto='.2s',
                                    title="channel_viewcount", )
                fig_topview.update_traces(textfont_size=16, marker_color='#E6064A')
                fig_topview.update_layout(title_font_color='#1308C2 ', title_font=dict(size=25))
                st.plotly_chart(fig_topview, use_container_width=True)

        # Q8
    elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
            cursor.execute('''
                SELECT youtube_videos.channel_name, youtube_videos.pub_at 
                        FROM youtube_videos WHERE EXTRACT(YEAR FROM pub_at) = 2022''')
            result_8 = cursor.fetchall()
            df8 = pd.DataFrame(result_8, columns=['channel_name','Year 2022 only']).reset_index(drop=True)
            df8.index += 1
            st.dataframe(df8)

        # Q9
    elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            cursor.execute('''
                SELECT youtube_videos.channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(youtube_videos.duration)))), '%H:%i:%s') AS duration  
                FROM youtube_videos GROUP by channel_name ORDER BY duration DESC ''')
            result_9 = cursor.fetchall()
            df9 = pd.DataFrame(result_9, columns=['channel_name', 'Average duration of youtube_videos (HH:MM:SS)']).reset_index(drop=True)
            df9.index += 1
            st.dataframe(df9)

        # # Q10
    elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            cursor.execute(
                "SELECT youtube_videos.channel_name, youtube_videos.video_id, youtube_videos.comm_count FROM youtube_videos ORDER BY youtube_videos.comm_count DESC;")
            result_10 = cursor.fetchall()
            df10 = pd.DataFrame(result_10, columns=['channel_name', 'video_name', 'comm_count']).reset_index(drop=True)
            df10.index += 1
            st.dataframe(df10)

        # SQL DB connection close
    connect_for_question.close()
