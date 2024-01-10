from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection

def Api_connect():
    Api_Id="AIzaSyARU83mtGmJ4diggZkxXOaMTaDhY5PTCYg"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
)
    response=request.execute()

    for i in response['items']:
         data=dict(Channel_Name=i["snippet"]["title"],
                 Channel_Id=i["id"],
                 Subscribers=i['statistics']['subscriberCount'],
                 Views=i["statistics"]["viewCount"],
                 Total_Videos=i["statistics"]["videoCount"],
                 Channel_Description=i["snippet"]["description"],
                 Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
         return data
    

    #get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response= youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True: 
      response=youtube.playlistItems().list(
                                          part='snippet',
                                          playlistId=Playlist_Id,
                                          maxResults=50,
                                          pageToken=next_page_token).execute()
    
      for i in range(len(response['items'])):
                  video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token=response.get('nextPageToken')

      if next_page_token is None:
                   break
    return video_ids

#get video information
def get_video_info(Video_Ids):
        video_data=[]
        for video_id in Video_Ids:
           request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
           )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                  Channel_Id=item['snippet']['channelId'],
                  Video_Id=item['id'],
                  Title=item['snippet']['title'],
                  Tags=item['snippet'].get('tags'),
                  Description=item['snippet'].get('description'),
                  Published_Date=item['snippet']['publishedAt'],
                  Duration=item['contentDetails']['duration'],
                  Views=item['statistics'].get('viewCount'),
                  Likes=item['statistics'].get('likeCount'),
                  Comments=item['statistics'].get('commentCount'),
                  Favorite_Count=item['statistics']['favoriteCount'],
                  Definition=item['contentDetails']['definition'],
                  Caption_Status=item['contentDetails']['caption']
                  )
            video_data.append(data)
        return video_data

#get comment information
def get_comment_info(Videos_Ids):
     Comment_data=[]
     try:
        for video_id in Videos_Ids:
             request=youtube.commentThreads().list(
                 part="snippet",
                 videoId=video_id ,
                maxResults=50
            )
        response=request.execute()

        for item in response['items']:
             data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                  Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                  Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                  Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
             Comment_data.append(data)
     except:
          pass
     return Comment_data


#upload to mongoDB
client=pymongo.MongoClient("mongodb+srv://Nethra:TlKBjLEAeFKtNQUJ@cluster0.eq1776g.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

def channels_details(channel_id):
     ch_details=get_channel_info(channel_id)
     vi_ids= get_videos_ids(channel_id)
     vi_details= get_video_info(vi_ids)
     com_details=get_comment_info(vi_ids)

     Coll1=db["channel_details"]
     Coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,"comment_information":com_details})
    
     return"upload completed successfully"

#Table creation for channels,videos,comments
def channels_table():
     mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="Nethra",
                      database="youtube_data",
                      port="5432")
     cursor=mydb.cursor()

     drop_query='''drop table if exists channels'''
     cursor.execute(drop_query)
     mydb.commit()
   
     try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),Channel_Id varchar(80) primary key,Subscribers bigint,Views bigint,Total_Videos int,Channel_Description text)'''
        cursor.execute(create_query)
        mydb.commit()

     except:
        print("Channels table already created")
    
        ch_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
             ch_list.append(ch_data["channel_information"])
        df=pd.DataFrame(ch_list)

        for index,row in df.iterrows():
                 insert_query='''insert into channels(Channel_Name,Channel_Id,Subscribers,Views,Total_Videos,Channel_Description)
                values(%s,%s,%s,%s,%s,%s)'''
        
        
        values=(row['Channel_Name'],
            row['Channel_Id'],
            row['Subscribers'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'])
        try:
           cursor.execute(insert_query,values)
           mydb.commit()

        except:
           print("Channel values are already inserted")

def videos_table():
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Nethra",
                                database="youtube_data",
                                port="5432")
        cursor=mydb.cursor()
            
        drop_query='''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists videos(Channel_Name  varchar(100),Channel_Id varchar(100),
                                Video_Id varchar(30) primary key,
                                Title varchar(150),
                                Tags text,
                                Description text,
                                Published_Date timestamp,
                                Duration interval,
                                Views bigint,
                                Likes bigint,
                                Comments int,
                                Favorite_Count int,
                                Definition varchar(10),
                                Caption_Status varchar(50)
                                )'''
        cursor.execute(create_query)
        mydb.commit()

        vi_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
              vi_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_list)
        for index,row in df2.iterrows():
                 insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status 
                                                )
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                 values=(row['Channel_Name'],
                                    row['Channel_Id'],
                                    row['Video_Id'],
                                    row['Title'],
                                    row['Tags'],
                                    row['Description'],
                                    row['Published_Date'],
                                    row['Duration'],
                                    row['Views'],
                                    row['Likes'],
                                    row['Comments'],
                                    row['Favorite_Count'],
                                    row['Definition'],
                                    row['Caption_Status']
                                    )
                 cursor.execute(insert_query,values)
                 mydb.commit()

def comments_tables():
            mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Nethra",
                            database="youtube_data",
                            port="5432")
            cursor=mydb.cursor()
            
            
            drop_query='''drop table if exists comments'''
            cursor.execute(drop_query)
            mydb.commit()


            create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                            Video_Id varchar(50),
                            Comment_Text text,
                            Comment_Author varchar(150),
                            Comment_Published timestamp
                            )'''

            cursor.execute(create_query)
            mydb.commit()
            com_list=[]
            db=client["Youtube_data"]
            coll1=db["channel_details"]
            for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                 com_list.append(com_data["comment_information"][i])
            df3=pd.DataFrame(com_list)

            for index,row in df3.iterrows():
                           insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''

            values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )

            cursor.execute(insert_query,values)
            mydb.commit()
def tables():
    channels_table()
    videos_table()
    comments_tables()

    return"Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
        df2=st.dataframe(vi_list)

    return df2

def show_comments_tables():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(com_data["comment_information"])):
                 com_list.append(com_data["comment_information"][i])
    df3=pd.dataframe(com_list)

    return df3


#streamlit part

with st.sidebar:
     st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
     st.header("Digital Analytics")
     st.caption("Data Collection")
     st.caption("Data Entry in Mongodb")
     st.caption("Data Migration To SQL")
     st.caption("Data Analysis")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}): 
         ch_ids.append(ch_data["channels_information"]["Channel_Id"]) 

if channel_id in ch_ids:
         st.success("Channel Details of the given channel id already exists")

else:
        insert= channels_details(channel_id)
        st.success(insert) 

if st.button("Migrate to Sql"):
             Table=tables()
             st.success(Table)
             
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
  show_channels_table()

elif show_table=="VIDEOS":
  show_videos_table()

elif show_table=="COMMENTS":
   show_comments_tables()  

   #SQL Connection

mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Nethra",
                        database="youtube_data",
                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                      "2. Which channels have the most number of videos, and how many videos do they have?",
                      "3. What are the top 10 most viewed videos and their respective channels?",
                      "4. How many comments were made on each video, and what are their corresponding video names?",
                      "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                      "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                      "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                      "8. What are the names of all the channels that have published videos in the year  2022?",
                      "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                      "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

         
