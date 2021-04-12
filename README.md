
# Project: LaLiga Players, Trophies and Twitter Followers
![alt text](https://github.com/FranLeston/W3-pipelines-project/blob/main/images/ratingsdis.png?raw=true)

## Overview
For my Ironhack week 3 project, I wanted to analyse current players from La Liga while also incorporating some 3rd party data. The data I have collected includes trophies won by the players over their careers and also twitter data such as number of followers to determine if the players success is correlated with the amount of followers they have.

Though not a requirement, I have included an initial setup main.py file which in addition to cleaning the data acquired, it will first create a Database Schema and populate it with information from 
https://www.api-football.com/ .  This is mainly what the main.py does when executed. <br/>

1. Connect to DB and create the schemas for 3 tables:<br/>
    a. Teams<br/>
    b. PLayers<br/>
    c. Trophies<br/>
    
    Here is a EER Diagram of th final SQL Arquitcture:<br/>
    ![alt text](https://github.com/FranLeston/W3-pipelines-project/blob/main/images/EER_Diagram.png?raw=true)

2. Using the Football API, I get all the Team, Player Stats and Trophies Information for LaLiga PLayers in the current 2020-2021 season. I then add this info to the DB. (Over 1,000 API calls, About    two minutes to finish!).

3. The next step was to gather twitter followers from the name of the football players in the database. I had to loop through each player name and subsequently loop throught the Twitter API results    for all the matched users and getting the user that had a Verified Status. In the end, this was very hard to do, since a lot of the names in the DB did not match twitter profile names. I opted to    instead use the Tweepy API to search for 11 known Football Player accounts and get their followers. The Players were: Messi, Pique, Sergio Ramos, Marcelo, Sergi Roberto, Umtiti, Luis Suarez,Jordi    Alba, Benzema and Toni Kroos. 

4. Once I recieved the follower counts for each player i added them in the database as well. 

5. Once all the data is the the DB, the script cleans the data and exports it to a .csv file in the data folder, which is then used by the jupyter notebook to visulaize the data. 

   You can view the Notebook without having to initialize the main.py file since all the data is in the csv file. 

## Installation

Before running main.py please sign up for an api-football.com api key and a Twitter Developer acoount. Also you must have MySQL installed on your machine. I included a sample .env file in the repo. <br/>

DB_USER=<br/>
DB_PASSWORD=<br/>

TWEEPY_CONSUMER_KEY=<br/>
TWEEPY_CONSUMER_SECRET=<br/>
TWEEPY_ACCESS_TOKEN=<br/>
TWEEPY_ACCESS_TOKEN_SECRET=<br/>

FOOTBALL_API_KEY=<br/>
FOOTBALL_API_URL=<br/>

### Requirements
I have included requirements.txt with all the needed libraries:<br/>

tweepy==3.10.0<br/>
SQLAlchemy==1.3.23<br/>
SQLAlchemy_Utils==0.36.8<br/>
PyMySQL==1.0.2<br/>
numpy==1.20.2<br/>
pandas==1.2.3<br/>
requests==2.22.0<br/>
python-dotenv==0.17.0<br/>

Run Python3 main.py after fulfilling all the requiments.


