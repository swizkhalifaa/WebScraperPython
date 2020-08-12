import numpy as np # linear algebra 
import pandas as pd # data processing 
import pymongo
from pymongo import MongoClient
import requests
import json 
import csv
from bs4 import BeautifulSoup
from pathlib import Path


# Input league and year
leaguerequest = input("1. La Liga \n2. EPL \n3. Bundesliga \n4. Serie A \n5. Ligue 1 \n6. RFPL\n\nChoose League: ") 
seasonrequest = input("1. 2014 \n2. 2015 \n3. 2016 \n4. 2017 \n5. 2018\n\nChoose Season: ")
# URL customization
base_url = 'https://understat.com/league' 
leagues = ['La_liga', 'EPL', 'Bundesliga', 'Serie_A', 'Ligue_1', 'RFPL'] 
seasons = ['2014', '2015', '2016', '2017', '2018']

# Get URL & html
url = base_url+'/'+leagues[int(leaguerequest)-1]+'/'+seasons[int(seasonrequest)-1] 
res = requests.get(url) 
soup = BeautifulSoup(res.content, "lxml")
scripts = soup.find_all('script')

string_with_json_obj = '' 

# Find data for teams 
for el in scripts: 
    if 'teamsData' in el.text: 
        string_with_json_obj = el.text.strip()

# Strip unnecessary symbols and get only JSON data 
ind_start = string_with_json_obj.index("('")+2 
ind_end = string_with_json_obj.index("')") 
json_data = string_with_json_obj[ind_start:ind_end] 

json_data = json_data.encode('utf8').decode('unicode_escape')

# Get teams and their relevant ids and put them into separate dictionary
data = json.loads(json_data) 
teams = {} 
for id in data.keys(): 
  teams[id] = data[id]['title']
  
# Check the sample of values per each column values = [] 
for id in data.keys(): 
    columns = list(data[id]['history'][0].keys()) 
    values = list(data[id]['history'][0].values()) 
    break

# Getting data for all teams 
dataframes = {} 
for id, team in teams.items(): 
    teams_data = []    
    for row in data[id]['history']:
        teams_data.append(list(row.values())) 
    df = pd.DataFrame(teams_data, columns=columns) 
    dataframes[team] = df 

# Transforming ppda and oppda metrics to coefficient values
    for team, df in dataframes.items(): 
        dataframes[team]['ppda_coef'] = dataframes[team]['ppda'].apply(lambda x: x['att']/x['def'] if x['def'] != 0 else 0)
        dataframes[team]['oppda_coef'] = dataframes[team]['ppda_allowed'].apply(lambda x: x['att']/x['def'] if x['def'] != 0 else 0)
        
# Splitting metrics into sum and mean groups for team totals
    cols_to_sum = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored', 'missed', 'xpts', 'wins', 'draws', 'loses', 'pts', 'npxGD'] 
    cols_to_mean = ['ppda_coef', 'oppda_coef']

# Calculating the total sum and mean of metrics and transposing them into a dataframe inside a list
    frames = [] 
    for team, df in dataframes.items(): 
        sum_data = pd.DataFrame(df[cols_to_sum].sum()).transpose()
        mean_data = pd.DataFrame(df[cols_to_mean].mean()).transpose()
        final_df = sum_data.join(mean_data) 
        final_df['team'] = team
        final_df['matches'] = len(df) 
        frames.append(final_df) 
# Put in the final full_stat dataframe      
    full_stat = pd.concat(frames)

# Ordering columns by expected goals
    full_stat = full_stat[['team', 'matches', 'wins', 'draws', 'loses', 'scored', 'missed', 'pts', 'xG', 'npxG', 'xGA', 'npxGA', 'npxGD', 'ppda_coef', 'oppda_coef', 'deep', 'deep_allowed', 'xpts']]
    full_stat.sort_values('xG', ascending=False, inplace=True)
    full_stat.reset_index(inplace=True, drop=True)

# Extracting features to provide non-redundant/universal metrics 
    full_stat['position'] = range(1,len(full_stat)+1)
    full_stat['season'] = seasons[int(seasonrequest)-1]
    full_stat['gpgA'] = full_stat['scored']/full_stat['matches']
    full_stat['gapgA'] = full_stat['missed']/full_stat['matches']
    full_stat['ppgA'] = full_stat['pts']/full_stat['matches']
    full_stat['deepA'] = full_stat['deep']/full_stat['matches']
    full_stat['deep_allowedA'] = full_stat['deep_allowed']/full_stat['matches']
    
# Extracting difference values for other metrics
    full_stat['xG_diff'] = full_stat['xG'] - full_stat['scored'] 
    full_stat['xGA_diff'] = full_stat['xGA'] - full_stat['missed'] 
    full_stat['xpts_diff'] = full_stat['xpts'] - full_stat['pts']
    
    
# Setting the dataframes final view for exportion   
    col_order = ['season','position','team', 'gpgA', 'gapgA', 'ppgA', 'xG', 'xG_diff', 'npxG', 'xGA', 'xGA_diff', 'npxGA', 'npxGD', 'ppda_coef', 'oppda_coef', 'deepA', 'deep_allowedA', 'xpts', 'xpts_diff'] 
    full_stat = full_stat[col_order] 
    full_stat.columns = ['season', 'position', 'team', 'gPG', 'gaPG', 'ptsPG', 'xG', 'xG_diff', 'NPxG', 'xGA', 'xGA_diff', 'NPxGA', 'NPxGD', 'PPDA', 'OPPDA', 'DC', 'ODC', 'xPTS', 'xPTS_diff'] 
    pd.options.display.float_format = '{:,.2f}'.format 
    
# Converting dataframe to csv format and exporting it
    full_stat.to_csv (r'C:\Users\Darren\Desktop\export_dataframe.csv', index = False, header=True)

# Connecting to mongoDB and creating a collection  
client = pymongo.MongoClient("mongodb+srv://darren:dembaba19@clusterairlinenodes-c6yeq.mongodb.net/test?retryWrites=true&w=majority")
db = client.FootballBase
collection = db['Stats']

# Converting csv file to dictionary and inserting records into mongoDB
reader = pd.read_csv("export_dataframe.csv")
records_ = reader.to_dict(orient = 'records')
result = db.collection.insert_many(records_)
client.close()

print("Data successfully scraped!")
