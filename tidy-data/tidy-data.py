from pymongo import MongoClient
from datetime import datetime, timezone

import csv

client = MongoClient() # Connect to locally running MongoDB instance
db = client.iot
observationsCol = db.observations

observations = []

# Obtain experiment data from the database
query = {'$or': [
    {'$and': [
        {'deviceId': 'photon_001'},
        {'timestamp': {'$gt': datetime(2017, 4, 30, 11, 0, tzinfo=timezone.utc)}},
        {'timestamp': {'$lt': datetime(2017, 7, 11, 10, 0, tzinfo=timezone.utc)}}]},
    {'$and': [
        {'deviceId': 'photon_002'},
        {'timestamp': {'$gt': datetime(2017, 4, 30, 11, 0, tzinfo=timezone.utc)}},
        {'timestamp': {'$lt': datetime(2017, 6, 5, 0, 0, tzinfo=timezone.utc)}}
    ]}
]}
for observation in observationsCol.find(query):
    del observation['_id']
    observations.append(observation)

bat_obs = [obs for obs in observations if obs['type'] == 'battery']

right_bat_obs = []
wrong_bat_obs = []
for obs in bat_obs:
    obs['value'] = obs['value'] * 100
    if obs['deviceId'] == 'photon_002' and obs['timestamp'] > datetime(2017, 5, 24, 4, 0): # Total discharge timestamp
        wrong_bat_obs.append(obs)
    else:
        right_bat_obs.append(obs)

def day_from_obs(obs):
    return obs['timestamp'].timetuple().tm_yday
    

# Align wrong data towards 0 instead of towards 100 (deafault battery value after boot)
daily_bat_obs = [[]]
first_day = day_from_obs(wrong_bat_obs[0])
current_day = first_day
for obs in sorted(wrong_bat_obs, key=lambda x: x['timestamp']):
    day = day_from_obs(obs)
    if day == current_day:
        daily_bat_obs[current_day - first_day].append(obs)
    else:
        current_day = day
        daily_bat_obs.append([obs])

fixed_battery_obs = []
for daily_obs in daily_bat_obs:
    daily_min = min(daily_obs, key=lambda x: x['value'])['value'])
    for obs in daily_obs:
        obs['value'] = obs['value'] - daily_min
        fixed_battery_obs.append(obs)

bat_obs = right_bat_obs + fixed_battery_obs
without_bat_obs = [obs for obs in observations if obs['type'] != 'battery']
clean_obs = sorted(without_bat_obs + bat_obs, key=lambda obs: obs['timestamp'].timestamp())

# Write tidy data to exp-data.csv file
with open('exp-data.csv', 'w', newline='', encoding='utf-8') as csv_file:
    ts = 'timestamp'
    d_id = 'device_id'
    typ = 'type'
    val = 'value'
    field_names = [ts, d_id, typ, val]
    csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
    csv_writer.writeheader()
    for obs in clean_obs:
        csv_writer.writerow({ts: int(obs[ts].timestamp()), d_id: obs['deviceId'], typ: obs[typ], val: obs[val]})