__author__ = "Bayode Olorundare"
__copyright__ = "Copyright 2023, Motivate International Inc."
__credits__ = ["Google Data Analyst Capstone Project"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Bayode Olorundare"
__email__ = "polycraftdigital@gmail.com"
__status__ = "Production"


import pandas as pd 
from pandas.api.types import CategoricalDtype
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches



# Locate examples files
csv_file_1 = './data-files/Divvy_Trips_2019_Q2.csv'
csv_file_2 = './data-files/Divvy_Trips_2019_Q3.csv'
csv_file_3 = './data-files/Divvy_Trips_2019_Q4.csv'
csv_file_4 = './data-files/Divvy_Trips_2020_Q1.csv'

df_file_1 = pd.read_csv(csv_file_1)
df_file_2 = pd.read_csv(csv_file_2)
df_file_3 = pd.read_csv(csv_file_3)
df_file_4 = pd.read_csv(csv_file_4)

#List column names
print(df_file_1.columns)
print(df_file_2.columns)
print(df_file_3.columns)
print(df_file_4.columns)

# Define a dictionary with the old column names as keys and the new column names as values
df_file_1_names = {'01 - Rental Details Rental ID' : 'ride_id',
                   '01 - Rental Details Local Start Time' : 'started_at',
                   '01 - Rental Details Local End Time' : 'ended_at',
                   '01 - Rental Details Bike ID' : 'rideable_type',
                   '01 - Rental Details Duration In Seconds Uncapped' : 'tripduration',
                   '03 - Rental Start Station ID' : 'start_station_id',
                   '03 - Rental Start Station Name' : 'start_station_name',
                   '02 - Rental End Station ID' : 'end_station_id',
                   '02 - Rental End Station Name' : 'end_station_name',
                   'User Type' : 'member_casual',
                   'Member Gender' : 'gender',
                   '05 - Member Details Member Birthday Year' : 'birthyear'}
df_file_2_names = {'trip_id' : 'ride_id',
                   'start_time' : 'started_at',
                   'end_time' : 'ended_at',
                   'bikeid' : 'rideable_type',
                   'from_station_id' : 'start_station_id',
                   'from_station_name' : 'start_station_name',
                   'to_station_id' : 'end_station_id',
                   'to_station_name' : 'end_station_name',
                   'usertype' : 'member_casual'}
df_file_3_names = {'trip_id' : 'ride_id',
                   'start_time' : 'started_at',
                   'end_time' : 'ended_at',
                   'bikeid' : 'rideable_type',
                   'from_station_id' : 'start_station_id',
                   'from_station_name' : 'start_station_name',
                   'to_station_id' : 'end_station_id',
                   'to_station_name' : 'end_station_name',
                   'usertype' : 'member_casual'}

# Rename the columns using the dictionary
df_file_1.rename(columns=df_file_1_names, inplace = True)
df_file_2.rename(columns=df_file_2_names, inplace = True)
df_file_3.rename(columns=df_file_3_names, inplace = True)

# Drop the columns you want to remove
df_file_1.drop(['tripduration', 'gender', 'birthyear'], axis=1, inplace=True)
df_file_2.drop(['tripduration', 'gender', 'birthyear'], axis=1, inplace=True)
df_file_3.drop(['tripduration', 'gender', 'birthyear'], axis=1, inplace=True)
df_file_4.drop(['start_lat', 'start_lng', 'end_lat', 'end_lng'], axis=1, inplace=True)

#Confirm List column names after updates
print(df_file_1.columns)
print(df_file_2.columns)
print(df_file_3.columns)
print(df_file_4.columns)

# Reorder the columns of df_file_1, df_file_2 and df_file_3 to match df_file_4
df_file_1 = df_file_1.reindex(columns=df_file_4.columns)
df_file_2 = df_file_2.reindex(columns=df_file_4.columns)
df_file_3 = df_file_3.reindex(columns=df_file_4.columns)

# Concatenate the four DataFrames
df_all_trips = pd.concat([df_file_1, df_file_2, df_file_3, df_file_4])

# Print the resulting DataFrame
print(df_all_trips)

# Drop rows with null or empty values in any column
df_all_trips.dropna(inplace=True)

# Print the data types of each column
print(df_all_trips.dtypes)

# Convert columns to ensure they are consistent so that they can stack correctly
df_all_trips['ride_id'] = df_all_trips['ride_id'].astype(str)
df_all_trips['rideable_type'] = df_all_trips['rideable_type'].astype(str)
df_all_trips['started_at'] = pd.to_datetime(df_all_trips['started_at'])
df_all_trips['ended_at'] = pd.to_datetime(df_all_trips['ended_at'])
df_all_trips['start_station_name'] = df_all_trips['start_station_name'].astype(str)
df_all_trips['start_station_id'] = df_all_trips['start_station_id'].astype(int)
df_all_trips['end_station_name'] = df_all_trips['end_station_name'].astype(str)
df_all_trips['end_station_id'] = df_all_trips['end_station_id'].astype(int)
df_all_trips['member_casual'] = df_all_trips['member_casual'].astype(str)

# Print the data types of each column
print(df_all_trips.dtypes)

# Replace values in the "member_casual" column
df_all_trips['member_casual'].replace({'Subscriber': 'member', 'Customer': 'casual'}, inplace=True)

# count the values in the 'member_casual' column
counts = df_all_trips['member_casual'].value_counts()
print(counts)

# Add columns that list the date, month, day, and year of each ride
df_all_trips['day'] = df_all_trips['started_at'].dt.day
df_all_trips['hour'] = df_all_trips['started_at'].dt.hour
df_all_trips['month'] = df_all_trips['started_at'].dt.month
df_all_trips['year'] = df_all_trips['started_at'].dt.year
df_all_trips['day_of_week'] = df_all_trips['started_at'].dt.day_name()

# Add a "ride_length" calculation to all_trips (in seconds)
df_all_trips['ride_length'] = (df_all_trips['ended_at'] - df_all_trips['started_at']).dt.total_seconds()

# Filter the negative values in 'ride_length'
df_all_trips = df_all_trips[df_all_trips['ride_length'] > 0]

print(df_all_trips[df_all_trips['ride_length'] > 0])

# Filter out starting station name
df_all_trips = df_all_trips[df_all_trips['start_station_name'] != 'HQ QR']

print(df_all_trips[df_all_trips['start_station_name'] == 'HQ QR'])

# Calulcate a summary of for ride length (count, mean, std, min, max, mode)
summary = df_all_trips['ride_length'].describe()
mode = df_all_trips['ride_length'].mode()

# print filtered rows
print(summary, mode)

# Calculate mean ride length by member_casual
mean_ride_length = df_all_trips.groupby('member_casual')['ride_length'].mean()

# Calculate median ride length by member_casual
median_ride_length = df_all_trips.groupby('member_casual')['ride_length'].median()

# Calculate maximum ride length by member_casual
max_ride_length = df_all_trips.groupby('member_casual')['ride_length'].max()

# Calculate minimum ride length by member_casual
min_ride_length = df_all_trips.groupby('member_casual')['ride_length'].min()

# Define the order of the days of the week
weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Create a categorical variable with the defined order
cat_weekdays = pd.Categorical(df_all_trips['day_of_week'], categories=weekdays_order, ordered=True)

# Group by member_casual and the categorical weekdays variable and calculate the mean ride length
mean_ride_length_day = df_all_trips.groupby(['member_casual', cat_weekdays])['ride_length'].mean()

# Now, let's run the average ride time by each day for members vs casual users
mean_ride_length_ride_time = df_all_trips.groupby(['member_casual', 'day_of_week'])['ride_length'].mean().reset_index()

# create a weekday categorical type with ordered days
weekday_cats = CategoricalDtype(categories= weekdays_order, ordered=True)

########################################################
#Total Number of Rides of Member & Casual During Weekday
########################################################
# create a new dataframe with counts of each weekday and member type
grouped_data = df_all_trips.groupby(['member_casual', 'day_of_week']).size().reset_index(name='count')

# create a sample dataframe
data = grouped_data
df = pd.DataFrame(data)

# define colors for member and casual categories
colors = {'member': 'blue', 'casual': 'Salmon'}

# create a list of weekdays in the desired order
weekdays_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

# convert the day_of_week column to a categorical data type with the specified order
df['day_of_week'] = pd.Categorical(df['day_of_week'], categories=weekdays_order, ordered=True)

# create a pivot table with day_of_week as index, member_casual as columns, and count as values
pivot_df = df.pivot_table(index='day_of_week', columns='member_casual', values='count')

# create the plot
ax = pivot_df.plot(kind='bar', figsize=(10,6), width=0.8)

# set the title, axes labels, and legend
ax.set_title('Total Number of Rides of Member & Casual During Weekday', fontsize=15)
ax.set_xlabel('Weekday', fontsize=12)
ax.set_ylabel('Number of Rides', fontsize=12)

# set the colors for each group
handles = []
for i, bars in enumerate(ax.containers):
    for bar in bars:
        bar.set_color(colors[pivot_df.columns[i]])
    # create a custom legend handle for each group
    handle = mpatches.Patch(color=colors[pivot_df.columns[i]], label=pivot_df.columns[i])
    handles.append(handle)

# add the custom legend handles to the legend
ax.legend(handles=handles, fontsize=12, title='Member/Casual', title_fontsize=14)

# adjust the plot layout and show the plot
plt.subplots_adjust(bottom=0.2)
plt.figtext(0.5, 0.02, 'Data from 2019-04 to 2020-03', ha='center', fontsize=12)
plt.show()

########################################################
#Average Duration of Riders During Weekday
########################################################

# create a new dataframe with counts of each weekday and member type
grouped_data = df_all_trips.groupby(['member_casual', 'day_of_week'])['ride_length'].mean().reset_index(name='avg_duration')

# create a sample dataframe
data = grouped_data
df = pd.DataFrame(data)

# define colors for member and casual categories
colors = {'member': 'blue', 'casual': 'Salmon'}

# create a list of weekdays in the desired order
weekdays_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

# convert the day_of_week column to a categorical data type with the specified order
df['day_of_week'] = pd.Categorical(df['day_of_week'], categories=weekdays_order, ordered=True)

# create a pivot table with day_of_week as index, member_casual as columns, and count as values
pivot_df = df.pivot_table(index='day_of_week', columns='member_casual', values='avg_duration')

# create the plot
ax = pivot_df.plot(kind='bar', figsize=(10,6), width=0.8)

# set the title, axes labels, and legend
ax.set_title('Average Duration of Riders During Weekday', fontsize=15)
ax.set_xlabel('Weekday', fontsize=12)
ax.set_ylabel('Average Duration', fontsize=12)

# set the colors for each group
handles = []
for i, bars in enumerate(ax.containers):
    for bar in bars:
        bar.set_color(colors[pivot_df.columns[i]])
    # create a custom legend handle for each group
    handle = mpatches.Patch(color=colors[pivot_df.columns[i]], label=pivot_df.columns[i])
    handles.append(handle)

# add the custom legend handles to the legend
ax.legend(handles=handles, fontsize=12, title='Member/Casual', title_fontsize=14)

# adjust the plot layout and show the plot
plt.subplots_adjust(bottom=0.2)
plt.figtext(0.5, 0.02, 'Data from 2019-04 to 2020-03', ha='center', fontsize=12)
plt.show()

########################################################
#Total Number of Rides Per Hour of Day of Riders
########################################################

# create a new dataframe with counts of each hour and member type
grouped_data = df_all_trips.groupby(['member_casual', 'hour']).size().reset_index(name='count')

# create a sample dataframe
data = grouped_data
df = pd.DataFrame(data)

# define colors for member and casual categories
colors = {'member': 'blue', 'casual': 'Salmon'}

# create a pivot table with hour as index, member_casual as columns, and count as values
pivot_df = df.pivot_table(index='hour', columns='member_casual', values='count')

# create the plot
ax = pivot_df.plot(kind='bar', figsize=(10,6), width=0.8)

# set the title, axes labels, and legend
ax.set_title('Total Number of Rides Per Hour of the Day of the Riders', fontsize=15)
ax.set_xlabel('Hour', fontsize=12)
ax.set_ylabel('Number of Rides', fontsize=12)

# set the colors for each group
handles = []
for i, bars in enumerate(ax.containers):
    for bar in bars:
        bar.set_color(colors[pivot_df.columns[i]])
    # create a custom legend handle for each group
    handle = mpatches.Patch(color=colors[pivot_df.columns[i]], label=pivot_df.columns[i])
    handles.append(handle)

# add the custom legend handles to the legend
ax.legend(handles=handles, fontsize=12, title='Member/Casual', title_fontsize=14)

# adjust the plot layout and show the plot
plt.subplots_adjust(bottom=0.2)
plt.figtext(0.5, 0.02, 'Data from 2019-04 to 2020-03', ha='center', fontsize=12)
plt.show()

########################################################
#Total Number of Rides Per Month of Day of Riders
########################################################

#create a new dataframe with counts of each hour and member type
grouped_data = df_all_trips.groupby(['member_casual', 'month']).size().reset_index(name='count')

# create a sample dataframe
data = grouped_data
df = pd.DataFrame(data)

# define colors for member and casual categories
colors = {'member': 'blue', 'casual': 'Salmon'}

# create a new column with the month names
df['month_name'] = df['month'].apply(lambda x: datetime.strptime(str(x), "%m").strftime("%b"))

# define the desired order of months
month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# convert the month_name column to a categorical data type with the specified order
df['month_name'] = pd.Categorical(df['month_name'], categories=month_order, ordered=True)

# sort the dataframe by the month order
df = df.sort_values('month_name')

# create a pivot table with hour as index, member_casual as columns, and count as values
pivot_df = df.pivot_table(index='month_name', columns='member_casual', values='count')

# create the plot
ax = pivot_df.plot(kind='bar', figsize=(10,6), width=0.8)

# set the title, axes labels, and legend
ax.set_title('Total Number of Rides Per Month For Riders', fontsize=15)
ax.set_xlabel('Month', fontsize=12)
ax.set_ylabel('Number of Rides', fontsize=12)

# set the colors for each group
handles = []
for i, bars in enumerate(ax.containers):
    for bar in bars:
        bar.set_color(colors[pivot_df.columns[i]])
    # create a custom legend handle for each group
    handle = mpatches.Patch(color=colors[pivot_df.columns[i]], label=pivot_df.columns[i])
    handles.append(handle)

# add the custom legend handles to the legend
ax.legend(handles=handles, fontsize=12, title='Member/Casual', title_fontsize=14)

# adjust the plot layout and show the plot
plt.subplots_adjust(bottom=0.2)
plt.figtext(0.5, 0.02, 'Data from 2019-04 to 2020-03', ha='center', fontsize=12)
plt.show()