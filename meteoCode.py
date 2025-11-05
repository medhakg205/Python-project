import mysql.connector as con
import shutil
from datetime import datetime, timedelta
import re
import numpy as np

cnc = con.connect(
    host='localhost',
    user='root',
    password='MYSQL123',
    database='meteorological'
)

if not cnc.is_connected:
    print('Error connecting to database!')
    exit()
else:
    cursor = cnc.cursor()


def createTable():
    qry1 = '''CREATE TABLE IF NOT EXISTS locations(
           location_id int primary key auto_increment,
           city varchar(50) not null unique,
           state varchar(50),
           latitude float,
           longitude float
           );'''
    cursor.execute(qry1)

    qry2 = '''CREATE TABLE IF NOT EXISTS Observations (
            obvs_id INTEGER PRIMARY KEY auto_increment,
            location_id INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            temp float NOT NULL,
            humidity INTEGER,
            wind_speed_kmh REAL,
            FOREIGN KEY (location_id) REFERENCES Locations (location_id)
        );'''

    cursor.execute(qry2)

    cnc.commit()


def InsertIntoLocations():
    query = "INSERT INTO locations (city, state, latitude, longitude) VALUES (%s, %s, %s, %s)"

    data = [
        ('Mumbai', 'Maharashtra', 19.0760, 72.8777),
        ('Delhi', 'Delhi', 28.6139, 77.2090),
        ('Bengaluru', 'Karnataka', 12.9716, 77.5946),
        ('Chennai', 'Tamil Nadu', 13.0827, 80.2707),
        ('Kolkata', 'West Bengal', 22.5726, 88.3639),
        ('Hyderabad', 'Telangana', 17.3850, 78.4867),
        ('Pune', 'Maharashtra', 18.5204, 73.8567),
        ('Ahmedabad', 'Gujarat', 23.0225, 72.5714),
        ('Jaipur', 'Rajasthan', 26.9124, 75.7873),
        ('Lucknow', 'Uttar Pradesh', 26.8467, 80.9462)]

    cursor.executemany(query, data)
    cnc.commit()


def InsertIntoObservations():
    query2 = '''INSERT INTO Observations (location_id, timestamp, temp, humidity, wind_speed_kmh) VALUES
        (1, '2025-11-04 08:00:00', 28.5, 65, 12.4),
        (1, '2025-11-04 14:00:00', 33.2, 58, 15.1),
        (1, '2025-11-03 08:00:00', 27.8, 68, 10.3),
        (2, '2025-11-04 09:00:00', 21.4, 72, 8.7),
        (2, '2025-11-03 15:00:00', 24.0, 69, 9.8),
        (3, '2025-11-04 07:00:00', 17.6, 80, 5.4),
        (3, '2025-11-03 13:00:00', 22.1, 70, 6.8),
        (4, '2025-11-04 10:00:00', 30.5, 55, 20.3),
        (4, '2025-11-03 16:00:00', 31.0, 50, 18.9),
        (5, '2025-11-04 11:00:00', 25.8, 63, 11.2);'''
    cursor.execute(query2)
    cnc.commit()


def insert_new_observation():
    try:
        loc_id = int(input("\nEnter Location ID: "))

        # Check if location exists
        cursor.execute("SELECT city FROM locations WHERE location_id = %s", (loc_id,))
        location = cursor.fetchone()

        if not location:
            print("Invalid Location ID! No such location exists.")
            return

        print(f"Location found: {location[0]}")

        # Get weather values
        timestamp = input("Enter timestamp (YYYY-MM-DD HH:MM:SS): ").strip()
        temp = float(input("Enter temperature (°C): "))
        humidity = int(input("Enter humidity (%): "))
        wind_speed = float(input("Enter wind speed (km/h): "))

        # Insert query
        query = '''
            INSERT INTO observations (location_id, timestamp, temp, humidity, wind_speed_kmh)
            VALUES (%s, %s, %s, %s, %s);
        '''

        cursor.execute(query, (loc_id, timestamp, temp, humidity, wind_speed))
        cnc.commit()

        print("\nNew observation record inserted successfully!")

    except Exception as e:
        print("\nError inserting observation:", e)


def editCity(locid):
    qry = 'Select * from locations where location_id = %s;'
    cursor.execute(qry, (locid,))
    d = cursor.fetchone()

    print(f"\nWhat would you like to change the name {d[1]} to?")
    cityn = input().strip()

    # REGEX: Only letters and spaces
    if not re.match("^[A-Za-z ]+$", cityn):
        print("Invalid city name! Use only letters and spaces.")
        return

    qrynew = ''' update locations set city = %s where location_id = %s '''
    cursor.execute(qrynew, (cityn, locid))
    cnc.commit()
    print(f"\nName has successfully been changed to {cityn} .")


def view_location_weather(loc_id):
    try:
        qry = '''
            SELECT l.city, l.state, l.latitude, l.longitude,
                   o.timestamp, o.temp, o.humidity, o.wind_speed_kmh
            FROM locations l
            JOIN observations o ON l.location_id = o.location_id
            WHERE l.location_id = %s
            ORDER BY o.timestamp ASC;
        '''
        cursor.execute(qry, (loc_id,))
        records = cursor.fetchall()

        if not records:
            print("\nNo data found for that Location ID!")
            return

        city, state, lat, lon = records[0][0], records[0][1], records[0][2], records[0][3]

        print(f"\n--- Location Details ---")
        print(f"City:       {city}")
        print(f"State:      {state}")
        print(f"Latitude:   {lat}")
        print(f"Longitude:  {lon}")
        print("\n--- Weather Observations ---")
        print(f"{'Timestamp':<20} {'Temp (°C)':<10} {'Humidity (%)':<13} {'Wind (km/h)':<12}")
        print("-" * 60)

        for row in records:
            timestamp, temp, humidity, wind = row[4], row[5], row[6], row[7]
            print(f"{str(timestamp):<20} {temp:<10} {humidity:<13} {wind:<12}")

        print("\n")

    except Exception as e:
        print("Error:", e)


def login():
    user = input("Admin Login or Client Login: ")
    if user.lower() == 'admin':
        passw = int(input("Enter password: "))
        if passw == 123:
            return True
        else:
            print("Invalid password")
            return -1
    else:
        return False


def adminview():
    cursor.execute('select * from locations order by location_id ASC ; ')
    data = cursor.fetchall()
    print(data)
    cnc.commit()


def removeCity():
    cursor.execute("select * from locations;")
    data = cursor.fetchall()
    for i in data:
        print(i[0], i[1], end='\n')
    locid = int(input("\nEnter Location ID you want to remove: "))
    conf = input("\nAre you sure you want to remove this location? (y/n): ")
    if conf.lower() == 'y':
        qry = ' select exists (select 1 from locations where location_id =  %s); '
        cursor.execute(qry, (locid,))
        data = cursor.fetchall()

    if qry:
        cursor.execute("delete from observations where location_id= %s;", (locid,))
        cnc.commit()
        try:

            cursor.execute("DELETE FROM Observations WHERE location_id = %s;", (locid,))

            cursor.execute("DELETE FROM locations WHERE location_id = %s;", (locid,))

            cnc.commit()
            print(f"\nSuccessfully removed.")

        except Exception as e:
            print(f"\nAn error occurred during deletion: {e}")

    else:
        print(f"\nUnable to find Location ID")


def analyze_weather_stats(location_id, days=7):
    data_list = fetch_temperature_data(location_id, days)

    if not data_list:
        print("No observation data available for analysis.")
        return

    temperatures = np.array(data_list)

    # Using NumPy functions for efficient calculations
    max_temp = np.max(temperatures)
    min_temp = np.min(temperatures)
    average_temp = np.mean(temperatures)
    std_dev = np.std(temperatures)

    # --- CLI Output ---
    print(f"\n--- Weather Analysis for Location ID {location_id} (Last {days} Days) ---")
    print(f"Total Observations: {len(temperatures)}")
    print(f"**Highest Temperature:** {max_temp:.2f}°C")
    print(f"**Lowest Temperature:** {min_temp:.2f}°C")
    print(f"**Average Temperature:** {average_temp:.2f}°C")
    print(f"**Temperature Standard Deviation (Variability):** {std_dev:.2f}")


def fetch_temperature_data(location_id, days=7):
    try:

        sql_select = '''
            SELECT temp
            FROM Observations
            WHERE location_id = %s 
            AND timestamp >= NOW()-INTERVAL %s DAY
            ORDER BY timestamp DESC
        '''

        cursor.execute(sql_select, (location_id, days))
        temperature_list = [row[0] for row in cursor.fetchall()]
        return temperature_list

    except:
        print(f"Error fetching data.")
        return []


# =========================================================

def main():
    header1 = "WELCOME TO WEATHER-WISE"
    header2 = "Meteorological Data Analysis"

    try:
        terminal_width = shutil.get_terminal_size().columns
    except:
        terminal_width = 80  # default in some platforms

    print(header1.center(terminal_width))
    print(header2.center(terminal_width), end='\n\n')

    result = login()
    if result==True:
        while True:
            print('''\n    
                                 1. View Data
                                 2. Add Data
                                 3. Edit Data
                                 4. Delete Data
                                 If you wish to exit, type '0'
                                ''')
            ch = int(input('Enter your choice: '))

            if ch == 0:
                print("\nGoodbye! See you soon!")
                break

            elif ch == 1:
                print("You selected: View Weather Data")
                loc_id = int(input("Enter location ID"))
                view_location_weather(loc_id)
                analyze_weather_stats(loc_id)
            elif ch == 2:
                print("You selected: Insert New Observation")
                insert_new_observation()
            elif ch == 3:
                print("You selected: Edit Location Name")
                loc_id = int(input("Enter location ID"))
                editCity(loc_id)
            elif ch == 4:
                print("You selected: Delete Location")
                removeCity()

    elif result == False:
        print("Welcome Client!\nClick on 'e' to exit, any other key to view data")
        cch = input("Press any key (except 'e') to view data. ")
        if cch.lower() == 'e':

            print("\nGoodbye! See you soon!")
            exit()

        else:
            n = int(input("Enter location ID"))
            view_location_weather(n)
    elif result :
        print("Wrong Credentials, retry")
        return 0

main()
cnc.close()


