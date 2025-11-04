import mysql.connector as con
import random
from datetime import datetime, timedelta
import re

cnc = con.connect(
        host = 'localhost',
        user = 'root',
        password = 'MYSQL123',
        database = 'meteorological'
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
        query2='''INSERT INTO Observations (location_id, timestamp, temp, humidity, wind_speed_kmh) VALUES
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

def generate_initial_data(conn):
    """Generates synthetic weather data for multiple dates and locations."""
    cursor = cnc.cursor()

    # 1. Ensure Locations are in the table (You must run this first)
    # The actual IDs will be needed for the Observations table
    # This example assumes you've run the SQL INSERTs for CITIES

    # Simple way to get location IDs (assuming they are in the database)
    location_data = cursor.execute("select location_id, city from Locations").fetchall()

    if not location_data:
        print("ERROR: Please populate the Locations table first.")
        return

    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Data for the last 30 days

    print("Generating 30 days of observations...")

    current_date = start_date
    while current_date <= end_date:
        for loc_id, city_name in location_data:
            # Generate somewhat realistic, random data for weather
            temp = round(random.uniform(20.0, 35.0), 1)  # 20.0 to 35.0 C
            humidity = random.randint(50, 95)  # 50% to 95%
            wind = round(random.uniform(5.0, 25.0), 1)  # 5.0 to 25.0 km/h

            # Insert the generated observation
            sql_insert = '''
                INSERT INTO Observations 
                (location_id, timestamp, temperature_c, humidity_perc, wind_speed_kmh)
                VALUES (?, ?, ?, ?, ?)
            '''
            cursor.execute(sql_insert, (loc_id, current_date, temp, humidity, wind))

        current_date += timedelta(hours=6)  # Log observations every 6 hours

    conn.commit()
    print(f"✅ Successfully inserted {cursor.rowcount} total observations.")

def arrangeLoc():
    cursor.execute('select * from locations order by location_id ASC ; ')
    data=cursor.fetchall()
    print(data)
    cnc.commit()

def removeCity():
    cursor.execute("select * from locations, observations;")
    data = cursor.fetchall()
    print(data)

    locid = int(input("\nEnter Location ID you want to remove: "))

    qry = ' select exists (select 1 from locations where location_id =  %s); '
    cursor.execute(qry, (locid,))
    data = cursor.fetchall()

    if qry:
        cursor.execute("delete from locations where location_id= %s;", (locid,))
        cnc.commit()
        print(f"\nSuccessfully removed.")

    else:
        print(f"\nUnable to find Location ID")

def analyze_weather_stats(location_id, days=7):
    """Analyzes the fetched temperature data using NumPy."""

    data_list = fetch_temperature_data(location_id, days)

    if not data_list:
        print("No observation data available for analysis.")
        return

    # Convert the Python list into a NumPy array
    # This is where NumPy is incorporated
    temperatures = np.array(data_list)

    # Use NumPy functions for efficient calculations
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

# --- Integration into Admin Menu ---
# def admin_view_reports_menu():
#     # ... ask admin for location_id and days ...
#     analyze_weather_stats(location_id=1, days=7)

def fetch_temperature_data(location_id, days=7):
    try:

        # SQL to get temperature and timestamp for the location
        sql_select = '''
            SELECT temp
            FROM Observations
            WHERE location_id = %s 
            AND timestamp >= NOW()-INTERVAL %s DAY
            ORDER BY timestamp DESC
        '''

    
        # Fetch temperatures for the last 7 days
        cursor.execute(sql_select, (location_id, days))
        # Use a list comprehension to flatten the list of tuples into a simple list of numbers
        temperature_list = [row[0] for row in cursor.fetchall()]
        print("The following temperatures have been recorded this week")
        for i in temperature_list:
            print(i)
        return ''
    
    except:
        print(f"Error fetching data.")
        return []











def main():
        header1 = "WELCOME TO WEATHER-WISE"
        header2 = "Meteorological Data Analysis"

        try:
            terminal_width = shutil.get_terminal_size().columns
        except:
            terminal_width = 80  #default in some platforms

        print(header1.center(terminal_width))
        print(header2.center(terminal_width), end = '\n\n')

        result = login()

        if result:
        while True:
            print('''\n    
                 1. Load
                 2. Remove
                 3. Edit
                 If you wish to exit, type '0'
                ''')
            ch = int(input('Enter your choice: '))

            if ch == 0:
                print("\nGoodbye! See you soon!")
                break
