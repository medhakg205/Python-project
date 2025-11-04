import mysql.connector as con
import random
from datetime import datetime, timedelta

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
            obvs_id INTEGER PRIMARY KEY,
            location_id INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            temp float NOT NULL,
            humidity INTEGER,
            wind_speed_kmh REAL,
            FOREIGN KEY (location_id) REFERENCES Locations (location_id)
        );'''

    cursor.execute(qry2)

    cnc.commit()

def editCity(locid):
    qry = 'Select * from locations where location_id = %s;'
    cursor.execute(qry, (locid,))
    d = cursor.fetchone()

    print(f"\nWhat would you like to change the name {d[1]} to?")
    cityn = input()

    qrynew = ''' update locations set city = %s where location_id = %s '''
    cursor.execute(qrynew,(cityn, locid))
    cnc.commit()
    print(f"\nName has successfully been changed to {cityn} .")

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

def arrangeSub():
    cursor.execute('select * from locations order by location_id ASC ; ')
    cursor.fetchall()
    cnc.commit()

def removeCity(locid):
    # cursor.execute("select * from locations;")
    cursor.execute("select * from locations,observations where location_id = %s; ",(locid,))
    data = cursor.fetchall()
    print(data)

    subj = int(input("\nEnter Location ID you want to remove: "))

    qry = ' select exists (select 1 from subjects where subid =  %s); '
    cursor.execute(qry, (subj,))
    data = cursor.fetchall()

    if qry:
        cursor.execute("delete from subjects where subid= %s;", (subj,))
        cnc.commit()
        print(f"\nSuccessfully removed {d[1]} from user ID {user_id}.")

    else:
        print(f"\nUnable to find subject ID {subj}.")

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

    # SQL to get temperature and timestamp for the location
    sql_select = '''
        SELECT temperature_c
        FROM Observations
        WHERE location_id = ? 
        AND timestamp >= datetime('now', ?)
        ORDER BY timestamp DESC
    '''

    try:
        # Fetch temperatures for the last 7 days
        cursor.execute(sql_select, (location_id, f'-{days} day'))
        # Use a list comprehension to flatten the list of tuples into a simple list of numbers
        temperature_list = [row[0] for row in cursor.fetchall()]
        return temperature_list
    
    except:
        print(f"Error fetching data.")
        return []
