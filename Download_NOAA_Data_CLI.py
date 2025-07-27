"""
NOAA Data Downloader Module

Downloads oceanographic and meteorological data from NOAA Tides and Currents API.
Supports multiple data products including water levels, weather data, and tide 
predictions with automated station filtering and visualization generation.
Developer: Afshin Shabani, PhD
Contact: Afshin.shabani@tetratech.com
Github: AfshinShabani
"""

import os
import requests
import pandas as pd
import geopandas as gpd
import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import numpy as np
from datetime import datetime, timedelta

def download_url(url, filepath):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    # Save the data to a file
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(response.text)
    return

# Legacy hardcoded paths - no longer used, functions now accept parameters
# path=r"D:\Data Downloader\Sources\NOAA Stations\Test"
# boundary=gpd.read_file(r"D:\Data Downloader\Sources\NOAA Stations\Test\Boundary.shp")
# NOAA_stations=gpd.read_file(r"D:\Data Downloader\Sources\NOAA Stations\Shapefiles\NOAA_Stations.shp")  
# Intersection= gpd.overlay(NOAA_stations, boundary, how='intersection')

# Global constants
API_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

# Data product options (from CO-OPS API)
DATA_PRODUCTS = {
    "water_level": "Water Level",
    "air_temperature": "Air Temperature",
    "water_temperature": "Water Temperature",
    "wind": "Wind",
    "air_pressure": "Air Pressure",
    "air_gap": "Air Gap",
    "conductivity": "Conductivity",
    "visibility": "Visibility",
    "humidity": "Humidity",
    "salinity": "Salinity",
    "currents": "Currents"
}

# Time interval options
TIME_INTERVALS = {
    "h": "Hourly",
    "hilo": "High/Low",
    "6min": "6-Minute",
    "hourly": "Hourly",
    "daily": "Daily",
    "monthly": "Monthly"
}

#### This time interval is only available for water temperature, salinity,and conductivity, and metrological data
TIME_INTERVALS_1={
    "h": "Hourly",
    "6": "6-Minute"}

# Datum options
DATUMS = ["CRD","IGLD","LWD","MHHW", "MHW", "MTL", "MSL", "MLW", "MLLW", "NAVD", "STND"] # in order Columbia River datum, International Great Lakes Datum, Great Lake Low Water Datum, mean higher high water, mean high water, mean tide level, mean sea level, mean low water, mean lower low water, north american vertical datum, standard

# Time zone options
TIMEZONES = {
    "gmt": "Greenwich Mean Time",
    "lst": "Local Standard Time",
    "lst_ldt": "Local Standard/Daylight Time"
}

# Units options
UNITS = {
    "metric": "Metric",
    "english": "English"
}

datum="MLLW"
time_zone="lst_ldt"
units="metric"
interval="h"
begin_date="20250401"
end_date="20250428"

######### Download water level 
def download_realtime_water_level(datum, time_zone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
    
    output = os.path.join(output_base_path, 'Real Time Water Level')
    if not os.path.exists(output):
        os.makedirs(output)
    intersection = intersection.loc[intersection['type'] == "Water Level"]
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Real Time Water Level Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []

    for station_id in intersection['id'].unique():
        print(f"Downloading data for station {station_id}...")
        try:
            # Construct URL for realtime data
            url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
            url += f"date=today&station={station_id}&product=water_level&datum={datum}&time_zone={time_zone}&"
            url += f"units={units}&application=DataAPI_Sample&format=csv"
            
            # Define file path
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            
            # Download data
            download_url(url, filepath)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Water Level']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Water Level' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Water Level'], 'b-')
                    plt.title(f'Water Level for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Water Level (m)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Real Time Water Level data")
        except Exception as e:
            print(f"Error generating station map: {e}")
    
    return(stations_with_data,plot_output,intersection)
    
def download_verified_hourly_heights(begin_date, end_date, datum, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Verified Hourly Heights')
    if not os.path.exists(output):
        os.makedirs(output)
        
    intersection = intersection.loc[intersection['type'] == "Water Level"]
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Verified Hourly Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []

    for station_id in intersection['id'].unique():
        print(f"Downloading data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            # Extract years from begin_date and end_date
            start_year = int(begin_date[:4])
            end_year = int(end_date[:4])
            
            # Download data year by year
            for year in range(start_year, end_year + 1):
                # Define year start and end dates
                if year == start_year:
                    year_begin = begin_date
                else:
                    year_begin = f"{year}0101"  # January 1st
                
                if year == end_year:
                    year_end = end_date
                else:
                    year_end = f"{year}1231"  # December 31st
                
                print(f"  Downloading hourly heights for year {year}: {year_begin} to {year_end}")
                
                # Construct URL for this year
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"begin_date={year_begin}&end_date={year_end}&station={station_id}&product=hourly_height&datum={datum}&time_zone={timezone}&units={units}&"
                url += "application=DataAPI_Sample&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_{year}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this year
                download_url(url, temp_filepath)
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        temp_df = temp_df.iloc[:, :2]
                        temp_df.columns = ['Date Time', 'Water Level']
                        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                        print(f"  Added {len(temp_df)} records for {year}")
                    else:
                        print(f"  No data available for {year}")
                else:
                    print(f"  No data file created for {year}")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Water Level']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Water Level' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Water Level'], 'b-')
                    plt.title(f'Water Level for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Water Level (m)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                      # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Verified Hourly Heights data")
        except Exception as e:
            print(f"Error generating station map: {e}")
    
    return(stations_with_data, plot_output, intersection)

def tide_prediction(begin_date, end_date, datum, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Tide Prediction')
    if not os.path.exists(output):
        os.makedirs(output)
        
    intersection = intersection.loc[intersection['type'] == "Water Level"]
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Tide Prediction Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            # Extract years from begin_date and end_date
            start_year = int(begin_date[:4])
            end_year = int(end_date[:4])
            
            # Download data year by year
            for year in range(start_year, end_year + 1):
                # Define year start and end dates
                if year == start_year:
                    year_begin = begin_date  # Use the original begin date for the first year
                else:
                    year_begin = f"{year}0101"  # January 1st for other years
                
                if year == end_year:
                    year_end = end_date  # Use the original end date for the last year
                else:
                    year_end = f"{year}1231"  # December 31st for other years
                
                print(f"  Downloading tide predictions for year {year}: {year_begin} to {year_end}")
                
                # Construct URL for this year
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"begin_date={year_begin}&end_date={year_end}&station={station_id}&product=predictions&datum={datum}&time_zone={timezone}&"
                url += f"interval={interval}&units={units}&application=DataAPI_Sample&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_{year}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this year
                download_url(url, temp_filepath)
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and water level columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Water Level']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records for {year}")
                        else:
                            print(f"  Warning: Unexpected column format in data for {year}")
                    else:
                        print(f"  No data available for {year}")
                else:
                    print(f"  No data file created for {year}")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Water Level']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Water Level' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Water Level'], 'b-')
                    plt.title(f'Predicted Water Level for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Water Level (m)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Tide Prediction data")
        except Exception as e:
            print(f"Error generating station map: {e}")
    
    return(stations_with_data, plot_output, intersection)

def wind_data(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Wind Data')
    if not os.path.exists(output):
        os.makedirs(output)
        
    intersection = intersection.loc[intersection['type'] == "met"]
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Wind Data Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            # Extract years from begin_date and end_date
            start_year = int(begin_date[:4])
            end_year = int(end_date[:4])
            
            # Download data year by year
            for year in range(start_year, end_year + 1):
                # Define year start and end dates
                if year == start_year:
                    year_begin = begin_date  # Use the original begin date for the first year
                else:
                    year_begin = f"{year}0101"  # January 1st for other years
                
                if year == end_year:
                    year_end = end_date  # Use the original end date for the last year
                else:
                    year_end = f"{year}1231"  # December 31st for other years
                
                print(f"  Downloading wind data for year {year}: {year_begin} to {year_end}")
                
                # Construct URL for this year
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"begin_date={year_begin}&end_date={year_end}&station={station_id}&product=wind&time_zone={timezone}&"
                url += f"interval={interval}&units={units}&application=DataAPI_Sample&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_{year}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this year
                download_url(url, temp_filepath)
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time, speed and direction columns
                        if len(temp_df.columns) >= 3:
                            temp_df = temp_df.iloc[:, :3]
                            temp_df.columns = ['Date Time', 'Speed', 'Direction']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records for {year}")
                        else:
                            print(f"  Warning: Unexpected column format in data for {year}")
                    else:
                        print(f"  No data available for {year}")
                else:
                    print(f"  No data file created for {year}")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
                    
            # Save combined data to a single file
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :3]
                df.columns = ['Date Time','Speed', 'Direction']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Speed' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Speed'], 'b-')
                    plt.title(f'Wind Data for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Wind Speed (m/s)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()

                    # Create a wind rose plot
                    fig_windrose = plt.figure(figsize=(10, 10))
                    ax_windrose = fig_windrose.add_subplot(111, polar=True)
                                        
                    # Group data into bins
                    bins = 16  # Number of direction bins
                    dir_bins = np.linspace(0, 2*np.pi, bins+1)
                    speed_bins = [0, 2, 4, 6, 8, 10, 12]  # Speed bins in m/s
                                        
                    # Convert degrees to radians for polar plot
                    wind_dir_rad = np.radians(df['Direction'])
                                        
                    # Count occurrences in each bin
                    dir_counts = np.zeros((len(speed_bins)-1, bins))
                    for i in range(len(speed_bins)-1):
                        mask = (df['Speed'] >= speed_bins[i]) & (df['Speed'] < speed_bins[i+1])
                        for j in range(bins):
                            mask_dir = (wind_dir_rad >= dir_bins[j]) & (wind_dir_rad < dir_bins[j+1])
                            dir_counts[i, j] = np.sum(mask & mask_dir)
                                        
                    # Normalize by total
                    dir_freq = dir_counts / dir_counts.sum() * 100 if dir_counts.sum() > 0 else dir_counts
                                        
                    # Plot each speed bin
                    colors = ['#E6F5FF', '#CCE5FF', '#99CCFF', '#66B2FF', '#3399FF', '#0080FF']
                    width = dir_bins[1] - dir_bins[0]
                    for i in range(len(speed_bins)-1):
                        bars = ax_windrose.bar(dir_bins[:-1], dir_freq[i], width=width, bottom=np.sum(dir_freq[:i], axis=0))
                        for bar, color in zip(bars, [colors[i]]*bins):
                            bar.set_facecolor(color)
                                        
                    # Configure the plot
                    ax_windrose.set_theta_zero_location('N')
                    ax_windrose.set_theta_direction(-1)  # Clockwise
                    ax_windrose.set_thetagrids(np.degrees(dir_bins[:-1]), ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 
                                                                            'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'])
                                        
                    # Add legend
                    labels = [f'{speed_bins[i]}-{speed_bins[i+1]} m/s' for i in range(len(speed_bins)-1)]
                    ax_windrose.legend(labels, loc='lower right', bbox_to_anchor=(1.1, -0.1))
                    plt.title(f'Wind Rose for Station {station_id}')
                                        
                    # Save wind rose plot
                    windrose_filepath = os.path.join(plot_output, f"{station_id}_windrose.png")
                    plt.savefig(windrose_filepath, bbox_inches='tight')
                    plt.close(fig_windrose)
        
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                    
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Wind data")
        except Exception as e:
            print(f"Error generating station map: {e}")
    
    return(stations_with_data, plot_output, intersection)

def plot_data(intersection, stations_with_data, plot_output, boundary_data):
    # Plot stations on a map that have data
    if stations_with_data:
        # Filter to include only stations with data
        stations_map = intersection[intersection['id'].isin(stations_with_data)]
        
        # Make sure the data is in Web Mercator for contextily
        stations_map_web_mercator = stations_map.to_crs(epsg=3857)
        boundary_web_mercator = boundary_data.to_crs(epsg=3857)
        
        # Create map
        fig, ax = plt.subplots(figsize=(12, 10))
        
        boundary_web_mercator.boundary.plot(ax=ax, color='black', linewidth=0,alpha=0)
        # Plot stations with data
        stations_map_web_mercator.plot(ax=ax, color='red', markersize=50)
        
        for idx, row in stations_map_web_mercator.iterrows():
            plt.text(row.geometry.x, row.geometry.y, row['id'], fontsize=12, ha='center',
            path_effects=[path_effects.withStroke(linewidth=3, foreground='white')])

        
        # Add contextily basemap
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
        # Set plot limits based on boundary
        minx, miny, maxx, maxy = boundary_web_mercator.total_bounds
        # Add a small buffer (5% of width/height) around the boundary
        # ax.set_xlim(minx, maxx)
        # ax.set_ylim(miny , maxy )
        # plt.title('NOAA Stations with Water Level Data')
        map_filepath = os.path.join(plot_output, "stations_map.png")
        plt.savefig(map_filepath, bbox_inches='tight', dpi=300)
        plt.close()
        
        print(f"Map of stations with data saved to {map_filepath}")
    else:
        print("No stations had data available for mapping")
    
def download_water_temperature_data(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Water Temperature')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Water Temperature Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading temperature data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading temperature data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=water_temperature&application=NOS.COOPS.TAC.PHYSOCEAN&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Water Temperature']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Water Temperature']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Water Temperature' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Water Temperature'], 'r-')
                    plt.title(f'Water Temperature for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Water Temperature (°C)' if units == 'metric' else 'Water Temperature (°F)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Water Temperature data")
        except Exception as e:
            print(f"Error generating station map: {e}")

    return(stations_with_data, plot_output, intersection)

def download_conductivity_data(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Conductivity')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Conductivity Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading Conductivity data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading conductivity data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=conductivity&application=NOS.COOPS.TAC.PHYSOCEAN&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Conductivity']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Conductivity']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Conductivity' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Conductivity'], 'r-')
                    plt.title(f'Conductivity for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Conductivity (mS/cm)' if units == 'metric' else 'Conductivity') # use english unit for else 
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Conductivity data")
        except Exception as e:
            print(f"Error generating station map: {e}")

    return(stations_with_data, plot_output, intersection)

def download_air_temperature(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Air Temperature Data')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Air Temperature Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading Air Temperature data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading temperature data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=air_temperature&application=NOS.COOPS.TAC.METEROLOGICALOBS&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Air Temperature']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Air Temperature']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Air Temperature' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Air Temperature'], 'r-')
                    plt.title(f'Air Temperature for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Air Temperature (°C)' if units == 'metric' else 'Air Temperature (°F)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Air Temperature data")
        except Exception as e:
            print(f"Error generating station map: {e}")
    
    return(stations_with_data, plot_output, intersection)

def download_air_pressure(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Air Pressure Data')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Air Pressure Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading Air Pressure data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading pressure data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=air_pressure&application=NOS.COOPS.TAC.METEROLOGICALOBS&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Air Pressure']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Air Pressure']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Air Pressure' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Air Pressure'], 'r-')
                    plt.title(f'Air Pressure for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Air Pressure (mb)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                stations_with_data.append(station_id)
                print(f"Successfully plotted data for station {station_id}")
                # else:
                #     print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Air Pressure data")
        except Exception as e:
            print(f"Error generating station map: {e}")

    return(stations_with_data, plot_output, intersection)

def download_humidity(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Humidity Data')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Humidity Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading Humidity data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading Humidity data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=humidity&application=NOS.COOPS.TAC.METEROLOGICALOBS&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Humidity']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Humidity']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Humidity' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Humidity'], 'r-')
                    plt.title(f'Humidity for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Humidity (mb)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                stations_with_data.append(station_id)
                print(f"Successfully plotted data for station {station_id}")
                # else:
                #     print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Humidity data")
        except Exception as e:
            print(f"Error generating station map: {e}")

    return(stations_with_data, plot_output, intersection)

def download_visibility(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Visibility Data')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Visibility Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading Humidity data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading Visibility data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=visibility&application=NOS.COOPS.TAC.METEROLOGICALOBS&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Visibility']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Visibility']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Visibility' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Visibility'], 'r-')
                    plt.title(f'Visibility for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Visibility (km)' if units == 'metric' else 'Visibility (miles)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                stations_with_data.append(station_id)
                print(f"Successfully plotted data for station {station_id}")
                # else:
                #     print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Visibility data")
        except Exception as e:
            print(f"Error generating station map: {e}")

    return(stations_with_data, plot_output, intersection)

def download_salinity_data(begin_date, end_date, interval, timezone, units, output_base_path, boundary_shapefile, noaa_stations_shapefile):
    # Read the boundary shapefile
    boundary_data = gpd.read_file(boundary_shapefile)
    boundary_data = boundary_data.to_crs('EPSG:4326')
        
    # Read the NOAA stations shapefile  
    noaa_stations_data = gpd.read_file(noaa_stations_shapefile)
    
    # Create intersection
    intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
        
    # Create output folder for data
    output = os.path.join(output_base_path, 'Salinity Data')
    if not os.path.exists(output):
        os.makedirs(output)
    
    intersection.drop_duplicates(subset='id', inplace=True)

    # Create output folder for plots
    plot_output = os.path.join(output_base_path, 'Salinity Plots')
    if not os.path.exists(plot_output):
        os.makedirs(plot_output)

    # Track stations with data for map plotting later
    stations_with_data = []
    for station_id in intersection['id'].unique():
        print(f"Downloading temperature data for station {station_id}...")
        try:
            # Initialize an empty DataFrame to store all data
            combined_df = pd.DataFrame()
            
            
            # Convert begin_date and end_date to datetime objects
            start_date = datetime.strptime(begin_date, "%Y%m%d")
            end_date_dt = datetime.strptime(end_date, "%Y%m%d")
            
            # Process data in 31-day chunks
            current_date = start_date
            chunk_counter = 1
            
            while current_date <= end_date_dt:
                # Calculate end date for this chunk (30 days from current date or end_date, whichever is earlier)
                chunk_end_date = min(current_date + timedelta(days=30), end_date_dt)
                
                # Format dates for API
                chunk_begin = current_date.strftime("%Y%m%d")
                chunk_end = chunk_end_date.strftime("%Y%m%d")
                
                print(f"  Downloading temperature data for chunk {chunk_counter}: {chunk_begin} to {chunk_end}")
                
                # Construct URL for this chunk
                url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'
                url += f"product=salinity&application=NOS.COOPS.TAC.PHYSOCEAN&"
                url += f"begin_date={chunk_begin}&end_date={chunk_end}&station={station_id}&time_zone={timezone}&"
                url += f"units={units}&interval={interval}&format=csv"
                
                # Define temp file path
                temp_filename = f"{station_id}_chunk{chunk_counter}.csv"
                temp_filepath = os.path.join(output, temp_filename)
                
                # Download data for this chunk
                download_url(url, temp_filepath)
                
                # Move to next chunk
                current_date = chunk_end_date + timedelta(days=1)
                chunk_counter += 1
                
                # If file exists and has data, append to combined DataFrame
                if os.path.exists(temp_filepath) and os.path.getsize(temp_filepath) > 0:
                    temp_df = pd.read_csv(temp_filepath)
                    if len(temp_df) > 0:
                        # Use only the date/time and temperature columns
                        if len(temp_df.columns) >= 2:
                            temp_df = temp_df.iloc[:, :2]
                            temp_df.columns = ['Date Time', 'Salinity']
                            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
                            print(f"  Added {len(temp_df)} records")
                        else:
                            print(f"  Warning: Unexpected column format in data")
                    else:
                        print(f"  No data available")
                else:
                    print(f"  No data file created")
                
                # Remove temporary file
                if os.path.exists(temp_filepath):
                    os.remove(temp_filepath)
            
            # Save combined data to a single file (using only station_id without chunk numbers)
            filename = f"{station_id}.csv"
            filepath = os.path.join(output, filename)
            if not combined_df.empty:
                combined_df.to_csv(filepath, index=False)
            
            # Read and plot the data if file is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                df = pd.read_csv(filepath)
                df = df.iloc[:, :2]
                df.columns = ['Date Time', 'Salinity']
                df = df.loc[df['Date Time'] != 'Error: No data was found. This product may not be offered at this station at the requested time.']
                
                # Check if the file has data with required columns
                if len(df) > 0 and 'Date Time' in df.columns and 'Salinity' in df.columns:
                    # Convert Date Time to datetime
                    df['Date Time'] = pd.to_datetime(df['Date Time'])
                    
                    # Create plot
                    plt.figure(figsize=(10, 6))
                    plt.plot(df['Date Time'], df['Salinity'], 'r-')
                    plt.title(f'Salinity for Station {station_id}')
                    plt.xlabel('Date Time')
                    plt.ylabel('Salinity (PSU)')
                    plt.grid(True)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    # Save the plot
                    plot_filepath = os.path.join(plot_output, f"{station_id}_plot.png")
                    plt.savefig(plot_filepath)
                    plt.close()
                    
                    # Add station to list of stations with data
                    stations_with_data.append(station_id)
                    print(f"Successfully plotted data for station {station_id}")
                else:
                    print(f"No valid data available for station {station_id}")
            else:
                print(f"Empty file for station {station_id}")
                
        except Exception as e:
            print(f"Error processing station {station_id}: {e}")
    # Generate station map for this data product
    if stations_with_data:
        try:
            plot_data(intersection, stations_with_data, plot_output, boundary_data)
            print(f"Station map generated for Salinity data")
        except Exception as e:
            print(f"Error generating station map: {e}")

    return(stations_with_data, plot_output, intersection)




    