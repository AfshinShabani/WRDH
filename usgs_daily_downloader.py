"""
USGS Daily Data Downloader Module

Specialized module for downloading USGS daily statistical data (mean, min, max values).
Optimized for long-term trend analysis and large date ranges with efficient processing
and comprehensive visualization output.
Developer: Afshin Shabani, PhD
Contact: Afshin.shabani@tetratech.com
Github: AfshinShabani
"""

import os
import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import datetime as dt
import matplotlib.pyplot as plt
import contextily as ctx
import traceback
from io import StringIO
from multiprocessing.pool import ThreadPool
import threading
import warnings
import folium
from folium.plugins import MeasureControl
import dataretrieval.nwis as nwis
import re
import openpyxl  # For Excel file support

warnings.filterwarnings("ignore")

# Daily parameter service codes
DAILY_PARAMETER_CODES = {
    'dv': 'Daily Values (Streamflow)'
}

# Station types with descriptions
STATION_TYPES = {
    'Surface Water': ['ES', 'LK', 'ST', 'ST_CA', 'ST-DCH', 'ST-TS', 'WE'],
    'Ground Water': ['GW', 'GW-CR', 'GW-EX', 'GW-HZ', 'GW-IW', 'GW-MW', 'GW-TH', 'SB', 'SB-CV', 'SB-GWD', 'SB-TSM', 'SB-UZ'],
    'Spring': ['SP'],
    'Atmospheric': ['AT']
}

def fetch_url(path, entry, max_retries=3, timeout=30):
    """Download data from a URL and save to a file with retry logic."""
    if os.path.exists(path):
        return True
        
    for attempt in range(max_retries):
        try:
            print(f"Attempting to download (attempt {attempt + 1}/{max_retries}): {entry}")
            
            # Set headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
              # Use timeout and stream for large files
            r = requests.get(entry, stream=True, timeout=timeout, headers=headers)
            r.raise_for_status()  # Raise an exception for bad status codes
            
            if r.status_code == 200:
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                print(f"Successfully downloaded: {entry}")
                return True
            else:
                print(f"HTTP error {r.status_code} for {entry}")
                
        except requests.exceptions.Timeout:
            print(f"Timeout error (attempt {attempt + 1}/{max_retries}) for {entry}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error (attempt {attempt + 1}/{max_retries}) for {entry}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}) for {entry}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
        except Exception as e:
            print(f"Unexpected error (attempt {attempt + 1}/{max_retries}) downloading {entry}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
    
    print(f"Failed to download after {max_retries} attempts: {entry}")
    return False

def fetch_usgs_station_inventory(path, west, east, south, north, max_retries=3):
    """
    Specialized function to download USGS station inventory with better error handling.
    """
    if os.path.exists(path):
        return True
    
    # Build the USGS URL
    USGS_url = 'https://nwis.waterdata.usgs.gov/nwis/inventory?'
    USGS_url += f'nw_longitude_va={west}&nw_latitude_va={north}'
    USGS_url += f'&se_longitude_va={east}&se_latitude_va={south}'
    USGS_url += '&coordinate_format=decimal_degrees&group_key=NONE&format=sitefile_output'
    USGS_url += '&sitefile_output_format=rdb&column_name=agency_cd&column_name=site_no'
    USGS_url += '&column_name=station_nm&column_name=site_tp_cd&column_name=dec_lat_va'
    USGS_url += '&column_name=dec_long_va&column_name=coord_datum_cd&list_of_search_criteria=lat_long_bounding_box'
    
    print(f"Downloading USGS station inventory for area: W={west}, E={east}, S={south}, N={north}")
    
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} to download USGS station inventory...")
            
            # Use a longer timeout for USGS servers and browser-like headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(USGS_url, headers=headers, timeout=60)
            response.raise_for_status()
            
            # Check if we got valid data (not an error page)
            content = response.text
            if 'agency_cd' in content and 'site_no' in content:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Successfully downloaded USGS station inventory.")
                return True
            else:
                print(f"Invalid response from USGS server (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    print("Retrying in 10 seconds...")
                    import time
                    time.sleep(10)
                    
        except requests.exceptions.Timeout:
            print(f"Timeout error (attempt {attempt + 1}/{max_retries}) - USGS server is slow")
            if attempt < max_retries - 1:
                print("Retrying in 15 seconds...")
                import time
                time.sleep(15)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print("Retrying in 10 seconds...")
                import time
                time.sleep(10)
        except Exception as e:
            print(f"Error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print("Retrying in 10 seconds...")
                import time
                time.sleep(10)
    
    print(f"Failed to download USGS station inventory after {max_retries} attempts")
    return False

def download_usgs_daily_data(parameter, start, end, base_path, area_name, shapefile_path, 
                            selected_station_types, save_urls=False, stop_download_callback=None,
                            update_status_callback=None, update_progress_callback=None, 
                            log_callback=None, verbose_errors=False):
    """
    Download USGS daily data for specified parameters and area.
    
    Args:
        parameter: Parameter service code (e.g., 'dv' for daily values)
        start: Start date string
        end: End date string  
        base_path: Base directory path for saving data
        area_name: Name of the study area
        shapefile_path: Path to boundary shapefile
        selected_station_types: List of station types to include
        save_urls: Boolean to save download URLs to file
        stop_download_callback: Function to check if download should stop
        update_status_callback: Function to update status message
        update_progress_callback: Function to update progress bar
        log_callback: Function to log messages
        verbose_errors: Boolean for verbose error reporting
    
    Returns:
        Dictionary with download results
    """
    
    def _update_status(msg):
        if update_status_callback:
            update_status_callback(msg)
        else:
            print(f"Status: {msg}")
    
    def _log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(f"Log: {msg}")
    
    def _update_progress(current, total):
        if update_progress_callback:
            update_progress_callback(current, total)
        else:
            print(f"Progress: {current}/{total}")
    
    def _stop_check():
        if stop_download_callback:
            return stop_download_callback()
        return False
    
    _update_status(f"Starting download for daily parameter: {parameter} ({DAILY_PARAMETER_CODES.get(parameter, 'Unknown')})")
    _log(f"Selected station types: {', '.join(selected_station_types)}")
    
    # Create directories
    area_dir = os.path.join(base_path, area_name)
    if not os.path.exists(area_dir):
        os.mkdir(area_dir)
        _log(f"Created area directory: {area_dir}")
    
    # Create parameter directory
    data_dir = os.path.join(area_dir, parameter)
    if not os.path.exists(data_dir): 
        os.mkdir(data_dir)
        _log(f"Created parameter directory: {data_dir}")
        
    GIS_dir = os.path.join(data_dir, 'GIS')
    if not os.path.exists(GIS_dir): 
        os.mkdir(GIS_dir)
        _log(f"Created directory: {GIS_dir}")
    
    txt_dir = os.path.join(data_dir, 'Data')
    if not os.path.exists(txt_dir): 
        os.mkdir(txt_dir)
        _log(f"Created directory: {txt_dir}")
    
    txt_dir_fig = os.path.join(data_dir, 'Figures')
    if not os.path.exists(txt_dir_fig): 
        os.mkdir(txt_dir_fig)
        _log(f"Created directory: {txt_dir_fig}")
    
    # Read boundary shapefile and get coordinates bounds
    _update_status("Reading boundary shapefile...")
    boundary = gpd.read_file(shapefile_path)
    boundary = boundary.to_crs('EPSG:4326')
    
    bnds = boundary.bounds
    
    West = round(bnds['minx'][0], 2)
    East = round(bnds['maxx'][0], 2)
    South = round(bnds['miny'][0], 2)
    North = round(bnds['maxy'][0], 2)
    
    _log(f"Boundary coordinates: West={West}, East={East}, South={South}, North={North}")    # Download USGS Gauge Stations using the same approach as core downloader
    _update_status("Downloading USGS station information...")
    USGS_url = 'https://nwis.waterdata.usgs.gov/nwis/inventory?'
    USGS_url += f'nw_longitude_va={West}&nw_latitude_va={North}'
    USGS_url += f'&se_longitude_va={East}&se_latitude_va={South}'
    USGS_url += '&coordinate_format=decimal_degrees&group_key=NONE&format=sitefile_output'
    USGS_url += '&sitefile_output_format=rdb&column_name=agency_cd&column_name=site_no'
    USGS_url += '&column_name=station_nm&column_name=site_tp_cd&column_name=dec_lat_va'
    USGS_url += '&column_name=dec_long_va&column_name=coord_datum_cd&list_of_search_criteria=lat_long_bounding_box'
    
    ofile_path = os.path.join(data_dir, f'{area_name}_stations_raw.txt')
    
    # Try the specialized function first, then fall back to regular fetch_url
    success = False
    try:
        success = fetch_usgs_station_inventory(ofile_path, West, East, South, North, max_retries=3)
    except:
        _log("Specialized USGS function failed, trying regular download...")
        success = fetch_url(ofile_path, USGS_url, max_retries=3, timeout=60)
    
    if not success:
        _update_status("Failed to download station information.")
        return {'success': False, 'error': 'Failed to download station information'}
      # Read the downloaded file and parse stations
    _update_status("Processing station information...")
    stations_df = None
    
    try:
        # Try to read the downloaded file first
        if os.path.exists(ofile_path) and os.path.getsize(ofile_path) > 0:
            with open(ofile_path, 'r') as f:
                lines = f.readlines()
                
            # Find the line that starts with agency_cd (header line)
            header_index = 0
            for i, line in enumerate(lines):
                if line.startswith('agency_cd'):
                    header_index = i
                    break
            
            if header_index < len(lines) - 2:  # Make sure we have data after header
                # Create a dataframe from the lines
                header = lines[header_index].strip().split('\t')
                
                # Skip the header and the line after it (which has formatting info)
                data_lines = lines[header_index + 2:]
                
                # Process data lines
                stations_data = []
                for line in data_lines:
                    if line.strip() and not line.startswith('#'):
                        parts = line.strip().split('\t')
                        if len(parts) >= 7:  # Make sure all expected columns are present
                            stations_data.append(parts)
                
                if stations_data:  # Only create DataFrame if we have data
                    stations_df = pd.DataFrame(stations_data, columns=header)
                    _log(f"Successfully parsed {len(stations_df)} stations from downloaded file")
        
        # If file parsing failed, try dataretrieval as fallback
        if stations_df is None or stations_df.empty:
            _log("File parsing failed or no data found, trying dataretrieval as fallback...")
            stations_df = get_stations_using_dataretrieval(West, East, South, North, selected_station_types, _log)
        
        # Final check
        if stations_df is None or stations_df.empty:
            _update_status("No station information could be obtained from any source.")
            return {'success': False, 'error': 'Failed to obtain station information from all sources'}
        
        # Filter stations by station type (only if not already filtered by dataretrieval)
        if 'site_tp_cd' in stations_df.columns:
            _log(f"Filtering stations to only include types: {selected_station_types}")
            filtered_stations = stations_df[stations_df['site_tp_cd'].isin(selected_station_types)]
        else:
            filtered_stations = stations_df  # Assume already filtered if no site_tp_cd column
        
        # Check if we have any stations
        if filtered_stations.empty:
            _update_status(f"No stations found for the selected types: {selected_station_types}")
            return {'success': False, 'error': f'No stations found for selected types: {selected_station_types}'}
        
        _log(f"Found {len(filtered_stations)} stations of selected types.")
        
        # Create a GIS layer with the stations
        gdf = gpd.GeoDataFrame(
            filtered_stations,
            geometry=gpd.points_from_xy(
                pd.to_numeric(filtered_stations['dec_long_va'], errors='coerce'),
                pd.to_numeric(filtered_stations['dec_lat_va'], errors='coerce')
            ),
            crs='EPSG:4326'
        )
        
        # Remove rows with invalid coordinates
        gdf = gdf.dropna(subset=['geometry'])
        
        # Clip stations to boundary
        gdf_clipped = gpd.clip(gdf, boundary)
        
        if gdf_clipped.empty:
            _update_status("No stations found within the boundary.")
            return {'success': False, 'error': 'No stations found within boundary'}
        
        _log(f"Found {len(gdf_clipped)} stations within the boundary.")
        
        # Save the stations shapefile
        stations_shapefile = os.path.join(GIS_dir, f'{area_name}_stations.shp')
        gdf_clipped.to_file(stations_shapefile)
        _log(f"Saved stations shapefile: {stations_shapefile}")
        
        # Use dataretrieval to get daily data for each station
        _update_status("Downloading daily data using dataretrieval...")
        
        successful_downloads = 0
        failed_downloads = 0
        all_data = []
        
        total_stations = len(gdf_clipped)
        
        for i, (_, station) in enumerate(gdf_clipped.iterrows()):
            if _stop_check():
                _update_status("Download stopped by user.")
                return {'success': False, 'error': 'Download stopped by user'}
            
            site_id = station['site_no']
            
            try:                # Download daily values using dataretrieval - FIXED SYNTAX
                _log(f"Downloading data for station {site_id}...")
                df = nwis.get_record(sites=site_id, service=parameter, start=start, end=end)
                if not df.empty:
                    # Add station information to the data
                    df['site_no'] = site_id
                    df['station_nm'] = station['station_nm']
                    df['site_tp_cd'] = station['site_tp_cd']
                    df['dec_lat_va'] = station['dec_lat_va']
                    df['dec_long_va'] = station['dec_long_va']
                    
                    # Create discharge line plot for this station
                    try:
                        # Find discharge column (should start with '00060')
                        discharge_cols = [col for col in df.columns if col.startswith('00060')]
                        if discharge_cols:
                            discharge_col = discharge_cols[0]
                            
                            # Create the plot
                            plt.figure(figsize=(12, 6))
                            plt.plot(df.index, df[discharge_col], linewidth=1.5, color='blue')
                            plt.xlabel('Date')
                            plt.ylabel('Discharge (cfs)')
                            plt.title(f'Daily Discharge - Station {site_id}\n{station["station_nm"]}')
                            plt.xticks(rotation=45)
                            plt.grid(True, alpha=0.3)
                            plt.tight_layout()
                            
                            # Save the plot
                            plot_file = os.path.join(txt_dir_fig, f"{site_id}_daily_discharge.png")
                            plt.savefig(plot_file, dpi=300, bbox_inches='tight')
                            plt.close()
                            
                            _log(f"Created discharge plot for station {site_id}")
                    except Exception as e:
                        _log(f"Error creating plot for station {site_id}: {str(e)}")
                    
                    # Save individual station file
                    station_file = os.path.join(txt_dir, f"{site_id}_daily.csv")
                    df.to_csv(station_file)
                    
                    all_data.append(df)
                    successful_downloads += 1
                    _log(f"Downloaded data for station {site_id}: {len(df)} records")
                else:
                    failed_downloads += 1
                    if verbose_errors:
                        _log(f"No data available for station {site_id}")
                        
            except Exception as e:
                failed_downloads += 1
                if verbose_errors:
                    _log(f"Error downloading data for station {site_id}: {str(e)}")
            
            # Update progress
            _update_progress(i + 1, total_stations)
            _update_status(f"Processing station {i + 1}/{total_stations}: {site_id}")
        
        _log(f"Successfully downloaded data for {successful_downloads} out of {total_stations} stations.")
        _log(f"Failed downloads: {failed_downloads}")
        
        # Combine all data and create summary
        if all_data:
            _update_status("Creating combined dataset...")
            
            # Combine all station data
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Save combined dataset
            combined_file = os.path.join(txt_dir, f"{area_name}_combined_daily.csv")
            combined_df.to_csv(combined_file, index=False)
            _log(f"Saved combined dataset: {combined_file}")
            
            # Generate summary statistics
            _update_status("Generating summary statistics...")
            create_daily_summary(combined_df, gdf_clipped, boundary, txt_dir_fig, area_name, _log)
            
            _update_status("Daily data download completed.")
            return {
                'success': True,
                'total_stations': total_stations,
                'successful_downloads': successful_downloads,
                'failed_downloads': failed_downloads,
                'data_directory': data_dir,
                'combined_file': combined_file
            }
        else:
            _update_status("No data was successfully downloaded.")
            return {'success': False, 'error': 'No data was successfully downloaded'}
            
    except Exception as e:
        error_msg = f"Error in daily download process: {str(e)}\n{traceback.format_exc()}"
        _log(error_msg)
        return {'success': False, 'error': error_msg}

def create_daily_summary(combined_df, stations_gdf, boundary, figures_dir, area_name, log_callback):
    """
    Create summary statistics and visualizations for daily data.
    
    Args:
        combined_df: Combined DataFrame with all station data
        stations_gdf: GeoDataFrame of station locations
        boundary: Study area boundary
        figures_dir: Directory to save figures
        area_name: Name of study area
        log_callback: Function to log messages
    """
    
    def _log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(f"Log: {msg}")
    
    try:
        # Create summary statistics
        summary_stats = []
        
        for site_id in combined_df['site_no'].unique():
            site_data = combined_df[combined_df['site_no'] == site_id]
            
            # Get discharge column (should be the first numeric column)
            discharge_cols = [col for col in site_data.columns if col.startswith('00060')]
            if discharge_cols:
                discharge_col = discharge_cols[0]
                discharge_data = pd.to_numeric(site_data[discharge_col], errors='coerce')
                
                stats = {
                    'site_no': site_id,
                    'station_name': site_data['station_nm'].iloc[0],
                    'count': len(discharge_data.dropna()),
                    'mean': discharge_data.mean(),
                    'median': discharge_data.median(),
                    'min': discharge_data.min(),
                    'max': discharge_data.max(),
                    'std': discharge_data.std()
                }
                summary_stats.append(stats)
        
        # Create summary DataFrame
        summary_df = pd.DataFrame(summary_stats)
          # Save summary statistics as Excel file
        summary_file = os.path.join(figures_dir, f"{area_name}_daily_summary_stats.xlsx")
        summary_df.to_excel(summary_file, index=False)
        _log(f"Saved summary statistics: {summary_file}")
        
        # Create visualizations
        create_daily_visualizations(summary_df, stations_gdf, boundary, figures_dir, area_name, _log)
        
    except Exception as e:
        _log(f"Error creating daily summary: {str(e)}")

def create_daily_visualizations(summary_df, stations_gdf, boundary, figures_dir, area_name, log_callback):
    """
    Create visualizations for daily data summary.
    
    Args:
        summary_df: DataFrame with summary statistics
        stations_gdf: GeoDataFrame of station locations
        boundary: Study area boundary
        figures_dir: Directory to save figures
        area_name: Name of study area
        log_callback: Function to log messages
    """
    
    def _log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(f"Log: {msg}")
    
    try:
        # Merge summary stats with station locations
        merged_gdf = stations_gdf.merge(summary_df, on='site_no', how='inner')
          # Create a map figure showing mean discharge
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        
        # Plot boundary
        boundary.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=2, alpha=0.7)
        
        # Plot stations as simple points (no colorbar)
        merged_gdf.plot(ax=ax, color='red', markersize=60, alpha=0.8)
        
        # Add station ID labels
        for idx, row in merged_gdf.iterrows():
            ax.annotate(row['site_no'], (row.geometry.x, row.geometry.y), 
                       xytext=(5, 5), textcoords='offset points', 
                       fontsize=8, ha='left', va='bottom',
                       bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))
        
        # Add basemap
        try:
            # Ensure boundary has valid CRS and convert to Web Mercator for contextily
            if boundary.crs is None:
                _log("Warning: Boundary GeoDataFrame has no CRS, assuming EPSG:4326")
                boundary = boundary.set_crs("EPSG:4326")
            
            if merged_gdf.crs is None:
                _log("Warning: Stations GeoDataFrame has no CRS, assuming EPSG:4326")
                merged_gdf = merged_gdf.set_crs("EPSG:4326")
            
            # Convert to Web Mercator for contextily
            boundary_mercator = boundary.to_crs("EPSG:3857")
            merged_gdf_mercator = merged_gdf.to_crs("EPSG:3857")
            
            # Re-plot with corrected CRS
            ax.clear()
            boundary_mercator.plot(ax=ax, facecolor='none', edgecolor='red', linewidth=2)
            merged_gdf_mercator.plot(ax=ax, color='blue', markersize=30, alpha=0.8)
            
            # Re-add station labels
            for idx, row in merged_gdf_mercator.iterrows():
                ax.annotate(row['site_no'], (row.geometry.x, row.geometry.y), 
                           xytext=(5, 5), textcoords='offset points', 
                           fontsize=8, ha='left', va='bottom',
                           bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))
            
            # Try different basemap providers and CRS formats with robust error handling
            basemap_added = False
            
            basemap_attempts = [
                ("OpenStreetMap.Mapnik", ctx.providers.OpenStreetMap.Mapnik, "EPSG:3857"),
                ("OpenStreetMap.Mapnik", ctx.providers.OpenStreetMap.Mapnik, 3857),
                ("CartoDB.Positron", ctx.providers.CartoDB.Positron, "EPSG:3857"),
                ("CartoDB.Positron", ctx.providers.CartoDB.Positron, 3857),
                ("OpenStreetMap.Mapnik", ctx.providers.OpenStreetMap.Mapnik, None)  # Let contextily auto-detect
            ]
            
            for provider_name, provider, crs_val in basemap_attempts:
                try:
                    if crs_val is None:
                        ctx.add_basemap(ax, source=provider, alpha=0.6)
                    else:
                        ctx.add_basemap(ax, crs=crs_val, source=provider, alpha=0.6)
                    _log(f"Successfully added basemap using {provider_name} with CRS={crs_val}")
                    basemap_added = True
                    break
                except Exception as e:
                    _log(f"Basemap attempt failed with {provider_name} (CRS={crs_val}): {str(e)}")
                    continue
            
            if not basemap_added:
                _log("Warning: Could not add any basemap. Map will be created without background tiles.")
                # Add a simple grid as fallback
                ax.grid(True, alpha=0.3)
                ax.set_facecolor('lightgray')
                
        except Exception as e:
            _log(f"Could not add basemap to figure: {str(e)}")
        
        # Customize the plot
        ax.set_title(f'{area_name} - Daily Discharge Stations\nStation Locations', 
                    fontsize=14, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        # Save the figure
        map_file = os.path.join(figures_dir, f'{area_name}_daily_discharge_map.png')
        plt.savefig(map_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        _log(f"Saved daily discharge map: {map_file}")
        
        # Histogram generation removed - user requested line plots only
        
        # Create interactive map
        try:
            create_daily_interactive_map(merged_gdf, boundary, figures_dir, area_name, _log)
        except Exception as e:
            _log(f"Could not create interactive map: {str(e)}")
        
    except Exception as e:
        _log(f"Error creating daily visualizations: {str(e)}")

def create_daily_interactive_map(stations_gdf, boundary, figures_dir, area_name, log_callback):
    """
    Create an interactive Folium map for daily data stations.
    
    Args:
        stations_gdf: GeoDataFrame of stations with summary statistics
        boundary: Study area boundary
        figures_dir: Directory to save map
        area_name: Name of study area
        log_callback: Function to log messages
    """
    
    def _log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(f"Log: {msg}")
    
    try:
        # Calculate center of boundary
        center_lat = boundary.geometry.centroid.y.iloc[0]
        center_lon = boundary.geometry.centroid.x.iloc[0]
        
        # Create base map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=10,
            tiles='OpenStreetMap'
        )
        
        # Add boundary
        folium.GeoJson(
            boundary.to_json(),
            style_function=lambda x: {
                'fillColor': 'none',
                'color': 'red',
                'weight': 3,
                'fillOpacity': 0
            }
        ).add_to(m)
        
        # Add station markers
        for _, station in stations_gdf.iterrows():
            # Create popup text with statistics
            popup_text = f"""
            <b>Station:</b> {station['site_no']}<br>
            <b>Name:</b> {station.get('station_nm', 'N/A')}<br>
            <b>Type:</b> {station['site_tp_cd']}<br>
            """
            
            if 'mean' in station and pd.notna(station['mean']):
                popup_text += f"<b>Mean Discharge:</b> {station['mean']:.2f} cfs<br>"
            if 'count' in station and pd.notna(station['count']):
                popup_text += f"<b>Data Points:</b> {int(station['count'])}<br>"
            
            # Color based on mean discharge if available
            if 'mean' in station and pd.notna(station['mean']):
                # Normalize color based on discharge value
                max_discharge = stations_gdf['mean'].max() if 'mean' in stations_gdf.columns else 1
                color_intensity = min(station['mean'] / max_discharge, 1.0) if max_discharge > 0 else 0
                color = f"#{int(255 * (1 - color_intensity)):02x}{int(255 * color_intensity):02x}00"
            else:
                color = 'blue'
            
            folium.CircleMarker(
                location=[station.geometry.y, station.geometry.x],
                radius=8,
                popup=popup_text,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7
            ).add_to(m)
        
        # Add measure control
        m.add_child(MeasureControl())
        
        # Save the map
        map_file = os.path.join(figures_dir, f'{area_name}_daily_interactive_map.html')
        m.save(map_file)
        
        _log(f"Saved interactive daily map: {map_file}")
        
    except Exception as e:
        _log(f"Error creating interactive daily map: {str(e)}")

def get_stations_using_dataretrieval(west, east, south, north, selected_station_types, log_callback=None):
    """
    Alternative method to get stations using dataretrieval package.
    This can be more reliable than the USGS inventory URL.
    """
    def _log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(f"Log: {msg}")
    
    try:
        import dataretrieval.nwis as nwis
        
        _log("Attempting to get stations using dataretrieval package...")
        
        # Get sites in the bounding box
        sites_df = nwis.get_sites(
            bbox=(west, south, east, north),
            siteType=selected_station_types,
            hasDataTypeCd='dv'  # Only sites with daily values
        )
        
        if not sites_df.empty:
            _log(f"Found {len(sites_df)} stations using dataretrieval")
            
            # Rename columns to match expected format
            sites_df = sites_df.rename(columns={
                'site_no': 'site_no',
                'station_nm': 'station_nm', 
                'site_tp_cd': 'site_tp_cd',
                'dec_lat_va': 'dec_lat_va',
                'dec_long_va': 'dec_long_va'
            })
            
            # Add missing columns with default values
            if 'agency_cd' not in sites_df.columns:
                sites_df['agency_cd'] = 'USGS'
            if 'coord_datum_cd' not in sites_df.columns:
                sites_df['coord_datum_cd'] = 'NAD83'
                
            return sites_df
        else:
            _log("No stations found using dataretrieval")
            return None
            
    except Exception as e:
        _log(f"Error using dataretrieval to get stations: {e}")
        return None
