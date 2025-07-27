"""
USGS Core Data Downloader Module

Core functionality for downloading and processing USGS water data. Handles station 
discovery, data retrieval, quality control, and visualization generation. Designed 
as a modular backend that can be used independently of the GUI interface.
Developer: Afshin Shabani, PhD
Contact: Afshin.shabani@tetratech.com
Github: AfshinShabani
"""

import os
import sys
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
import warnings
import folium
from folium.plugins import MeasureControl
import dataretrieval.nwis as nwis

warnings.filterwarnings("ignore")

# Parameter codes and descriptions
PARAMETER_CODES = {
    '00060': 'Discharge (cfs)',
    '00010': 'Temperature (C)',
    '00011': 'Temperature (F)',
    '00065': 'Gage height (ft)'
}

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

class USGSDataDownloader:
    """Core USGS data downloader class that handles all downloading and processing operations."""
    
    def __init__(self, progress_callback=None, log_callback=None, stop_check_callback=None):
        """
        Initialize the downloader.
        
        Args:
            progress_callback: Function to call for progress updates (current, total)
            log_callback: Function to call for logging messages
            stop_check_callback: Function to call to check if operation should stop
        """
        self.progress_callback = progress_callback or (lambda current, total: None)
        self.log_callback = log_callback or print
        self.stop_check_callback = stop_check_callback or (lambda: False)
        self._verbose_errors = True
    
    def _log(self, message):
        """Log a message."""
        self.log_callback(message)
    
    def _update_progress(self, current, total):
        """Update progress."""
        self.progress_callback(current, total)
    
    def _check_stop(self):
        """Check if operation should stop."""
        return self.stop_check_callback()
    
    def download_hourly_data(self, parameter, start_date, end_date, base_path, area_name, 
                           shapefile_path, selected_station_types, options=None):
        """
        Download hourly USGS data.
        
        Args:
            parameter: Parameter code (e.g., '00060')
            start_date: Start date string (YYYY-MM-DD)
            end_date: End date string (YYYY-MM-DD)
            base_path: Base output directory
            area_name: Name of the study area
            shapefile_path: Path to boundary shapefile
            selected_station_types: List of station type codes
            options: Dictionary of options for output control
        """
        if options is None:
            options = {
                'save_hourly': True,
                'save_daily': True,
                'create_plots': True,
                'save_raw': True,
                'create_aerial_map': True,
                'save_urls': True
            }
        
        self._log(f"Starting download for parameter: {parameter} ({PARAMETER_CODES.get(parameter, 'Unknown')})")
        self._log(f"Selected station types: {', '.join(selected_station_types)}")
        
        # Create directory structure
        directories = self._create_directory_structure(base_path, area_name, parameter)
        
        # Create URL file if option is selected
        url_file_path = os.path.join(directories['data_dir'], f'{area_name}_download_urls.txt')
        if options.get('save_urls', False):
            with open(url_file_path, 'w') as url_file:
                url_file.write(f"USGS Data Download URLs for {area_name}, Parameter: {parameter}\n")
                url_file.write(f"Date range: {start_date} to {end_date}\n")
                url_file.write("="*80 + "\n\n")
        
        # Process boundary shapefile and get stations
        stations_gdf, boundary = self._get_stations_in_boundary(
            shapefile_path, selected_station_types, directories['gis_dir'], area_name
        )
        
        if stations_gdf is None or len(stations_gdf) == 0:
            self._log("No stations found for the selected criteria.")
            return
        
        # Download and process data
        successful_stations = self._download_station_data(
            stations_gdf, parameter, start_date, end_date, directories, options, url_file_path
        )
        
        # Create maps if requested
        if options.get('create_aerial_map', False) and len(successful_stations) > 0:
            try:
                self._log("Creating aerial map with stations...")
                # Filter stations to only include successful ones
                map_stations = stations_gdf[stations_gdf['site_no'].isin(successful_stations)]
                
                # Save successful stations shapefile
                final_stations_shapefile = os.path.join(directories['gis_dir'], f"{area_name}_successful_stations.shp")
                map_stations.to_file(final_stations_shapefile)
                self._log(f"Saved successful stations to shapefile: {final_stations_shapefile}")
                
                # Create maps
                self._create_aerial_map(map_stations, boundary, directories['data_dir'], area_name, parameter)
                self._create_interactive_web_map(map_stations, boundary, directories['data_dir'], area_name, parameter)
            except Exception as e:
                self._log(f"Error creating aerial map: {str(e)}")
        
        self._update_progress(100, 100)
        self._log("Download and processing complete!")
    
    def download_daily_data(self, parameter, start_date, end_date, base_path, area_name,
                          shapefile_path, selected_station_types, options=None):
        """
        Download daily USGS data.
        
        Args:
            parameter: Parameter code (e.g., 'dv')
            start_date: Start date string (YYYY-MM-DD)
            end_date: End date string (YYYY-MM-DD)
            base_path: Base output directory
            area_name: Name of the study area
            shapefile_path: Path to boundary shapefile
            selected_station_types: List of station type codes
            options: Dictionary of options for output control
        """
        if options is None:
            options = {
                'save_data': True,
                'create_plots': True,
                'create_summary': True,
                'create_map': True,
                'save_excel': True
            }
        
        self._log(f"Starting download for daily parameter: {parameter} ({DAILY_PARAMETER_CODES.get(parameter, 'Unknown')})")
        self._log(f"Selected station types: {', '.join(selected_station_types)}")
        
        # Create directory structure
        directories = self._create_directory_structure_daily(base_path, area_name, parameter)
        
        # Process boundary shapefile and get stations
        stations_gdf, boundary = self._get_stations_in_boundary(
            shapefile_path, selected_station_types, directories['gis_dir'], area_name
        )
        
        if stations_gdf is None or len(stations_gdf) == 0:
            self._log("No stations found for the selected criteria.")
            return
        
        # Download and process daily data
        successful_stations, summary_data = self._download_daily_station_data(
            stations_gdf, parameter, start_date, end_date, directories, options
        )
        
        # Create summary if requested
        if options.get('create_summary', False) and parameter == 'dv' and len(successful_stations) > 0:
            self._create_daily_summary(summary_data, directories['data_dir'], parameter, options)
        
        # Create maps if requested
        if options.get('create_map', False) and len(successful_stations) > 0:
            self._create_daily_maps(stations_gdf, successful_stations, boundary, 
                                  directories, area_name, parameter)
        
        self._update_progress(100, 100)
        self._log(f"Daily data download and processing complete! Successfully processed {len(successful_stations)} stations.")
    
    def _create_directory_structure(self, base_path, area_name, parameter):
        """Create directory structure for hourly data."""
        directories = {}
        
        # Create root directory with area name
        area_dir = os.path.join(base_path, area_name)
        if not os.path.exists(area_dir):
            os.mkdir(area_dir)
            self._log(f"Created area directory: {area_dir}")
        
        # Create parameter directory
        data_dir = os.path.join(area_dir, parameter)
        if not os.path.exists(data_dir): 
            os.mkdir(data_dir)
            self._log(f"Created parameter directory: {data_dir}")
        
        # Create subdirectories
        subdirs = {
            'gis_dir': 'GIS',
            'csv_dir': 'Csvs',
            'figures_dir': 'Figures',
            'raw_data_dir': 'raw data'
        }
        
        directories['data_dir'] = data_dir
        
        for key, subdir_name in subdirs.items():
            subdir_path = os.path.join(data_dir, subdir_name)
            if not os.path.exists(subdir_path): 
                os.mkdir(subdir_path)
                self._log(f"Created directory: {subdir_path}")
            directories[key] = subdir_path
        
        # Create CSV subdirectories
        csv_subdirs = ['hourly', 'sub-hourly', 'daily']
        for subdir in csv_subdirs:
            subdir_path = os.path.join(directories['csv_dir'], subdir)
            if not os.path.exists(subdir_path):
                os.mkdir(subdir_path)
                self._log(f"Created directory: {subdir_path}")
            directories[f'{subdir.replace("-", "_")}_dir'] = subdir_path
        
        return directories
    
    def _create_directory_structure_daily(self, base_path, area_name, parameter):
        """Create directory structure for daily data."""
        directories = {}
        
        # Create root directory with area name
        area_dir = os.path.join(base_path, area_name)
        if not os.path.exists(area_dir):
            os.mkdir(area_dir)
            self._log(f"Created area directory: {area_dir}")
        
        # Create parameter directory
        data_dir = os.path.join(area_dir, parameter)
        if not os.path.exists(data_dir): 
            os.mkdir(data_dir)
            self._log(f"Created parameter directory: {data_dir}")
        
        # Create subdirectories
        subdirs = {
            'gis_dir': 'GIS',
            'data_dir': 'Data',
            'figures_dir': 'Figures'
        }
        
        directories['main_data_dir'] = data_dir
        
        for key, subdir_name in subdirs.items():
            subdir_path = os.path.join(data_dir, subdir_name)
            if not os.path.exists(subdir_path): 
                os.mkdir(subdir_path)
                self._log(f"Created directory: {subdir_path}")
            directories[key] = subdir_path
        
        return directories
    
    def _get_stations_in_boundary(self, shapefile_path, selected_station_types, gis_dir, area_name):
        """Get USGS stations within the boundary shapefile."""
        self._log("Reading boundary shapefile...")
        boundary = gpd.read_file(shapefile_path)
        boundary = boundary.to_crs('EPSG:4326')
        
        bnds = boundary.bounds
        West = round(bnds['minx'][0], 2)
        East = round(bnds['maxx'][0], 2)
        South = round(bnds['miny'][0], 2)
        North = round(bnds['maxy'][0], 2)
        
        self._log(f"Boundary coordinates: West={West}, East={East}, South={South}, North={North}")
        
        # Download USGS station information
        self._log("Downloading USGS station information...")
        USGS_url = 'https://nwis.waterdata.usgs.gov/nwis/inventory?'
        USGS_url += f'nw_longitude_va={West}&nw_latitude_va={North}'
        USGS_url += f'&se_longitude_va={East}&se_latitude_va={South}'
        USGS_url += '&coordinate_format=decimal_degrees&group_key=NONE&format=sitefile_output'
        USGS_url += '&sitefile_output_format=rdb&column_name=agency_cd&column_name=site_no'
        USGS_url += '&column_name=station_nm&column_name=site_tp_cd&column_name=dec_lat_va'
        USGS_url += '&column_name=dec_long_va&column_name=coord_datum_cd&list_of_search_criteria=lat_long_bounding_box'
        
        ofile_path = os.path.join(gis_dir, f'{area_name}_stations_raw.txt')
        success = fetch_url(ofile_path, USGS_url)
        if not success:
            self._log("Failed to download station information.")
            return None, None
        
        # Parse station information
        self._log("Processing station information...")
        try:
            with open(ofile_path, 'r') as f:
                lines = f.readlines()
            
            # Find the header line
            header_index = 0
            for i, line in enumerate(lines):
                if line.startswith('agency_cd'):
                    header_index = i
                    break
            
            # Create dataframe
            header = lines[header_index].strip().split('\t')
            data_lines = lines[header_index + 2:]
            
            stations_data = []
            for line in data_lines:
                if line.strip() and not line.startswith('#'):
                    parts = line.strip().split('\t')
                    if len(parts) >= 7:
                        stations_data.append(parts)
            
            stations_df = pd.DataFrame(stations_data, columns=header)
            
            # Filter by station type
            self._log(f"Filtering stations to only include types: {selected_station_types}")
            filtered_stations = stations_df[stations_df['site_tp_cd'].isin(selected_station_types)]
            
            if len(filtered_stations) == 0:
                self._log("No stations found for the selected station types.")
                return None, None
            
            self._log(f"Found {len(filtered_stations)} stations matching the selected types.")
            
            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame(
                filtered_stations, 
                geometry=gpd.points_from_xy(
                    filtered_stations['dec_long_va'].astype(float), 
                    filtered_stations['dec_lat_va'].astype(float)
                ),
                crs="EPSG:4326"
            )
            
            # Filter to boundary
            gdf = gpd.sjoin(gdf, boundary, how="inner", predicate="within")
            
            if len(gdf) == 0:
                self._log("No stations found within the boundary area.")
                return None, None
            
            self._log(f"Found {len(gdf)} stations within the boundary area.")
            
            # Save stations shapefile
            stations_shapefile = os.path.join(gis_dir, f"{area_name}_stations.shp")
            gdf.to_file(stations_shapefile)
            self._log(f"Saved stations to shapefile: {stations_shapefile}")
            
            return gdf, boundary
            
        except Exception as e:
            self._log(f"Error processing station information: {str(e)}")
            if self._verbose_errors:
                self._log(traceback.format_exc())
            return None, None
    
    def _download_station_data(self, stations_gdf, parameter, start_date, end_date, 
                             directories, options, url_file_path):
        """Download and process hourly station data."""
        station_ids = stations_gdf['site_no'].tolist()
        total_stations = len(station_ids)
        self._update_progress(0, total_stations)
        
        # Create URLs for data download
        urls = []
        for site_id in station_ids:
            url = 'https://nwis.waterservices.usgs.gov/nwis/iv/?'
            url += f'sites={site_id}&parameterCd={parameter}'
            url += f'&startDT={start_date}T00:00:00.000-04:00&endDT={end_date}T23:59:59.999-04:00'
            url += '&siteStatus=all&format=rdb'
            urls.append((site_id, url))
            
            # Add URL to file if option enabled
            if options.get('save_urls', False):
                with open(url_file_path, 'a') as url_file:
                    url_file.write(f"Station {site_id}: {url}\n")
        
        # Download data
        self._log(f"Downloading data for {total_stations} stations...")
        
        def download_station_data(args):
            if self._check_stop():
                return None
            
            site_id, url = args
            try:
                data_file = os.path.join(directories['raw_data_dir'], f"{site_id}.txt")
                success = fetch_url(data_file, url)
                return {'site_id': site_id, 'success': success, 'data_file': data_file}
            except Exception as e:
                if self._verbose_errors:
                    self._log(f"Error downloading data for station {site_id}: {str(e)}")
                return {'site_id': site_id, 'success': False, 'error': str(e)}
        
        # Use ThreadPool for parallel downloads
        successful_downloads = 0
        with ThreadPool(processes=min(8, os.cpu_count() or 4)) as pool:
            for i, result in enumerate(pool.imap_unordered(download_station_data, urls)):
                if self._check_stop():
                    pool.terminate()
                    self._log("Download stopped by user.")
                    return []
                
                if result and result.get('success'):
                    successful_downloads += 1
                
                self._update_progress(i + 1, total_stations)
                self._log(f"Downloaded {i + 1}/{total_stations} stations...")
        
        self._log(f"Successfully downloaded data for {successful_downloads} out of {total_stations} stations.")
        
        # Process downloaded data
        if successful_downloads > 0:
            return self._process_hourly_data(stations_gdf, urls, directories, options, parameter)
        else:
            self._log("No data was successfully downloaded.")
            return []
    
    def _process_hourly_data(self, stations_gdf, urls, directories, options, parameter):
        """Process the downloaded hourly data files."""
        self._log("Processing downloaded data...")
        successful_stations = []
        processed_count = 0
        
        for site_id, url in urls:
            if self._check_stop():
                self._log("Processing stopped by user.")
                break
            
            data_file = os.path.join(directories['raw_data_dir'], f"{site_id}.txt")
            if not os.path.exists(data_file):
                continue
            
            try:
                # Read and process data file
                with open(data_file, 'r') as file:
                    lines = file.readlines()
                    lines = [line for line in lines if not line.startswith('#')]
                    
                    if not lines:
                        self._log(f"No data for station {site_id}")
                        continue
                    
                    df = pd.read_csv(StringIO(''.join(lines)), sep='\t', skiprows=1, on_bad_lines='skip')
                    
                    if len(df.columns) < 4:
                        self._log(f"Insufficient data columns for station {site_id}")
                        continue
                
                # Get station name
                station_name = site_id
                try:
                    station_name = stations_gdf.loc[stations_gdf['site_no'] == site_id, 'station_nm'].values[0]
                except:
                    self._log(f"Could not find station name for {site_id}, using ID instead.")
                
                # Process data based on parameter
                df, value_col, ylabel = self._prepare_dataframe_columns(df, parameter, site_id)
                if df is None:
                    continue
                
                # Convert and clean data
                df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
                df = df.dropna(subset=[value_col])
                
                if len(df) == 0:
                    self._log(f"No valid data left after conversion for station {site_id}")
                    continue
                
                df['Date'] = pd.to_datetime(df['Date'])
                
                # Save processed data
                self._save_hourly_data_files(df, site_id, directories, options, value_col)
                
                # Create plots
                if options.get('create_plots', False):
                    self._create_station_plot(df, site_id, station_name, value_col, ylabel, 
                                            directories['figures_dir'], parameter)
                
                successful_stations.append(site_id)
                processed_count += 1
                
            except Exception as e:
                if self._verbose_errors:
                    self._log(f"Error processing data for station {site_id}: {str(e)}")
                    self._log(traceback.format_exc())
        
        self._log(f"Successfully processed data for {processed_count} stations.")
        return successful_stations
    
    def _prepare_dataframe_columns(self, df, parameter, site_id):
        """Prepare dataframe columns based on parameter type."""
        if parameter == '00060':
            if len(df.columns) >= 6:
                df.columns = ['Organization', 'Station', 'Date', 'Time Zone', 'Discharge (cfs)', 'Quality flag'] + [f"Column{i+7}" for i in range(len(df.columns)-6)]
                return df, 'Discharge (cfs)', 'Discharge (ft$^{3}$/s)'
        elif parameter == '00010':
            if len(df.columns) >= 6:
                df.columns = ['Organization', 'Station', 'Date', 'Time Zone', 'Temperature (C)', 'Quality flag'] + [f"Column{i+7}" for i in range(len(df.columns)-6)]
                return df, 'Temperature (C)', 'Temperature (C)'
        elif parameter == '00011':
            if len(df.columns) >= 6:
                df.columns = ['Organization', 'Station', 'Date', 'Time Zone', 'Temperature (F)', 'Quality flag'] + [f"Column{i+7}" for i in range(len(df.columns)-6)]
                return df, 'Temperature (F)', 'Temperature (F)'
        elif parameter == '00065':
            if len(df.columns) >= 6:
                df.columns = ['Organization', 'Station', 'Date', 'Time Zone', 'Gage height (ft)', 'Quality flag'] + [f"Column{i+7}" for i in range(len(df.columns)-6)]
                return df, 'Gage height (ft)', 'Gage height (ft)'
        else:
            # Generic parameter handling
            if len(df.columns) >= 6:
                cols = ['Organization', 'Station', 'Date', 'Time Zone', f'Value ({parameter})', 'Quality flag'] + [f"Column{i+7}" for i in range(len(df.columns)-6)]
                df.columns = cols
                return df, f'Value ({parameter})', f'Value ({parameter})'
        
        self._log(f"Column count mismatch for station {site_id}. Expected 6, got {len(df.columns)}")
        return None, None, None
    
    def _save_hourly_data_files(self, df, site_id, directories, options, value_col):
        """Save hourly data files in various formats."""
        # Daily data
        if options.get('save_daily', False):
            df_daily = df.copy()
            df_daily.set_index('Date', inplace=True)
            numeric_cols = df_daily.select_dtypes(include=[np.number]).columns
            df_daily = df_daily[numeric_cols].resample('1D').mean()
            daily_file = os.path.join(directories['daily_dir'], f"{site_id}.csv")
            df_daily.to_csv(daily_file)
            self._log(f"Saved daily data for station {site_id}")
        
        # Hourly data
        if options.get('save_hourly', False):
            df_hourly = df.copy()
            df_hourly.set_index('Date', inplace=True)
            numeric_cols = df_hourly.select_dtypes(include=[np.number]).columns
            df_hourly = df_hourly[numeric_cols].resample('1H').mean()
            hourly_file = os.path.join(directories['hourly_dir'], f"{site_id}.csv")
            df_hourly.to_csv(hourly_file)
            self._log(f"Saved hourly data for station {site_id}")
        
        # Sub-hourly data
        if options.get('save_raw', False):
            sub_hourly_file = os.path.join(directories['sub_hourly_dir'], f"{site_id}.csv")
            df.to_csv(sub_hourly_file, index=False)
            self._log(f"Saved sub-hourly data for station {site_id}")
    
    def _create_station_plot(self, df, site_id, station_name, value_col, ylabel, figures_dir, parameter):
        """Create a plot for a single station."""
        try:
            plt.figure(figsize=(10, 6))
            
            # Plot data by quality flag if available
            if 'Quality flag' in df.columns:
                for item in df['Quality flag'].unique():
                    if pd.isna(item) or item == 'A:e':
                        continue
                    
                    sub_df = df.loc[df['Quality flag'] == item]
                    
                    # Set label and color based on quality flag
                    label = item
                    if item == 'A':
                        label = 'Approved'
                        color = 'blue'
                    elif item == 'P':
                        label = 'Provisional'
                        color = 'green'
                    else:
                        color = 'grey'
                    
                    plt.plot(sub_df['Date'], sub_df[value_col], label=label, color=color)
            else:
                plt.plot(df['Date'], df[value_col], label='Data')
            
            plt.ylabel(ylabel, fontsize=12.0)
            plt.xlabel('Date')
            plt.xticks(rotation=45)
            plt.title(f"{station_name}\n{site_id}")
            plt.grid(True)
            plt.tight_layout()
            
            if len(plt.gca().get_legend_handles_labels()[0]) > 0:
                plt.legend(loc='upper right')
            
            plt_file = os.path.join(figures_dir, f"{site_id}_{station_name}.jpg")
            plt.savefig(plt_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            self._log(f"Created plot for station {site_id}")
        except Exception as e:
            self._log(f"Error creating plot for station {site_id}: {str(e)}")
            if self._verbose_errors:
                self._log(traceback.format_exc())
    
    def _download_daily_station_data(self, stations_gdf, parameter, start_date, end_date, 
                                   directories, options):
        """Download and process daily station data."""
        station_ids = stations_gdf['site_no'].tolist()
        total_stations = len(station_ids)
        self._update_progress(0, total_stations)
        
        self._log(f"Downloading daily data for {total_stations} stations...")
        
        # Initialize summary data lists
        summary_data = {
            'S_obs': [],
            'E_obs': [],
            'LENG': [],
            'Min_obs': [],
            'Max_obs': [],
            'USGS_st': [],
            'Discharge': []
        }
        
        successful_stations = []
        
        for index, site_id in enumerate(station_ids):
            if self._check_stop():
                self._log("Download stopped by user.")
                break
            
            try:
                self._log(f"Processing station {site_id} ({index+1}/{total_stations})")
                
                # Get data using dataretrieval
                df = nwis.get_record(sites=site_id, service=parameter, start=start_date, end=end_date)
                
                if len(df) > 0:
                    # Convert timezone
                    df.index = df.index.tz_convert("US/Central")
                    df.index = df.index.tz_localize(None)
                    df.index = pd.to_datetime(df.index.date)
                    
                    # Process based on parameter
                    if parameter == 'dv':
                        if '00060_Mean' in df.columns:
                            # Update summary data
                            summary_data['LENG'].append(len(df['00060_Mean'].dropna(how='any')))
                            summary_data['Min_obs'].append(np.min(df['00060_Mean']))
                            summary_data['Max_obs'].append(np.max(df['00060_Mean']))
                            summary_data['S_obs'].append(np.min(df.index))
                            summary_data['E_obs'].append(np.max(df.index))
                            
                            # Create plot if requested
                            if options.get('create_plots', False):
                                self._create_daily_plot(df, site_id, directories['figures_dir'])
                        else:
                            self._log(f"No flow data found for station {site_id}")
                            continue
                    
                    # Save data if requested
                    if options.get('save_data', False):
                        # Create merged dataframe for full date range
                        DF = pd.DataFrame()
                        DF.index = pd.date_range(start_date, end_date)
                        DF1 = pd.merge(DF, df, left_index=True, right_index=True)
                        summary_data['Discharge'].append(DF1)
                        
                        # Save to CSV
                        df.to_csv(os.path.join(directories['data_dir'], f"{site_id}.txt"), sep='\t')
                        self._log(f"Saved data for station {site_id}")
                    
                    summary_data['USGS_st'].append(site_id)
                    successful_stations.append(site_id)
                else:
                    self._log(f"No data found for station {site_id}")
                
                self._update_progress(index + 1, total_stations)
                
            except Exception as e:
                self._log(f"Error processing station {site_id}: {str(e)}")
                if self._verbose_errors:
                    self._log(traceback.format_exc())
        
        return successful_stations, summary_data
    
    def _create_daily_plot(self, df, site_id, figures_dir):
        """Create a plot for daily data."""
        try:
            plt.figure(figsize=(10, 6))
            plt.plot(df.index, df['00060_Mean'])
            plt.ylabel('Discharge (cfs)', fontsize=12.0)
            plt.xticks(rotation=45)
            plt.xlabel('Time (day)')
            plt.title(f'Station ID: {site_id}')
            plt.grid(True)
            plt.tight_layout()
            
            plt_file = os.path.join(figures_dir, f"{site_id}_flow.jpg")
            plt.savefig(plt_file, dpi=300, bbox_inches='tight')
            plt.close()
            self._log(f"Created plot for station {site_id}")
        except Exception as e:
            self._log(f"Error creating plot for station {site_id}: {str(e)}")
    
    def _create_daily_summary(self, summary_data, data_dir, parameter, options):
        """Create summary for daily data."""
        try:
            self._log("Creating summary...")
            
            summary = pd.DataFrame({
                'Number': [x for x in range(1, len(summary_data['USGS_st']) + 1)],
                'Station': summary_data['USGS_st'],
                'Starting Observation': summary_data['S_obs'],
                'Ending Observation': summary_data['E_obs'],
                'Length of record': summary_data['LENG'],
                'Minimum Discharge': summary_data['Min_obs'],
                'Maximum Discharge': summary_data['Max_obs']
            })
            
            # Save Excel summary if requested
            if options.get('save_excel', False):
                writer = pd.ExcelWriter(os.path.join(data_dir, f'{parameter}.xlsx'), engine='xlsxwriter')
                
                # Combine discharge data if available
                if summary_data['Discharge']:
                    try:
                        Disch = pd.concat(summary_data['Discharge'], axis=1)
                        frames = {'Summary': summary, 'All Data': Disch}
                    except:
                        frames = {'Summary': summary}
                else:
                    frames = {'Summary': summary}
                
                # Write each sheet
                for sheet, frame in frames.items():
                    frame.to_excel(writer, sheet_name=sheet)
                
                writer.close()
                self._log(f"Saved Excel summary to {os.path.join(data_dir, f'{parameter}.xlsx')}")
        
        except Exception as e:
            self._log(f"Error creating summary: {str(e)}")
            if self._verbose_errors:
                self._log(traceback.format_exc())
    
    def _create_daily_maps(self, stations_gdf, successful_stations, boundary, 
                         directories, area_name, parameter):
        """Create maps for daily data."""
        try:
            self._log("Creating maps with stations...")
            
            # Filter stations to only those with data
            map_stations = stations_gdf[stations_gdf['site_no'].isin(successful_stations)]
            
            # Save successful stations shapefile
            final_stations_shapefile = os.path.join(directories['gis_dir'], f"{area_name}_successful_stations.shp")
            map_stations.to_file(final_stations_shapefile)
            self._log(f"Saved successful stations to shapefile: {final_stations_shapefile}")
            
            if len(map_stations) > 0:
                # Create static map
                fig, ax = plt.subplots(figsize=(12, 10))
                
                # Ensure both GeoDataFrames have valid CRS and are in the same projection
                # Convert to Web Mercator (EPSG:3857) for contextily compatibility
                try:
                    # Check if boundary has a valid CRS
                    if boundary.crs is None:
                        self._log("Warning: Boundary GeoDataFrame has no CRS, assuming EPSG:4326")
                        boundary = boundary.set_crs("EPSG:4326")
                    
                    if map_stations.crs is None:
                        self._log("Warning: Stations GeoDataFrame has no CRS, assuming EPSG:4326")
                        map_stations = map_stations.set_crs("EPSG:4326")
                    
                    # Convert to Web Mercator for contextily
                    boundary_mercator = boundary.to_crs("EPSG:3857")
                    map_stations_mercator = map_stations.to_crs("EPSG:3857")
                    
                except Exception as crs_error:
                    self._log(f"CRS conversion error: {str(crs_error)}")
                    # Fallback: assume WGS84 and convert
                    boundary = boundary.set_crs("EPSG:4326")
                    map_stations = map_stations.set_crs("EPSG:4326")
                    boundary_mercator = boundary.to_crs("EPSG:3857")
                    map_stations_mercator = map_stations.to_crs("EPSG:3857")
                
                boundary_mercator.plot(ax=ax, facecolor='none', edgecolor='red', linewidth=2, alpha=0.7)
                map_stations_mercator.plot(ax=ax, color='blue', markersize=50, marker='o')
                
                # Add station labels
                for idx, row in map_stations_mercator.iterrows():
                    plt.annotate(
                        row['site_no'],
                        xy=(row.geometry.x, row.geometry.y),
                        xytext=(3, 3),
                        textcoords="offset points",
                        fontsize=8
                    )
                
                # Ensure axis extent is set to Web Mercator bounds
                minx, miny, maxx, maxy = boundary_mercator.total_bounds
                ax.set_xlim(minx, maxx)
                ax.set_ylim(miny, maxy)
                
                # Add basemap - use explicit Web Mercator CRS with multiple fallback options
                basemap_added = False
                
                # Try different basemap providers and CRS formats
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
                            ctx.add_basemap(ax, source=provider, zoom=12)
                        else:
                            ctx.add_basemap(ax, source=provider, zoom=12, crs=crs_val)
                        self._log(f"Successfully added basemap using {provider_name} with CRS={crs_val}")
                        basemap_added = True
                        break
                    except Exception as e:
                        self._log(f"Basemap attempt failed with {provider_name} (CRS={crs_val}): {str(e)}")
                        continue
                
                if not basemap_added:
                    self._log("Warning: Could not add any basemap. Map will be created without background tiles.")
                    # Add a simple grid as fallback
                    ax.grid(True, alpha=0.3)
                    ax.set_facecolor('lightgray')
                
                # Set title
                if parameter == 'dv' or parameter=='00060':
                    plt.title(f"{area_name} - USGS Stations for streamflow")
                else:
                    plt.title(f"{area_name} - USGS Stations for {parameter}")
                ax.set_axis_off()
                
                # Save map
                map_file = os.path.join(directories['figures_dir'], f"{area_name}_stations_map.png")
                plt.savefig(map_file, dpi=300, bbox_inches='tight')
                plt.close()
                
                self._log(f"Created map: {map_file}")
                
                # Create interactive web map
                self._create_interactive_web_map(map_stations, boundary, directories['main_data_dir'], area_name, parameter)
                
        except Exception as e:
            self._log(f"Error creating map: {str(e)}")
    
    def _create_aerial_map(self, stations_gdf, boundary_gdf, output_dir, area_name, parameter):
        """Create a map with aerial imagery showing stations."""
        try:
            fig, ax = plt.subplots(figsize=(12, 10))
            
            # Ensure both GeoDataFrames have valid CRS and are in the same projection
            # Convert to Web Mercator (EPSG:3857) for contextily compatibility
            try:
                # Check if boundary_gdf has a valid CRS
                if boundary_gdf.crs is None:
                    self._log("Warning: Boundary GeoDataFrame has no CRS, assuming EPSG:4326")
                    boundary_gdf = boundary_gdf.set_crs("EPSG:4326")
                
                # Convert to Web Mercator for contextily
                boundary_gdf_mercator = boundary_gdf.to_crs("EPSG:3857")
                stations_gdf_mercator = stations_gdf.to_crs("EPSG:3857")
                
            except Exception as crs_error:
                self._log(f"CRS conversion error: {str(crs_error)}")
                # Fallback: assume WGS84 and convert
                boundary_gdf = boundary_gdf.set_crs("EPSG:4326")
                stations_gdf = stations_gdf.set_crs("EPSG:4326")
                boundary_gdf_mercator = boundary_gdf.to_crs("EPSG:3857")
                stations_gdf_mercator = stations_gdf.to_crs("EPSG:3857")
            
            # Plot boundary
            boundary_gdf_mercator.plot(ax=ax, facecolor='none', edgecolor='red', linewidth=2, alpha=0.7)
            
            # Plot stations
            stations_gdf_mercator.plot(ax=ax, color='blue', markersize=50, marker='o')
            
            # Add station labels
            for idx, row in stations_gdf_mercator.iterrows():
                plt.annotate(
                    row['site_no'],
                    xy=(row.geometry.x, row.geometry.y),
                    xytext=(3, 3),
                    textcoords="offset points",
                    fontsize=8
                )
            
            # Ensure axis extent is set to Web Mercator bounds
            minx, miny, maxx, maxy = boundary_gdf_mercator.total_bounds
            ax.set_xlim(minx, maxx)
            ax.set_ylim(miny, maxy)
            
            # Add basemap - use explicit Web Mercator CRS with multiple fallback options
            basemap_added = False
            
            # Try different basemap providers and CRS formats
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
                        ctx.add_basemap(ax, source=provider, zoom=12)
                    else:
                        ctx.add_basemap(ax, source=provider, zoom=12, crs=crs_val)
                    self._log(f"Successfully added basemap using {provider_name} with CRS={crs_val}")
                    basemap_added = True
                    break
                except Exception as e:
                    self._log(f"Basemap attempt failed with {provider_name} (CRS={crs_val}): {str(e)}")
                    continue
            
            if not basemap_added:
                self._log("Warning: Could not add any basemap. Map will be created without background tiles.")
                # Add a simple grid as fallback
                ax.grid(True, alpha=0.3)
                ax.set_facecolor('lightgray')
            
            # Set title
            if parameter == '00060' or parameter=='dv':
                plt.title(f"{area_name} - USGS Stations for Streamflow")
            else: 
                plt.title(f"{area_name} - USGS Stations for {parameter}")
            
            ax.set_axis_off()
            
            # Save map
            map_file = os.path.join(output_dir, 'Figures', f"{area_name}_stations_map.png")
            plt.savefig(map_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            self._log(f"Created aerial map: {map_file}")
            
        except Exception as e:
            self._log(f"Error creating aerial map: {str(e)}")
            if self._verbose_errors:
                self._log(traceback.format_exc())
    
    def _create_interactive_web_map(self, stations_gdf, boundary_gdf, output_dir, area_name, parameter):
        """Create an interactive web map with stations and boundary."""
        try:
            # Calculate center
            center_lat = stations_gdf.geometry.y.mean()
            center_lon = stations_gdf.geometry.x.mean()
            
            # Create map
            m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
            
            # Add boundary
            folium.GeoJson(
                boundary_gdf,
                name='Boundary',
                style_function=lambda x: {
                    'fillColor': 'transparent',
                    'color': 'red',
                    'weight': 2
                }
            ).add_to(m)
            
            # Add stations
            for idx, row in stations_gdf.iterrows():
                folium.Marker(
                    location=[row.geometry.y, row.geometry.x],
                    popup=f"Station ID: {row['site_no']}<br>Name: {row['station_nm']}<br>Type: {row['site_tp_cd']}",
                    tooltip=row['site_no'],
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            
            # Add controls
            m.add_child(MeasureControl())
            folium.LayerControl().add_to(m)
            
            # Save map
            map_file = os.path.join(output_dir, 'Figures', f"{area_name}_interactive_map.html")
            m.save(map_file)
            
            self._log(f"Created interactive web map: {map_file}")
            
        except Exception as e:
            self._log(f"Error creating interactive web map: {str(e)}")
            if self._verbose_errors:
                self._log(traceback.format_exc())
