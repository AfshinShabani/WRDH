"""
EPA Water Quality Data Downloader

Downloads water quality monitoring data from EPA's Water Quality Portal API.
Supports multiple site types and sample media with geographic filtering using
shapefiles. Generates station maps and processes chemical measurement data.
Developer: Afshin Shabani, PhD
Contact: Afshin.shabani@tetratech.com
Github: AfshinShabani
"""

import geopandas as gpd
import requests
import os
import zipfile
import json
from datetime import datetime
import pandas as pd
from urllib.parse import urlencode
import time
import matplotlib.pyplot as plt
import contextily as ctx

class EPAWaterQualityDownloader:
    def __init__(self):
        self.base_url = "https://www.waterqualitydata.us"
        
        # Available site types from EPA
        self.site_types = [
            "Aggregate groundwater use",
            "Aggregate surface-water-use", 
            "Aggregate water-use establishment",
            "Atmosphere",
            "Estuary",
            "Facility",
            "Glacier",
            "Lake, Reservoir, Impoundment",
            "Land",
            "Spring",
            "Stream",
            "Well"
        ]
        
        # Available sample media types
        self.sample_media = [
            "Water",
            "Air", 
            "Biological",
            "Biological Tissue",
            "Habitat",
            "No media",
            "Other",
            "Sediment",
            "Soil",
            "Tissue"
        ]
        
        # Available data providers
        self.providers = ["NWIS", "STORET"]
    
    def read_shapefile_bounds(self, shapefile_path):
        """
        Read shapefile and extract bounding box coordinates
        
        Args:
            shapefile_path (str): Path to the shapefile
            
        Returns:
            tuple: (min_lon, min_lat, max_lon, max_lat)
        """
        try:
            gdf = gpd.read_file(shapefile_path)
            # Get the total bounds of all features
            bounds = gdf.total_bounds
            min_lon, min_lat, max_lon, max_lat = bounds
            
            print(f"Shapefile bounds: {min_lon:.6f}, {min_lat:.6f}, {max_lon:.6f}, {max_lat:.6f}")
            return min_lon, min_lat, max_lon, max_lat
            
        except Exception as e:
            print(f"Error reading shapefile: {e}")
            return None
    
    def get_data_type_preferences(self):
        """
        Ask user which types of data they want to download
        
        Returns:
            dict: Data type preferences
        """
        print("\nData Types Available:")
        print("1. Station data only (site locations and information)")
        print("2. Result data only (water quality measurements)")
        print("3. Both Station and Result data (recommended)")
        
        data_choice = input("\nSelect data types to download [default: 3]: ").strip()
        
        if data_choice == "1":
            return {"download_stations": True, "download_results": False}
        elif data_choice == "2":
            return {"download_stations": False, "download_results": True}
        else:
            return {"download_stations": True, "download_results": True}
    
    def get_user_preferences(self):
        """
        Get user preferences for data download
        
        Returns:
            dict: User preferences including site types, media, dates
        """
        print("\n=== EPA Water Quality Data Downloader ===")
        print("\nAvailable Site Types:")
        for i, site_type in enumerate(self.site_types, 1):
            print(f"{i}. {site_type}")
        
        # Get site type selection
        site_selection = input("\nEnter site type numbers (comma-separated, or 'all' for all types): ").strip()
        if site_selection.lower() == 'all':
            selected_sites = self.site_types.copy()
        else:
            try:
                indices = [int(x.strip()) - 1 for x in site_selection.split(',')]
                selected_sites = [self.site_types[i] for i in indices if 0 <= i < len(self.site_types)]
            except:
                print("Invalid selection, using all site types")
                selected_sites = self.site_types.copy()
        
        print("\nAvailable Sample Media:")
        for i, media in enumerate(self.sample_media, 1):
            print(f"{i}. {media}")
        
        # Get media selection
        media_selection = input("\nEnter media type numbers (comma-separated, or 'all' for all types): ").strip()
        if media_selection.lower() == 'all':
            selected_media = self.sample_media.copy()
        else:
            try:
                indices = [int(x.strip()) - 1 for x in media_selection.split(',')]
                selected_media = [self.sample_media[i] for i in indices if 0 <= i < len(self.sample_media)]
            except:
                print("Invalid selection, using all media types")
                selected_media = self.sample_media.copy()
        
        # Get date range
        start_date = input("\nEnter start date (MM-DD-YYYY) [default: 01-01-2020]: ").strip()
        if not start_date:
            start_date = "01-01-2020"
        
        end_date = input("Enter end date (MM-DD-YYYY) [default: 12-31-2024]: ").strip()
        if not end_date:
            end_date = "12-31-2024"
        
        # Get providers
        provider_selection = input("\nSelect providers (1=NWIS, 2=STORET, 3=Both) [default: 3]: ").strip()
        if provider_selection == "1":
            selected_providers = ["NWIS"]
        elif provider_selection == "2":
            selected_providers = ["STORET"]
        else:
            selected_providers = ["NWIS", "STORET"]
        
        return {
            'site_types': selected_sites,
            'sample_media': selected_media,
            'start_date': start_date,
            'end_date': end_date,
            'providers': selected_providers
        }
    
    def build_download_url(self, bounds, preferences, data_type="Station"):
        """
        Build the download URL for EPA water quality data
        
        Args:
            bounds (tuple): (min_lon, min_lat, max_lon, max_lat)
            preferences (dict): User preferences
            data_type (str): Type of data to download ("Station" or "Result")
            
        Returns:
            str: Complete download URL
        """
        min_lon, min_lat, max_lon, max_lat = bounds
        bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"
        
        params = {
            'bBox': bbox,
            'siteType': preferences['site_types'],
            'sampleMedia': preferences['sample_media'],
            'startDateLo': preferences['start_date'],
            'startDateHi': preferences['end_date'],
            'providers': preferences['providers'],
            'mimeType': 'csv',
            'zip': 'yes'
        }
        
        # Build URL
        if data_type == "Station":
            url = f"{self.base_url}/data/Station/search"
        else:
            url = f"{self.base_url}/data/Result/search"
        
        return url, params
    
    def download_data(self, url, params, output_filename):
        """
        Download data from EPA Water Quality Portal
        
        Args:
            url (str): API endpoint URL
            params (dict): Query parameters
            output_filename (str): Name for the output file
            
        Returns:
            bool: Success status
        """
        try:
            print(f"\nDownloading data to {output_filename}...")
            print("This may take several minutes depending on data size...")

            # Add headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            # Make the request with extended timeout and headers
            response = requests.get(url, params=params, headers=headers, stream=True, timeout=600)
            response.raise_for_status()

            # Save the file
            with open(output_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Check if it's a zip file and extract if so
            if output_filename.endswith('.zip') or zipfile.is_zipfile(output_filename):
                print("Extracting zip file...")
                with zipfile.ZipFile(output_filename, 'r') as zip_ref:
                    zip_ref.extractall(os.path.dirname(output_filename))

                # Remove the zip file and rename to .zip if it wasn't already
                if not output_filename.endswith('.zip'):
                    zip_name = output_filename + '.zip'
                    os.rename(output_filename, zip_name)
                    output_filename = zip_name

            print(f"? Successfully downloaded: {output_filename}")
            return True

        except requests.exceptions.RequestException as e:
            print(f"? Error downloading data: {e}")
            return False
        except Exception as e:
            print(f"? Unexpected error: {e}")
            return False
    
    def create_output_directory(self, shapefile_path):
        """
        Create output directory based on shapefile name
        
        Args:
            shapefile_path (str): Path to input shapefile
            
        Returns:
            str: Output directory path
        """
        base_name = os.path.splitext(os.path.basename(shapefile_path))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(os.path.dirname(shapefile_path), f"EPA_Data_{base_name}_{timestamp}")
        
        os.makedirs(output_dir, exist_ok=True)
        return output_dir
    
    def save_metadata(self, output_dir, shapefile_path, bounds, preferences, data_type_preferences=None):
        """
        Save metadata about the download
        
        Args:
            output_dir (str): Output directory
            shapefile_path (str): Path to input shapefile
            bounds (tuple): Bounding box coordinates
            preferences (dict): User preferences
            data_type_preferences (dict): Data type preferences
        """
        metadata = {
            'download_timestamp': datetime.now().isoformat(),
            'shapefile_path': shapefile_path,
            'bounding_box': {
                'min_longitude': bounds[0],
                'min_latitude': bounds[1], 
                'max_longitude': bounds[2],
                'max_latitude': bounds[3]
            },
            'selected_site_types': preferences['site_types'],
            'selected_sample_media': preferences['sample_media'],
            'date_range': {
                'start': preferences['start_date'],
                'end': preferences['end_date']
            },
            'data_providers': preferences['providers']
        }
        
        # Add data type preferences if provided
        if data_type_preferences:
            metadata['data_types_downloaded'] = {
                'stations': data_type_preferences.get('download_stations', False),
                'results': data_type_preferences.get('download_results', False)
            }
        
        metadata_file = os.path.join(output_dir, 'download_metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"? Metadata saved: {metadata_file}")
    
    def create_station_shapefile_and_plot(self, station_csv_path, output_dir):
        """
        Create a shapefile for stations and plot them on a street basemap.

        Args:
            station_csv_path (str): Path to the station data CSV file.
            output_dir (str): Directory to save the shapefile and plot.
        """

        try:
            # Read the station data CSV
            print(f"Reading station data from {station_csv_path}...")
            station_data = pd.read_csv(station_csv_path)

            # Extract required columns (include MonitoringLocationIdentifier for station IDs)
            required_columns = ["MonitoringLocationName", "MonitoringLocationIdentifier", "LatitudeMeasure", "LongitudeMeasure"]
            if not all(col in station_data.columns for col in required_columns):
                print("? Required columns are missing in the station data.")
                print(f"Available columns: {list(station_data.columns)}")
                # Try to work with what we have
                missing_cols = [col for col in required_columns if col not in station_data.columns]
                print(f"Missing columns: {missing_cols}")
                return

            # Create a GeoDataFrame
            gdf = gpd.GeoDataFrame(
                station_data,
                geometry=gpd.points_from_xy(station_data["LongitudeMeasure"], station_data["LatitudeMeasure"]),
                crs="EPSG:4326"
            )

            # Save the GeoDataFrame as a shapefile
            shapefile_path = os.path.join(output_dir, "stations.shp")
            gdf.to_file(shapefile_path)
            print(f"? Station shapefile created: {shapefile_path}")

            # Plot the stations on a street basemap using ESRI source (without labels)
            print("Plotting stations on a street basemap without station IDs...")
            fig, ax = plt.subplots(figsize=(12, 10))
            gdf.to_crs(epsg=3857).plot(ax=ax, color="red", markersize=20, label="Water Quality Stations")
            ctx.add_basemap(ax, source=ctx.providers.Esri.WorldStreetMap)
            ax.legend()

            # Save the plot without station IDs
            plot_path_no_names = os.path.join(output_dir, "stations_plot_no_names.png")
            plt.tight_layout()
            plt.savefig(plot_path_no_names, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"? Station plot without IDs saved: {plot_path_no_names}")

            # Plot the stations on a street basemap with station IDs
            print("Plotting stations on a street basemap with station IDs...")
            fig, ax = plt.subplots(figsize=(12, 10))
            
            # Transform to Web Mercator for plotting
            gdf_transformed = gdf.to_crs(epsg=3857)
            gdf_transformed.plot(ax=ax, color="red", markersize=20, label="Water Quality Stations")
            ctx.add_basemap(ax, source=ctx.providers.Esri.WorldStreetMap)
            
            # Add station ID labels using the transformed coordinates
            for idx, row in gdf_transformed.iterrows():
                # Use MonitoringLocationIdentifier for station IDs
                station_id = row["MonitoringLocationIdentifier"]
                x, y = row.geometry.x, row.geometry.y
                
                # Add text with better styling for visibility
                ax.annotate(station_id, 
                           (x, y), 
                           xytext=(5, 5), 
                           textcoords='offset points',
                           fontsize=8, 
                           color='blue',
                           fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7, edgecolor='blue'))
            
            ax.legend()

            # Save the plot with station IDs
            plot_path_with_names = os.path.join(output_dir, "stations_plot_with_station_ids.png")
            plt.tight_layout()
            plt.savefig(plot_path_with_names, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"? Station plot with IDs saved: {plot_path_with_names}")
            
            # Create interactive web map
            try:
                self.create_interactive_epa_map(gdf, output_dir, "EPA Water Quality Stations")
            except Exception as e:
                print(f"?? Error creating interactive map: {str(e)}")

        except Exception as e:
            print(f"? Error creating station shapefile or plot: {e}")
    
    def create_interactive_epa_map(self, stations_gdf, output_dir, title="Water Quality Stations"):
        """Create an interactive web map for EPA stations."""
        try:
            import folium
            from folium.plugins import MeasureControl
            
            # Ensure data is in WGS84 for folium
            if stations_gdf.crs != 'EPSG:4326':
                stations_gdf = stations_gdf.to_crs('EPSG:4326')
            
            # Calculate center
            center_lat = stations_gdf.geometry.y.mean()
            center_lon = stations_gdf.geometry.x.mean()
            
            # Create map
            m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
            
            # Add stations with detailed information
            for idx, row in stations_gdf.iterrows():
                # Create popup content with station information
                popup_content = f"""
                <b>Station ID:</b> {row.get('MonitoringLocationIdentifier', 'N/A')}<br>
                <b>Name:</b> {row.get('MonitoringLocationName', 'N/A')}<br>
                <b>Type:</b> {row.get('MonitoringLocationTypeName', 'N/A')}<br>
                <b>State:</b> {row.get('StateCode', 'N/A')}<br>
                <b>County:</b> {row.get('CountyCode', 'N/A')}<br>
                <b>Organization:</b> {row.get('OrganizationFormalName', 'N/A')}<br>
                <b>Latitude:</b> {row.geometry.y:.6f}<br>
                <b>Longitude:</b> {row.geometry.x:.6f}
                """
                
                # Use different colors for different station types
                station_type = row.get('MonitoringLocationTypeName', 'Unknown')
                if 'Stream' in str(station_type):
                    color = 'blue'
                    icon = 'tint'
                elif 'Lake' in str(station_type) or 'Reservoir' in str(station_type):
                    color = 'lightblue'
                    icon = 'tint'
                elif 'Well' in str(station_type):
                    color = 'brown'
                    icon = 'circle'
                elif 'Spring' in str(station_type):
                    color = 'green'
                    icon = 'leaf'
                else:
                    color = 'gray'
                    icon = 'circle'
                
                folium.Marker(
                    location=[row.geometry.y, row.geometry.x],
                    popup=folium.Popup(popup_content, max_width=400),
                    tooltip=str(row.get('MonitoringLocationIdentifier', 'Station')),
                    icon=folium.Icon(color=color, icon=icon, prefix='fa')
                ).add_to(m)
            
            # Add controls
            m.add_child(MeasureControl())
            folium.LayerControl().add_to(m)
            
            # Add title and legend
            title_html = f'''
            <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
            '''
            m.get_root().html.add_child(folium.Element(title_html))
            
            # Add legend with improved styling - positioned in upper left corner
            legend_html = '''
            <div style="position: fixed; 
                        top: 60px; left: 20px; width: 220px; height: auto; 
                        background-color: rgba(255, 255, 255, 0.95); 
                        border: 2px solid #333; border-radius: 8px;
                        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                        z-index: 9999; 
                        font-size: 14px; padding: 15px; margin: 5px;
                        font-family: Arial, sans-serif;">
            <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 16px; border-bottom: 2px solid #333; padding-bottom: 5px;">Station Types</p>
            <p style="margin: 5px 0; padding: 2px 0;"><i class="fa fa-tint" style="color:blue; margin-right: 8px; width: 16px;"></i> Stream/River</p>
            <p style="margin: 5px 0; padding: 2px 0;"><i class="fa fa-tint" style="color:lightblue; margin-right: 8px; width: 16px;"></i> Lake/Reservoir</p>
            <p style="margin: 5px 0; padding: 2px 0;"><i class="fa fa-circle" style="color:brown; margin-right: 8px; width: 16px;"></i> Well</p>
            <p style="margin: 5px 0; padding: 2px 0;"><i class="fa fa-leaf" style="color:green; margin-right: 8px; width: 16px;"></i> Spring</p>
            <p style="margin: 5px 0; padding: 2px 0;"><i class="fa fa-circle" style="color:gray; margin-right: 8px; width: 16px;"></i> Other</p>
            </div>
            '''
            m.get_root().html.add_child(folium.Element(legend_html))
            
            # Save map
            map_file = os.path.join(output_dir, "interactive_stations_map.html")
            m.save(map_file)
            
            print(f"? Interactive map saved to: {map_file}")
            
        except Exception as e:
            print(f"? Error creating interactive EPA map: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def calculate_sample_statistics(self, result_csv_path, output_dir):
        """
        Calculate the number of available samples and statistics (min, max, average)
        for the 'ResultMeasureValue' column, grouped by specific columns.

        Args:
            result_csv_path (str): Path to the result data CSV file.
            output_dir (str): Directory to save the statistics output.
        """
        import pandas as pd

        try:
            # Read the result data CSV
            print(f"Reading result data from {result_csv_path}...")
            result_data = pd.read_csv(result_csv_path)

            # Required columns for grouping and analysis
            group_columns = [
                "MonitoringLocationIdentifier",
                "ActivityMediaName",
                "ActivityMediaSubdivisionName",
                "CharacteristicName",
                "ResultMeasure/MeasureUnitCode"
            ]

            # Check if required columns exist
            if not all(col in result_data.columns for col in group_columns + ["ResultMeasureValue"]):
                print("? Required columns are missing in the result data.")
                return

            # Convert 'ResultMeasureValue' to numeric, coercing errors to NaN
            result_data["ResultMeasureValue"] = pd.to_numeric(result_data["ResultMeasureValue"], errors="coerce")

            # Group by the specified columns and calculate statistics
            stats = result_data.groupby(group_columns).agg(
                sample_count=("ResultMeasureValue", "count"),
                min_value=("ResultMeasureValue", "min"),
                max_value=("ResultMeasureValue", "max"),
                average_value=("ResultMeasureValue", "mean")
            ).reset_index()

            # Save the statistics to a CSV file
            stats_output_path = os.path.join(output_dir, "sample_statistics.csv")
            stats.to_csv(stats_output_path, index=False)
            print(f"? Sample statistics saved: {stats_output_path}")

        except Exception as e:
            print(f"? Error calculating sample statistics: {e}")
    
    def run(self):
        """
        Main execution function
        """
        print("EPA Water Quality Data Downloader")
        print("=" * 40)
        
        # Get shapefile path
        shapefile_path = input("\nEnter the path to your shapefile: ").strip()
        
        # Remove quotes if present
        shapefile_path = shapefile_path.strip('"\'')
        
        if not os.path.exists(shapefile_path):
            print(f"Error: Shapefile not found at {shapefile_path}")
            return
        
        # Read shapefile bounds
        bounds = self.read_shapefile_bounds(shapefile_path)
        if bounds is None:
            return
        
        # Get user preferences
        preferences = self.get_user_preferences()
        
        # Ask for data type preferences
        data_type_preferences = self.get_data_type_preferences()
        
        # Create output directory
        output_dir = self.create_output_directory(shapefile_path)
        print(f"\nOutput directory: {output_dir}")
        
        # Save metadata
        self.save_metadata(output_dir, shapefile_path, bounds, preferences, data_type_preferences)
        
        # Download station data if selected
        station_success = False
        if data_type_preferences["download_stations"]:
            print("\n" + "="*50)
            print("DOWNLOADING STATION DATA")
            print("="*50)
            print("?? Station data includes: site locations, monitoring information, site characteristics")
            
            station_url, station_params = self.build_download_url(bounds, preferences, "Station")
            station_file = os.path.join(output_dir, "EPA_Stations.zip")
            
            station_success = self.download_data(station_url, station_params, station_file)
        else:
            print("\n??  Skipping Station data download (not requested)")
            station_success = None  # Indicates skipped, not failed
        
        # Download result data if selected
        result_success = False
        if data_type_preferences["download_results"]:
            print("\n" + "="*50)
            print("DOWNLOADING RESULT DATA")
            print("="*50)
            print("?? Result data includes: water quality measurements, analytical results, sample data")
            
            result_url, result_params = self.build_download_url(bounds, preferences, "Result")
            result_file = os.path.join(output_dir, "EPA_Results.zip")
            
            result_success = self.download_data(result_url, result_params, result_file)
        else:
            print("\n??  Skipping Result data download (not requested)")
            result_success = None  # Indicates skipped, not failed
        
        # Summary
        print("\n" + "="*50)
        print("DOWNLOAD SUMMARY")
        print("="*50)
        print(f"Output directory: {output_dir}")
        
        # Display status for each data type
        if station_success is None:
            print(f"Station data: ?? Skipped (not requested)")
        else:
            print(f"Station data: {'? Success' if station_success else '? Failed'}")
            
        if result_success is None:
            print(f"Result data: ?? Skipped (not requested)")
        else:
            print(f"Result data: {'? Success' if result_success else '? Failed'}")
        
        # Check if any downloads were successful
        downloads_completed = (station_success is True) or (result_success is True)
        
        if downloads_completed:
            print(f"\n* Data download completed! Check the output directory:")
            print(f"  {output_dir}")
            
            # Provide information about the downloaded data
            if station_success:
                print(f"\n* Station Data:")
                print(f"   - Contains monitoring site locations and information")
                print(f"   - Useful for mapping and understanding data collection points")
                
            if result_success:
                print(f"\n* Result Data:")
                print(f"   - Contains actual water quality measurements")
                print(f"   - Includes parameters like pH, temperature, dissolved oxygen, etc.")
                print(f"   - This is the primary analytical data for water quality studies")
        else:
            print(f"\n?? No data was successfully downloaded.")
            if station_success is False or result_success is False:
                print("Check error messages above and try again with different parameters.")


def main():
    """
    Main function to run the EPA Water Quality Downloader
    """
    downloader = EPAWaterQualityDownloader()
    
    try:
        downloader.run()
    except KeyboardInterrupt:
        print("\n\nDownload cancelled by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        print("Please check your inputs and try again.")


if __name__ == "__main__":
    main()

