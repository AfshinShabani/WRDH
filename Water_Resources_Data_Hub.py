"""
Water Resources Data Hub (WRDH) - Main Application

A desktop application for downloading and analyzing hydrology and water quality observations
from USGS, NOAA, and EPA sources. Features a tabbed GUI interface with interactive 
boundary drawing, automated data processing, and built-in visualization capabilities.

Developer: Afshin Shabani, PhD
Contact: Afshin.shabani@tetratech.com
Github: AfshinShabani
"""

# Standard library imports
import http.server
import json
import os
import re
import shutil
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import webbrowser
from datetime import datetime
from importlib.util import spec_from_file_location, module_from_spec
from io import StringIO
from multiprocessing.pool import ThreadPool
from pathlib import Path
from urllib.parse import urljoin
import tkinter.simpledialog as simpledialog

# Third-party imports
import contextily as ctx
import datetime as dt
import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
import tkinter as tk
import warnings
from folium.plugins import MeasureControl
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from PIL import Image, ImageTk
from tkcalendar import DateEntry
from tkinter import ttk, filedialog, messagebox

# Try to import ThemedTk for better styling, fall back to regular Tk if not available
try:
    from ttkthemes import ThemedTk
except ImportError:
    ThemedTk = None

# Suppress warnings from pandas and geopandas
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")

# Import the modular downloader functions
# from usgs_hourly_downloader import download_usgs_hourly_data
from usgs_daily_downloader import download_usgs_daily_data

# Import the core downloader module
from usgs_core_downloader import USGSDataDownloader

# Default path - can be changed via the GUI
DEFAULT_PATH = ""

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
                time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error (attempt {attempt + 1}/{max_retries}) for {entry}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}) for {entry}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
        except Exception as e:
            print(f"Unexpected error (attempt {attempt + 1}/{max_retries}) downloading {entry}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
    
    print(f"Failed to download after {max_retries} attempts: {entry}")
    return False

class SplashScreen:
    """Simple splash screen showing only the WRDH.png logo."""
    
    def __init__(self):
        self.splash = tk.Tk()
        
        # Configure splash window
        self.splash.overrideredirect(True)  # Remove title bar
        self.splash.resizable(False, False)
        
        # Set size and center on screen - twice as big (1000x700)
        width, height = 1000, 700
        screen_width = self.splash.winfo_screenwidth()
        screen_height = self.splash.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        self.splash.geometry(f"{width}x{height}+{x}+{y}")
        
        # Simple light gray background to make the logo more visible
        self.splash.configure(bg='#f5f5f5')
        
        # Try to load and display only WRDH.png logo
        self.icon_photo = None
        try:
            # Multiple paths to check for WRDH.png
            possible_paths = []
            
            # First try sys.WRDH directory (set by runtime hook)
            wrdh_dir = getattr(sys, 'WRDH', None)
            if wrdh_dir:
                possible_paths.append(os.path.join(wrdh_dir, "WRDH.png"))
            
            # Try current working directory
            possible_paths.append(os.path.join(os.getcwd(), "WRDH.png"))
            
            # Try executable directory if frozen
            if getattr(sys, 'frozen', False):
                exe_dir = os.path.dirname(sys.executable)
                possible_paths.append(os.path.join(exe_dir, "WRDH.png"))
            
            # Try script directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            possible_paths.append(os.path.join(script_dir, "WRDH.png"))
            
            # Try _MEIPASS directory if PyInstaller
            if hasattr(sys, '_MEIPASS'):
                possible_paths.append(os.path.join(sys._MEIPASS, "WRDH.png"))
            
            icon_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    icon_path = path
                    break
            
            if icon_path:
                # Load original image
                icon_image = Image.open(icon_path)
                
                # Get original dimensions and scale to fill the entire splash screen
                original_width, original_height = icon_image.size
                
                scale_factor = max(1000 / original_width, 700 / original_height)  # Fill the entire splash
                
                new_width = int(original_width * scale_factor)
                new_height = int(original_height * scale_factor)
                
                # Resize to fill the splash screen
                icon_image = icon_image.resize((new_width, new_height), Image.Resampling.LANCZOS)
                
                # Create PhotoImage with the splash window as master
                self.icon_photo = ImageTk.PhotoImage(icon_image, master=self.splash)
                
                # Create label without border, filling the entire window
                icon_label = tk.Label(self.splash, image=self.icon_photo, bg='#f5f5f5', bd=0, highlightthickness=0)
                icon_label.place(x=0, y=0, width=1000, height=700)
            else:
                # Show fallback message
                fallback_label = tk.Label(
                    self.splash,
                    text="WRDH.png not found\nChecked locations:\n" + "\n".join(possible_paths[:3]),
                    font=("Arial", 16, "bold"),
                    bg='#f5f5f5',
                    fg='#333333',
                    justify='center'
                )
                fallback_label.place(relx=0.5, rely=0.5, anchor='center')
        except Exception as e:
            # Show error message
            error_label = tk.Label(
                self.splash,
                text=f"Error loading WRDH.png:\n{str(e)}",
                font=("Arial", 16),
                bg='#f5f5f5',
                fg='red',
                justify='center'
            )
            error_label.place(relx=0.5, rely=0.5, anchor='center')
        
        # Keep splash on top
        self.splash.attributes('-topmost', True)
    
    def show(self):
        """Show the splash screen."""
        self.splash.update()
    
    def update_message(self, message):
        """Update method - simplified splash doesn't show messages."""
        self.splash.update()
    
    def destroy(self):
        """Close the splash screen."""
        try:
            self.splash.destroy()
        except Exception:
            pass


class USGSDataDownloaderApp:
    def __init__(self, root):
        """Initialize the application."""
        self.root = root
        self.root.title("WRDH")
        self.root.geometry("1000x800")
        self.root.minsize(900, 700)
        
        # Set sys.WRDH if not already set (for script mode)
        if not hasattr(sys, 'WRDH'):
            sys.WRDH = os.path.dirname(os.path.abspath(__file__))
        
        # Set up NOAA shapefile default path first (needed for icon loading)
        self._setup_noaa_default_path()
        
        # Set application icon
        self.icon_path = None
        self.icon_photo = None
        try:
            # Get the WRDH directory using sys.WRDH attribute (set by runtime hook)
            wrdh_dir = getattr(sys, 'WRDH', os.getcwd())
            
            # First try to load from WRDH default directory (if it exists)
            default_icon_path = None
            if hasattr(self, 'noaa_default_dir'):
                default_icon_path = os.path.join(self.noaa_default_dir, "WRDH.png")
            
            # Try to load PNG icon - prioritize user WRDH folder, then application WRDH folder
            png_icon_path = None
            if default_icon_path and os.path.exists(default_icon_path):
                png_icon_path = default_icon_path
            else:
                fallback_icon_path = os.path.join(wrdh_dir, "WRDH.png")
                if os.path.exists(fallback_icon_path):
                    png_icon_path = fallback_icon_path
            
            if png_icon_path:
                # Load PNG icon for PhotoImage
                image = Image.open(png_icon_path)
                image = image.resize((32, 32), Image.Resampling.LANCZOS)
                self.icon_photo = ImageTk.PhotoImage(image)
                self.root.iconphoto(True, self.icon_photo)
                self.icon_path = png_icon_path
            else:
                # Fallback to ICO icon if PNG not found (look in executable directory for ICO)
                if getattr(sys, 'frozen', False):
                    exe_dir = os.path.dirname(sys.executable)
                    ico_icon_path = os.path.join(exe_dir, "WRDH.ico")
                else:
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    ico_icon_path = os.path.join(script_dir, "WRDH.ico")
                
                if os.path.exists(ico_icon_path):
                    self.icon_path = ico_icon_path
                    self.root.iconbitmap(ico_icon_path)
        except Exception as e:
            # If icon loading fails, continue without it
            pass
        
        # Variables
        self.shapefile_path_var = tk.StringVar(value="")
        self.parameter_var = tk.StringVar(value="00060")
        self.start_date_var = tk.StringVar(value="2020-1-01")
        self.end_date_var = tk.StringVar(value="2021-01-01")
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.log_text = ""
        self.area_name_var = tk.StringVar(value="")  # Will be derived from shapefile name
        self.base_path_var = tk.StringVar(value=DEFAULT_PATH)
        
        # Station type selection variables
        self.station_type_vars = {
            'Surface Water': tk.BooleanVar(value=True),
            'Ground Water': tk.BooleanVar(value=False),
            'Spring': tk.BooleanVar(value=False),
            'Atmospheric': tk.BooleanVar(value=False)
        }
        
        # Which station types are selectable
        self.station_type_selectable = {
            'Surface Water': True,
            'Ground Water': False,
            'Spring': False,
            'Atmospheric': False
        }
          # Download task variables
        self.download_thread = None
        self.stop_download = False
        self._verbose_errors = True  # Added for error handling in the download process
        
        # Initialize the core downloader
        self.core_downloader = USGSDataDownloader(
            progress_callback=self._update_progress,
            log_callback=self._log,
            stop_check_callback=lambda: self.stop_download
        )
        
        # Define the special interval variable
        self.noaa_special_interval_var = tk.StringVar(value="Hourly")
        
        # Define the datum variable
        self.noaa_datum_var = tk.StringVar(value="MLLW")
        
        # EPA tab variables
        self.epa_site_type_vars = {}
        self.epa_sample_media_vars = {}
        self.epa_download_stations_var = tk.BooleanVar(value=True)
        self.epa_download_results_var = tk.BooleanVar(value=True)
        
        # NOAA tab variables
        self.noaa_shapefile_var = tk.StringVar(value="")
        
        # Create menu bar
        self._create_menu_bar()
        
        # Create main frame
        self.main_frame = ttk.Frame(self.root, padding=10)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create the header with common inputs
        self._create_header()
        
        # Create notebook (tabbed interface)
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)        # First tab - Data Downloader
        self.tab1 = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab1, text="Download USGS Sub-Hourly Data")
          # Second tab - Daily Data Downloader
        self.tab2 = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab2, text="Download USGS Daily Data")
        # Third tab - NOAA Data Downloader
        self.tab3 = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab3, text="Download NOAA Data")
        
        # Fourth tab - EPA Water Quality Data Downloader
        self.tab4 = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab4, text="Download EPA Data")
        
        # Create the first tab interface (original functionality)
        self._create_tab1_interface()
          # Create the second tab interface (daily data functionality)
        self._create_tab2_interface()
        # Create the third tab interface (NOAA data functionality)
        self._create_tab3_interface()
        # Create the fourth tab interface (EPA data functionality)
        self._create_tab4_interface()
        
        # Status section common to all tabs
        self._create_status_section()
        self._create_log_section()
        
        # Configure weights for responsive design
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(2, weight=1)  # Notebook expands
        self.main_frame.rowconfigure(3, weight=0)  # Status section
        self.main_frame.rowconfigure(4, weight=1)  # Log section        # Configure the tabs to expand
        self.tab1.columnconfigure(0, weight=1)
        self.tab2.columnconfigure(0, weight=1)
        self.tab3.columnconfigure(0, weight=1)
        for i in range(7):  # Adjust based on number of rows in tab1
            self.tab1.rowconfigure(i, weight=1)
        
        # Set default NOAA shapefile path after GUI is set up
        self._set_default_noaa_shapefile()
    
    def _validate_output_directory(self):
        """Validate that the output directory is set and accessible."""
        current_path = self.base_path_var.get()
        
        if not current_path:
            self._custom_messagebox('error', "Error", "Please select an output directory first!")
            return False
        
        if not os.path.exists(current_path):
            self._custom_messagebox('error', "Error", f"Output directory does not exist:\n{current_path}")
            return False
        
        if not os.path.isdir(current_path):
            self._custom_messagebox('error', "Error", f"Selected path is not a directory:\n{current_path}")
            return False
        
        if not os.access(current_path, os.W_OK):
            self._custom_messagebox('error', "Error", f"No write permission to output directory:\n{current_path}")
            return False
        
        return True
    
    def _setup_noaa_default_path(self):
        """Set up default NOAA shapefile path in local app folder."""
        try:
            # Get local app data folder
            if sys.platform == 'win32':
                local_app_data = os.environ.get('LOCALAPPDATA', os.path.expanduser('~\\AppData\\Local'))
            else:
                local_app_data = os.path.expanduser('~/.local/share')
            
            # Create WRDH folder in local app data
            self.noaa_default_dir = os.path.join(local_app_data, 'WRDH')
            os.makedirs(self.noaa_default_dir, exist_ok=True)
            
            # Copy NOAA shapefiles if they don't exist in the default location
            self._copy_noaa_shapefiles_to_default()
            
        except Exception as e:
            # If setup fails, use current directory as fallback
            self.noaa_default_dir = os.getcwd()
    
    def _copy_noaa_shapefiles_to_default(self):
        """Copy NOAA shapefiles and WRDH icon from application directory to default location."""
        try:
            # Get the WRDH directory using sys.WRDH attribute (set by runtime hook)
            wrdh_dir = getattr(sys, 'WRDH', os.getcwd())
            
            # NOAA shapefile extensions to copy
            noaa_extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.sbn', '.sbx', '.shp.xml']
            noaa_base_names = ['NOAA_Stations_Active', 'NOAA_Stations']
            
            files_copied = 0
            
            for base_name in noaa_base_names:
                for ext in noaa_extensions:
                    source_file = os.path.join(wrdh_dir, f"{base_name}{ext}")
                    dest_file = os.path.join(self.noaa_default_dir, f"{base_name}{ext}")
                    
                    # Copy file if source exists and destination doesn't exist
                    if os.path.exists(source_file) and not os.path.exists(dest_file):
                        try:
                            shutil.copy2(source_file, dest_file)
                            files_copied += 1
                        except Exception:
                            pass  # Silent fail for individual files
                    elif os.path.exists(dest_file):
                        files_copied += 1
            
            # Copy WRDH.png icon to the default directory
            icon_source = os.path.join(wrdh_dir, "WRDH.png")
            icon_dest = os.path.join(self.noaa_default_dir, "WRDH.png")
            
            if os.path.exists(icon_source) and not os.path.exists(icon_dest):
                try:
                    shutil.copy2(icon_source, icon_dest)
                    files_copied += 1
                except Exception:
                    pass  # Silent fail
            elif os.path.exists(icon_dest):
                files_copied += 1
                
        except Exception:
            # If copying fails, continue silently
            pass
    
    def _set_window_icon(self, window):
        """Set the application icon for a window (PNG preferred, ICO fallback)."""
        try:
            if self.icon_photo:
                window.iconphoto(True, self.icon_photo)
            elif self.icon_path and self.icon_path.endswith('.ico'):
                window.iconbitmap(self.icon_path)
        except Exception:
            # If icon setting fails, continue without it
            pass
    
    def _set_default_noaa_shapefile(self):
        """Set default NOAA shapefile path if available."""
        try:
            if hasattr(self, 'noaa_default_dir'):
                # Look for NOAA_Stations_Active.shp first, then NOAA_Stations.shp
                for shapefile_name in ['NOAA_Stations_Active.shp', 'NOAA_Stations.shp']:
                    default_shapefile = os.path.join(self.noaa_default_dir, shapefile_name)
                    if os.path.exists(default_shapefile):
                        # Only set if current value is empty
                        if hasattr(self, 'noaa_shapefile_var') and not self.noaa_shapefile_var.get():
                            self.noaa_shapefile_var.set(default_shapefile)
                        break
        except Exception:
            # If setting default fails, continue silently
            pass
    
    def _custom_messagebox(self, message_type, title, message):
        """Custom messagebox with application icon."""
        # Create a temporary window to set icon for messagebox
        temp_window = tk.Toplevel(self.root)
        temp_window.withdraw()  # Hide the window
        self._set_window_icon(temp_window)
        
        # Show the messagebox
        if message_type == 'error':
            result = messagebox.showerror(title, message, parent=temp_window)
        elif message_type == 'warning':
            result = messagebox.showwarning(title, message, parent=temp_window)
        elif message_type == 'info':
            result = messagebox.showinfo(title, message, parent=temp_window)
        elif message_type == 'askokcancel':
            result = messagebox.askokcancel(title, message, parent=temp_window)
        elif message_type == 'askyesno':
            result = messagebox.askyesno(title, message, parent=temp_window)
        else:
            result = messagebox.showinfo(title, message, parent=temp_window)
        
        temp_window.destroy()
        return result
    
    def _create_menu_bar(self):
        """Create the menu bar with File and Help menus."""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Copy NOAA Files to WRDH Folder", command=self._manual_copy_files)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self._exit_application)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="User Guide", command=self._show_help)
        help_menu.add_separator()
        help_menu.add_command(label="About", command=self._show_about)
    
    def _manual_copy_files(self):
        """Manually trigger copying of NOAA files to WRDH folder for testing."""
        self._log("Copying NOAA files to WRDH folder...")
        self._copy_noaa_shapefiles_to_default()
        self._log("File copy operation completed.")
        
        # Show the user where the files are
        if hasattr(self, 'noaa_default_dir'):
            self._custom_messagebox('info', "File Copy Complete", 
                                   f"File copy operation completed.\n\n"
                                   f"WRDH folder location:\n{self.noaa_default_dir}")
    
    def _exit_application(self):
        """Exit the application with confirmation."""
        if self._custom_messagebox('askokcancel', "Exit", "Are you sure you want to exit?"):
            self.root.quit()
            self.root.destroy()
    
    def _show_about(self):
        """Show about dialog."""
        about_text = """Water Resources Data Hub (WRDH)
Version 1.0

A comprehensive environmental data downloader for:
• USGS Water Data
• NOAA Weather & Water Data  
• EPA Water Quality Data

Developed for hydrological and environmental research by Afshin Shabani (Afshin.shabani@tetratech.com)"""
        
        self._custom_messagebox('info', "About WRDH", about_text)
    
    def _show_help(self):
        """Show comprehensive help window."""
        help_window = tk.Toplevel(self.root)
        help_window.title("Water Resources Data Hub - User Guide")
        help_window.geometry("900x700")
        help_window.transient(self.root)
        help_window.grab_set()
        
        # Set icon for help window
        self._set_window_icon(help_window)
        
        # Create notebook for help tabs
        help_notebook = ttk.Notebook(help_window)
        help_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Overview tab
        overview_frame = ttk.Frame(help_notebook)
        help_notebook.add(overview_frame, text="Overview")
        self._create_overview_help(overview_frame)
        
        # USGS tab
        usgs_frame = ttk.Frame(help_notebook)
        help_notebook.add(usgs_frame, text="USGS Data")
        self._create_usgs_help(usgs_frame)
        
        # NOAA tab
        noaa_frame = ttk.Frame(help_notebook)
        help_notebook.add(noaa_frame, text="NOAA Data")
        self._create_noaa_help(noaa_frame)
        
        # EPA tab
        epa_frame = ttk.Frame(help_notebook)
        help_notebook.add(epa_frame, text="EPA Data")
        self._create_epa_help(epa_frame)
        
        # Quick Start tab
        quickstart_frame = ttk.Frame(help_notebook)
        help_notebook.add(quickstart_frame, text="Quick Start")
        self._create_quickstart_help(quickstart_frame)
        
        # Close button
        close_frame = ttk.Frame(help_window)
        close_frame.pack(pady=10)
        ttk.Button(close_frame, text="Close", command=help_window.destroy).pack()
    
    def _create_overview_help(self, parent):
        """Create overview help content."""
        text_widget = tk.Text(parent, wrap=tk.WORD, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        overview_text = """WATER RESOURCES DATA HUB (WRDH) - USER GUIDE

OVERVIEW
The Water Resources Data Hub is a comprehensive application for downloading and processing environmental data from multiple government agencies. It provides a unified interface for accessing:

• USGS (United States Geological Survey) - Water flow and level data
• NOAA (National Oceanic and Atmospheric Administration) - Weather and oceanographic data
• EPA (Environmental Protection Agency) - Water quality monitoring data

KEY FEATURES
✓ Boundary-based data filtering using shapefiles
✓ Interactive map drawing for custom boundaries
✓ Multiple data formats and time intervals
✓ Automated data processing and visualization
✓ Station mapping with aerial imagery
✓ Export to multiple formats (CSV, Excel, plots)

COMMON WORKFLOW
1. Select or draw a boundary area (shapefile)
2. Choose date range for data collection
3. Select data source tab (USGS/NOAA/EPA)
4. Configure parameters and options
5. Download and process data
6. View results in output folder

SYSTEM REQUIREMENTS
• Internet connection for data downloads
• Windows/Mac/Linux operating system
• Sufficient disk space for data storage
• Optional: GIS software for advanced shapefile editing

OUTPUT STRUCTURE
All downloaded data is organized in your selected output directory:
├── [Area_Name]/
│   ├── [Parameter]/
│   │   ├── Data/ (CSV files)
│   │   ├── Figures/ (Maps and plots)
│   │   └── Raw_Data/ (Original API responses)

GETTING STARTED
Use the Quick Start tab for step-by-step instructions, or select a specific data source tab for detailed information about USGS, NOAA, or EPA data downloads."""

        text_widget.insert(tk.END, overview_text)
        text_widget.config(state=tk.DISABLED)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def _create_usgs_help(self, parent):
        """Create USGS help content."""
        text_widget = tk.Text(parent, wrap=tk.WORD, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        usgs_text = """USGS DATA DOWNLOAD

The USGS tabs provide access to United States Geological Survey water monitoring data.

HOURLY DATA TAB
Downloads high-frequency water measurements:
• Stream flow (discharge)
• Water levels
• Temperature
• Other water quality parameters

DAILY DATA TAB  
Downloads daily statistical summaries:
• Daily mean values
• Long-term trend analysis
• Water flow statistics

PARAMETERS AVAILABLE
• 00060: Stream flow (discharge) - most common
• 00065: Gage height (water level)
• 00010: Water temperature
• 00300: Dissolved oxygen
• And many more...

STATION TYPES
✓ Surface Water - Rivers, streams, lakes
○ Ground Water - Wells and springs (limited)
○ Spring - Natural springs (limited)  
○ Atmospheric - Weather stations (limited)

OUTPUT OPTIONS
• Save Hourly Data - Raw hourly measurements
• Save Daily Data - Daily statistical summaries
• Create Plots - Time series graphs
• Save Raw Data - Original API responses
• Create Aerial Map - Station locations on maps
• Save URLs - Download links for reference

WORKFLOW
1. Select boundary shapefile or draw area
2. Set date range (start and end dates)
3. Choose parameter (default: stream flow)
4. Select station types (Surface Water recommended)
5. Configure output options
6. Click "Download Data"
7. Monitor progress in log area
8. Use "Open Output Folder" to view results

TIPS
• Larger date ranges take longer to download
• Surface water stations have the most data availability
• Interactive maps help visualize station locations
• Check log messages for download status and errors"""

        text_widget.insert(tk.END, usgs_text)
        text_widget.config(state=tk.DISABLED)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def _create_noaa_help(self, parent):
        """Create NOAA help content."""
        text_widget = tk.Text(parent, wrap=tk.WORD, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        noaa_text = """NOAA DATA DOWNLOAD

The NOAA tab provides access to National Oceanic and Atmospheric Administration data from tide gauges and weather stations.

SETUP REQUIREMENTS
You must provide a NOAA stations shapefile containing station locations. This is separate from your boundary shapefile.

DATA PRODUCTS AVAILABLE
✓ Water Level - Tide and water level measurements (default)
○ Air Temperature - Atmospheric temperature
○ Water Temperature - Water body temperature  
○ Wind - Wind speed and direction
○ Air Pressure - Barometric pressure
○ Conductivity - Water conductivity
○ Salinity - Water salinity
○ Humidity - Relative humidity
○ Visibility - Atmospheric visibility
○ Tide Prediction - Predicted tide levels

PARAMETERS
• Datum - Vertical reference (MLLW, MSL, etc.)
• Time Zone - GMT, LST, or LST/LDT
• Units - Metric or English
• Time Interval - Hourly, daily, 6-minute, etc.
• Special Interval - For water temp, salinity, conductivity

WORKFLOW
1. Select boundary shapefile
2. Select NOAA stations shapefile  
3. Set date range
4. Choose data products (Water Level is default)
5. Configure parameters (datum, timezone, units)
6. Click "Download NOAA Data"
7. Monitor progress for each data product
8. Use "Open Output Folder" to view results

OUTPUT
• Separate folders for each data product
• CSV files with time series data
• Station maps showing data availability
• Plots for each station with data

IMPORTANT NOTES
• NOAA stations shapefile must contain valid station IDs
• Not all stations have all data products available
• Water level data is most commonly available
• Special interval applies to meteorological data
• Some data products may have limited historical data"""

        text_widget.insert(tk.END, noaa_text)
        text_widget.config(state=tk.DISABLED)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def _create_epa_help(self, parent):
        """Create EPA help content."""
        text_widget = tk.Text(parent, wrap=tk.WORD, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        epa_text = """EPA WATER QUALITY DATA

The EPA tab provides access to Environmental Protection Agency water quality monitoring data from the Water Quality Portal.

SITE TYPES (Select multiple)
✓ Lake/Reservoir/Impoundment
✓ Stream  
✓ Spring
✓ Well
✓ Ocean
✓ Estuary
✓ Wetland
✓ Other-Surface Water
✓ Facility
✓ Land
✓ Aggregate groundwater use
✓ Aggregate surface-water-use

SAMPLE MEDIA (Select multiple)
✓ Water
✓ Biological Tissue
✓ Sediment
✓ Air
✓ Other
✓ Habitat

DOWNLOAD OPTIONS
✓ Download Station Data - Station locations and metadata
✓ Download Results Data - Water quality measurements

WORKFLOW
1. Select boundary shapefile (uses common settings)
2. Set date range (uses common settings)
3. Select desired site types (multiple selection allowed)
4. Select sample media types (multiple selection allowed)
5. Choose download options (stations and/or results)
6. Click "Run EPA Download"
7. Monitor progress in log area
8. Use "Open EPA Output Folder" to view results

OUTPUT STRUCTURE
EPA_Data/
├── Stations_Data/ - Station information and locations
├── Results_Data/ - Water quality measurements
└── Plots/ - Station location maps

STATION MAPS
• stations_plot_no_names.png - Clean station map
• stations_plot_with_station_ids.png - Map with station IDs
• Both maps show stations overlaid on street maps

DATA FORMATS
• CSV files for easy analysis
• Shapefiles for GIS applications  
• PNG maps for visualization

IMPORTANT NOTES
• EPA data coverage varies by region and time period
• More site types and media = more comprehensive results
• Large date ranges may result in substantial data downloads
• Station data download provides locations for mapping
• Results data contains the actual water quality measurements
• Some areas may have limited EPA monitoring stations"""

        text_widget.insert(tk.END, epa_text)
        text_widget.config(state=tk.DISABLED)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def _create_quickstart_help(self, parent):
        """Create quick start guide."""
        text_widget = tk.Text(parent, wrap=tk.WORD, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        quickstart_text = """QUICK START GUIDE

Follow these steps to download your first dataset:

STEP 1: PREPARE YOUR BOUNDARY
Option A - Use Existing Shapefile:
• Click "Browse..." next to "Boundary Shapefile"
• Select a .shp file defining your area of interest
• The area name will auto-populate from filename

Option B - Draw Custom Boundary:
• Click "Draw Boundary" button
• Use the interactive map to draw your area
• Save the boundary when finished

STEP 2: SET DATE RANGE
• Choose "Start Date" - when to begin data collection
• Choose "End Date" - when to end data collection
• Recommended: Start with 1-2 months for testing

STEP 3: CHOOSE DATA SOURCE
Select the appropriate tab:
• "Download USGS Hourly Data" - Stream flow, water levels
• "Download USGS Daily Data" - Daily summaries  
• "Download NOAA Data" - Weather and tide data
• "Download EPA Data" - Water quality monitoring

STEP 4: CONFIGURE OPTIONS
For USGS (easiest to start):
• Keep default parameter (Stream flow)
• Keep "Surface Water" selected
• Keep all output options checked
• Click "Download Data"

For NOAA:
• Browse for NOAA stations shapefile
• Select "Water Level" data product
• Keep default parameters
• Click "Download NOAA Data"

For EPA:
• Select site types (Lake, Stream recommended)
• Select "Water" sample media
• Keep both download options checked
• Click "Run EPA Download"

STEP 5: MONITOR PROGRESS
• Watch the progress bar and log messages
• Downloads may take several minutes
• Use "Stop" button if needed to cancel

STEP 6: VIEW RESULTS
• Click "Open Output Folder" to see files
• Check the "Figures" folder for maps and plots
• Data files are in CSV format for easy analysis

TROUBLESHOOTING
• No data found: Try a larger boundary area or different dates
• Download errors: Check internet connection
• Missing stations: Verify shapefile covers your area
• Large downloads: Use shorter date ranges

FIRST TIME RECOMMENDATIONS
1. Start with USGS hourly stream flow data
2. Use a small boundary area (city or county size)
3. Try a 1-month date range
4. Keep all default options enabled
5. Examine the output structure before larger downloads

Need more help? Check the specific tabs for detailed information about each data source."""

        text_widget.insert(tk.END, quickstart_text)
        text_widget.config(state=tk.DISABLED)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _create_header(self):
        """Create the header section with title and common controls."""
        header_frame = ttk.Frame(self.main_frame)
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        
        # Configure column weights
        header_frame.columnconfigure(0, weight=1)
        
        # Title
        title_label = ttk.Label(
            header_frame, 
            text="Water Resources Data Hub", 
            font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, pady=(0, 5))
        
        # Description
        desc_label = ttk.Label(
            header_frame,
            text="A Tool for Downloading Hydrology and Water Quality Measurements",
            font=("Arial", 10)
        )
        desc_label.grid(row=1, column=0)
        
        # Create common input section (shapefile and date selection)
        common_input_frame = ttk.LabelFrame(self.main_frame, text="Common Settings", padding=10)
        common_input_frame.grid(row=1, column=0, sticky="ew", pady=5, padx=5)
        
        # Configure grid for proper stretching
        common_input_frame.columnconfigure(1, weight=1)
          # Shapefile path
        ttk.Label(common_input_frame, text="Boundary Shapefile:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        ttk.Entry(common_input_frame, textvariable=self.shapefile_path_var, width=50).grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        ttk.Button(common_input_frame, text="Browse...", command=self._browse_shapefile).grid(row=0, column=2, sticky="e", padx=2, pady=5)
        ttk.Button(common_input_frame, text="Draw Boundary", command=self._draw_boundary).grid(row=0, column=3, sticky="e", padx=2, pady=5)          # Output directory preview
        ttk.Label(common_input_frame, text="Output Directory:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        ttk.Entry(common_input_frame, textvariable=self.base_path_var, width=50, state="readonly").grid(row=1, column=1, sticky="ew", padx=5, pady=5)
        ttk.Button(common_input_frame, text="Browse...", command=self._change_base_path).grid(row=1, column=2, columnspan=2, sticky="e", padx=2, pady=5)
        
        # Date range in common settings
        date_frame = ttk.Frame(common_input_frame)
        date_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=5)
        
        date_frame.columnconfigure(0, weight=0)
        date_frame.columnconfigure(1, weight=1)
        date_frame.columnconfigure(2, weight=0)
        date_frame.columnconfigure(3, weight=1)
        
        # Start date
        ttk.Label(date_frame, text="Start Date:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        start_date = DateEntry(date_frame, width=12, date_pattern='yyyy-mm-dd', 
                              textvariable=self.start_date_var)
        start_date.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        
        # End date
        ttk.Label(date_frame, text="End Date:").grid(row=0, column=2, sticky="e", padx=5, pady=5)
        end_date = DateEntry(date_frame, width=12, date_pattern='yyyy-mm-dd',
                            textvariable=self.end_date_var)
        end_date.grid(row=0, column=3, sticky="w", padx=5, pady=5)
  
    
    def _create_tab1_interface(self):
        """Create the interface for Tab 1 (USGS Data Downloader)."""
        # Parameter section
        self._create_parameter_section()
        
        # Station type section
        self._create_station_type_section()
        
        # Output options section
        self._create_output_section()
        
        # Action section
        self._create_action_section()
    
    def _create_parameter_section(self):
        """Create parameter selection section."""
        param_frame = ttk.LabelFrame(self.tab1, text="Parameter Selection", padding=10)
        param_frame.grid(row=0, column=0, sticky="ew", pady=5, padx=5)
        
        # Configure grid for proper stretching
        param_frame.columnconfigure(0, weight=0)
        param_frame.columnconfigure(1, weight=1)
        
        # Parameter selection
        param_combo = ttk.Combobox(param_frame, textvariable=self.parameter_var, state="readonly", width=30)
        param_combo['values'] = [f"{code}: {desc}" for code, desc in PARAMETER_CODES.items()]
        param_combo.current(0)
        param_combo.grid(row=0, column=0, sticky="w", padx=5, pady=5)
    
    def _create_station_type_section(self):
        """Create station type selection section."""
        station_frame = ttk.LabelFrame(self.tab1, text="Station Type Selection", padding=10)
        station_frame.grid(row=1, column=0, sticky="ew", pady=5, padx=5)
        
        # Configure grid for proper stretching
        station_frame.columnconfigure(0, weight=1)
        station_frame.columnconfigure(1, weight=1)
        
        # Station type checkbuttons
        ttk.Label(station_frame, text="Select Station Types:").grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=5)
        
        # Create checkbuttons for each station type
        row = 1
        col = 0
        for i, (station_type, var) in enumerate(self.station_type_vars.items()):
            # Determine if this station type should be selectable
            state = "normal" if self.station_type_selectable[station_type] else "disabled"
            
            # Create checkbutton with appropriate state
            ttk.Checkbutton(station_frame, text=station_type, variable=var, state=state).grid(
                row=row, column=col, sticky="w", padx=20, pady=5
            )
            col += 1
            if col > 1:  # Two columns of checkbuttons
                col = 0
                row += 1
    
    def _create_output_section(self):
        """Create output section."""
        output_frame = ttk.LabelFrame(self.tab1, text="Output Options", padding=10)
        output_frame.grid(row=2, column=0, sticky="ew", pady=5, padx=5)
        
        # Configure grid for proper stretching
        output_frame.columnconfigure(0, weight=1)
        output_frame.columnconfigure(1, weight=1)
        
        # Output options
        self.save_hourly_var = tk.BooleanVar(value=True)
        self.save_daily_var = tk.BooleanVar(value=True)
        self.create_plots_var = tk.BooleanVar(value=True)
        self.save_raw_var = tk.BooleanVar(value=True)
        self.create_aerial_map_var = tk.BooleanVar(value=True)
        self.save_urls_var = tk.BooleanVar(value=True)
        
        ttk.Checkbutton(output_frame, text="Save Hourly Data", variable=self.save_hourly_var).grid(row=0, column=0, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(output_frame, text="Save Daily Data", variable=self.save_daily_var).grid(row=0, column=1, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(output_frame, text="Create Plots", variable=self.create_plots_var).grid(row=1, column=0, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(output_frame, text="Save Raw Data", variable=self.save_raw_var).grid(row=1, column=1, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(output_frame, text="Create Aerial Map with Stations", variable=self.create_aerial_map_var).grid(row=2, column=0, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(output_frame, text="Save Download URLs to Text File", variable=self.save_urls_var).grid(row=2, column=1, sticky="w", padx=5, pady=5)
    def _create_action_section(self):
        """Create action buttons section."""
        action_frame = ttk.Frame(self.tab1)
        action_frame.grid(row=3, column=0, sticky="ew", pady=20, padx=8)
          # Configure for proper stretching
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=0)
        action_frame.columnconfigure(2, weight=0)
        action_frame.columnconfigure(3, weight=0)
        action_frame.columnconfigure(4, weight=0)
        action_frame.columnconfigure(5, weight=1)
        
        # Buttons
        ttk.Button(action_frame, text="Download Data", command=self._start_download, style="Accent.TButton", width=20).grid(row=0, column=1, padx=10, pady=(0, 10))
        ttk.Button(action_frame, text="Stop", command=self._stop_download, width=10).grid(row=0, column=2, padx=10, pady=(0, 10))
        ttk.Button(action_frame, text="Open Interactive Map", command=self._open_interactive_map, width=20).grid(row=0, column=3, padx=10, pady=(0, 10))
        ttk.Button(action_frame, text="Open Output Folder", command=self._open_output_folder, width=20).grid(row=0, column=4, padx=10, pady=(0, 10))
    def _create_tab2_interface(self):
        """Create the interface for Tab 2 (Daily Data Downloader)."""
        # Parameter section
        self._create_daily_parameter_section()
        
        # Output options section
        self._create_daily_output_section()
        
        # Action section
        self._create_daily_action_section()
        
    def _create_daily_parameter_section(self):
        """Create parameter selection section for daily data."""
        param_frame = ttk.LabelFrame(self.tab2, text="Parameter Selection", padding=10)
        param_frame.grid(row=0, column=0, sticky="ew", pady=5, padx=5)
        
        # Configure grid for proper stretching
        param_frame.columnconfigure(0, weight=0)
        param_frame.columnconfigure(1, weight=1)
        
        # Daily parameter selection
        self.daily_parameter_var = tk.StringVar(value="dv")
        param_combo = ttk.Combobox(param_frame, textvariable=self.daily_parameter_var, state="readonly", width=30)
        param_combo['values'] = [f"{code}: {desc}" for code, desc in DAILY_PARAMETER_CODES.items()]
        param_combo.current(0)
        param_combo.grid(row=0, column=0, sticky="w", padx=5, pady=5)
          # Add description label
        ttk.Label(param_frame, text="dv: Daily stream flow data").grid(
            row=1, column=0, columnspan=2, sticky="w", padx=5, pady=5
        )
    
    def _create_daily_output_section(self):
        """Create output section for daily data."""
        output_frame = ttk.LabelFrame(self.tab2, text="Output Options", padding=10)
        output_frame.grid(row=1, column=0, sticky="ew", pady=5, padx=5)
        
        # Configure grid for proper stretching
        output_frame.columnconfigure(0, weight=1)
        output_frame.columnconfigure(1, weight=1)
        
        # Output options for daily data
        self.daily_save_data_var = tk.BooleanVar(value=True)
        self.daily_create_plots_var = tk.BooleanVar(value=True)
        self.daily_create_summary_var = tk.BooleanVar(value=True)
        self.daily_create_map_var = tk.BooleanVar(value=True)
        self.daily_save_excel_var = tk.BooleanVar(value=True)
        
        ttk.Checkbutton(output_frame, text="Save Data Files", variable=self.daily_save_data_var).grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        ttk.Checkbutton(output_frame, text="Create Plots", variable=self.daily_create_plots_var).grid(
            row=0, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Checkbutton(output_frame, text="Create Summary", variable=self.daily_create_summary_var).grid(
            row=1, column=0, sticky="w", padx=5, pady=5
        )
        ttk.Checkbutton(output_frame, text="Create Map with Stations", variable=self.daily_create_map_var).grid(
            row=1, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Checkbutton(output_frame, text="Save Excel Summary", variable=self.daily_save_excel_var).grid(
            row=2, column=0, sticky="w", padx=5, pady=5
        )
    
    def _create_daily_action_section(self):
        """Create action buttons section for daily data."""
        action_frame = ttk.Frame(self.tab2)
        action_frame.grid(row=3, column=0, sticky="ew", pady=10, padx=5)
          # Configure for proper stretching
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=0)
        action_frame.columnconfigure(2, weight=0)
        action_frame.columnconfigure(3, weight=0)
        action_frame.columnconfigure(4, weight=0)
        action_frame.columnconfigure(5, weight=1)
        
        # Buttons
        ttk.Button(action_frame, text="Download Daily Data", 
                  command=self._start_daily_download, 
                  style="Accent.TButton", width=20).grid(row=0, column=1, padx=10, pady=5)
        ttk.Button(action_frame, text="Stop", 
                  command=self._stop_download, width=10).grid(row=0, column=2, padx=10, pady=5)
        ttk.Button(action_frame, text="Open Interactive Map", 
                  command=self._open_interactive_map, width=20).grid(row=0, column=3, padx=10, pady=5)
        ttk.Button(action_frame, text="Open Output Folder", 
                  command=self._open_output_folder, width=20).grid(row=0, column=4, padx=10, pady=5)
    def _create_tab3_interface(self):
        """Create the interface for Tab 3 (NOAA Data Downloader)."""
        # Parameter section
        param_frame = ttk.LabelFrame(self.tab3, text="NOAA Data Settings", padding=10)
        param_frame.grid(row=0, column=0, sticky="ew", pady=5, padx=5)

        # Configure grid for proper stretching
        param_frame.columnconfigure(0, weight=0)
        param_frame.columnconfigure(1, weight=1)

        # NOAA Station Shapefile
        ttk.Label(param_frame, text="NOAA Station Shapefile:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        ttk.Entry(param_frame, textvariable=self.noaa_shapefile_var, width=60).grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        ttk.Button(param_frame, text="Browse...", command=self._browse_noaa_shapefile).grid(row=0, column=2, sticky="e", padx=5, pady=5)

        # Data Products Frame
        data_products_frame = ttk.LabelFrame(param_frame, text="Data Products", padding=10)
        data_products_frame.grid(row=1, column=0, rowspan=6, sticky="nw", padx=5, pady=5)
        self.data_product_vars = {}
        for i, (code, name) in enumerate({
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
            "tide_prediction": "Tide Prediction"
        }.items()):
            # Set water_level to be checked by default
            default_value = True if code == "water_level" else False
            var = tk.BooleanVar(value=default_value)
            self.data_product_vars[code] = var
            ttk.Checkbutton(data_products_frame, text=name, variable=var).grid(row=i // 2, column=i % 2, sticky="w", padx=5, pady=2)

        # Parameters Frame
        parameters_frame = ttk.LabelFrame(param_frame, text="Parameters", padding=10)
        parameters_frame.grid(row=1, column=1, rowspan=6, sticky="nw", padx=5, pady=5)

        # Datum selection
        ttk.Label(parameters_frame, text="Datum:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        datum_combo = ttk.Combobox(parameters_frame, textvariable=self.noaa_datum_var, state="readonly", width=30)
        datum_combo['values'] = ["CRD", "IGLD", "LWD", "MHHW", "MHW", "MTL", "MSL", "MLW", "MLLW", "NAVD", "STND"]
        datum_combo.grid(row=0, column=1, sticky="w", padx=5, pady=5)

        # Time Zone selection
        ttk.Label(parameters_frame, text="Time Zone:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.noaa_timezone_var = tk.StringVar(value="lst_ldt")
        timezone_combo = ttk.Combobox(parameters_frame, textvariable=self.noaa_timezone_var, state="readonly", width=30)
        timezone_combo['values'] = ["gmt", "lst", "lst_ldt"]
        timezone_combo.grid(row=1, column=1, sticky="w", padx=5, pady=5)

        # Units selection
        ttk.Label(parameters_frame, text="Units:").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.noaa_units_var = tk.StringVar(value="metric")
        units_combo = ttk.Combobox(parameters_frame, textvariable=self.noaa_units_var, state="readonly", width=30)
        units_combo['values'] = ["metric", "english"]
        units_combo.grid(row=2, column=1, sticky="w", padx=5, pady=5)

        # Time Interval selection
        ttk.Label(parameters_frame, text="Time Interval:").grid(row=3, column=0, sticky="w", padx=5, pady=5)
        self.noaa_time_interval_var = tk.StringVar(value="h")
        time_interval_combo = ttk.Combobox(parameters_frame, textvariable=self.noaa_time_interval_var, state="readonly", width=30)
        time_interval_combo['values'] = ["h: Hourly", "hilo: High/Low", "6min: 6-Minute", "hourly: Hourly", "daily: Daily", "monthly: Monthly"]
        time_interval_combo.grid(row=3, column=1, sticky="w", padx=5, pady=5)

        # Special Interval Dropdown
        ttk.Label(parameters_frame, text="Special Interval\n(for water temperature, salinity, conductivity,\nand meteorological data):").grid(row=4, column=0, sticky="w", padx=5, pady=5)
        special_interval_combo = ttk.Combobox(parameters_frame, textvariable=self.noaa_special_interval_var, state="readonly", width=30)
        special_interval_combo['values'] = ["h: Hourly", "6: 6-Minute"]
        special_interval_combo.current(0)  # Default to the first option
        special_interval_combo.grid(row=4, column=1, sticky="w", padx=5, pady=5)

        # Action section
        action_frame = ttk.Frame(self.tab3)
        action_frame.grid(row=1, column=0, sticky="ew", pady=20, padx=8)        # Configure for proper stretching
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=0)
        action_frame.columnconfigure(2, weight=0)
        action_frame.columnconfigure(3, weight=0)
        action_frame.columnconfigure(4, weight=1)
        
        # Buttons
        ttk.Button(action_frame, text="Download NOAA Data", command=self._start_noaa_download, style="Accent.TButton", width=20).grid(row=0, column=1, padx=5, pady=(0, 10))
        ttk.Button(action_frame, text="Open Output Folder", command=self._open_noaa_output_folder, width=18).grid(row=0, column=2, padx=5, pady=(0, 10))
        ttk.Button(action_frame, text="Open Interactive Map", command=self._open_noaa_interactive_map, width=22).grid(row=0, column=3, padx=5, pady=(0, 10))

    def _browse_noaa_shapefile(self):
        """Browse for a NOAA station shapefile."""
        # Set initial directory to NOAA default directory
        initial_dir = getattr(self, 'noaa_default_dir', self.base_path_var.get())
        
        filepath = filedialog.askopenfilename(
            initialdir=initial_dir,
            title="Select NOAA Station Shapefile",
            filetypes=(("Shapefile", "*.shp"), ("All files", "*.*"))        )
        
        if filepath:
            self.noaa_shapefile_var.set(filepath)
            self._log(f"Selected NOAA station shapefile: {filepath}")

    def _start_noaa_download(self):
        """Start the NOAA data download process."""
        if not self.shapefile_path_var.get():
            self._custom_messagebox('error', "Error", "Please select a boundary shapefile first!")
            return
            
        if not self.noaa_shapefile_var.get():
            self._custom_messagebox('error', "Error", "Please select a NOAA stations shapefile first!")
            return
        
        # Validate output directory
        if not self._validate_output_directory():
            return

        # Extract parameters
        datum = self.noaa_datum_var.get()
        timezone = self.noaa_timezone_var.get()
        units = self.noaa_units_var.get()
        shapefile_path = self.shapefile_path_var.get()
        noaa_shapefile_path = self.noaa_shapefile_var.get()
          # Get the user-selected output directory
        output_directory = self.base_path_var.get()
        
        # Use the output directory directly (no subdirectories)
        area_output_dir = output_directory
        
        # Call the NOAA data download functions based on selected data products
        try:
            self._update_status("Downloading NOAA data...")
            
            # Get selected data products
            selected_products = []
            for code, var in self.data_product_vars.items():
                if var.get():
                    selected_products.append(code)
            
            if not selected_products:
                self._custom_messagebox('warning', "Warning", "Please select at least one data product to download!")
                return
            
            # Import all necessary functions
            from Download_NOAA_Data_CLI import (
                download_realtime_water_level,
                download_verified_hourly_heights,
                download_water_temperature_data,
                download_conductivity_data,
                download_air_temperature,
                download_air_pressure,
                download_humidity,
                download_visibility,
                download_salinity_data,
                tide_prediction,
                wind_data
            )

            self._log(f"Selected data products: {', '.join(selected_products)}")
            
            # Initialize progress bar
            total_products = len(selected_products)
            self._update_progress(0, total_products)
              # Get date range and convert format from YYYY-MM-DD to YYYYMMDD
            begin_date_raw = self.start_date_var.get()
            end_date_raw = self.end_date_var.get()
            
            # Convert date format for NOAA API (YYYY-MM-DD -> YYYYMMDD)
            try:
                begin_date = begin_date_raw.replace('-', '')
                end_date = end_date_raw.replace('-', '')
            except:
                # Fallback if the date is already in YYYYMMDD format
                begin_date = begin_date_raw
                end_date = end_date_raw
            
            # Get special interval for certain data types
            special_interval = self.noaa_special_interval_var.get().split(':')[0] if ':' in self.noaa_special_interval_var.get() else self.noaa_special_interval_var.get()
            time_interval = self.noaa_time_interval_var.get().split(':')[0] if ':' in self.noaa_time_interval_var.get() else self.noaa_time_interval_var.get()
            
            total_downloaded = 0
            
            # Download each selected data product
            for product_index, product in enumerate(selected_products):
                try:
                    self._update_status(f"Downloading {product.replace('_', ' ').title()} data...")
                    # Update progress bar for current product
                    self._update_progress(product_index, total_products)
                    self._log(f"Starting download for {product.replace('_', ' ').title()}...")
                    
                    if product == "water_level":
                        stations_with_data, plot_output, intersection = download_realtime_water_level(
                            datum, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "water_temperature":
                        stations_with_data, plot_output, intersection = download_water_temperature_data(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "conductivity":
                        stations_with_data, plot_output, intersection = download_conductivity_data(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "air_temperature":
                        stations_with_data, plot_output, intersection = download_air_temperature(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "air_pressure":
                        stations_with_data, plot_output, intersection = download_air_pressure(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "humidity":
                        stations_with_data, plot_output, intersection = download_humidity(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "visibility":
                        stations_with_data, plot_output, intersection = download_visibility(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "salinity":
                        stations_with_data, plot_output, intersection = download_salinity_data(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "wind":                        stations_with_data, plot_output, intersection = wind_data(
                            begin_date, end_date, special_interval, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "air_gap":
                        # Handle tide prediction for air gap data
                        stations_with_data, plot_output, intersection = tide_prediction(
                            begin_date, end_date, datum, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    elif product == "tide_prediction":
                        # Handle tide prediction data
                        stations_with_data, plot_output, intersection = tide_prediction(
                            begin_date, end_date, datum, timezone, units, area_output_dir, shapefile_path, noaa_shapefile_path
                        )
                    else:
                        self._log(f"Warning: {product} data download not yet implemented")
                        continue
                      # Check results and log them (only for implemented products)
                    if stations_with_data:
                        total_downloaded += len(stations_with_data)
                        self._log(f"Downloaded {product.replace('_', ' ').title()} data for {len(stations_with_data)} stations.")
                    else:
                        self._log(f"No {product.replace('_', ' ').title()} data available for the selected area.")
                        
                except Exception as e:
                    self._log(f"Error downloading {product} data: {str(e)}")
                finally:                    # Update progress after each product (successful or failed)
                    self._update_progress(product_index + 1, total_products)

            self._log(f"Total stations with data across all products: {total_downloaded}")
            self._log(f"Data saved to: {area_output_dir}")
            
            # Note: Station maps are generated automatically by each data product function
            if total_downloaded > 0:
                self._log("Station maps have been generated for each data product in their respective plot folders.")
            
            # Set progress bar to 100% complete
            self._update_progress(total_products, total_products)
            self._update_status("NOAA data download complete!")
            
            # Show completion message
            self._custom_messagebox('info', "Download Complete", f"NOAA data download completed successfully!\n\nData saved to: {self.base_path_var.get()}")
            
        except Exception as e:
            self._log(f"Error during NOAA data download: {str(e)}")
            self._custom_messagebox('error', "Error", f"Error downloading NOAA data: {str(e)}")

    
    def _open_output_folder(self):
        """Open the output folder in file explorer."""
        if not self.area_name_var.get():
            self._custom_messagebox('info', "Information", "Please select a shapefile first.")
            return
            
        area_name = self.area_name_var.get()
        
        # Get the appropriate parameter based on which tab is currently active
        current_tab = self.notebook.index(self.notebook.select())
        if current_tab == 0:  # Hourly data tab
            parameter = self.parameter_var.get().split(':')[0]
        else:  # Daily data tab
            parameter = self.daily_parameter_var.get().split(':')[0]
            
        output_dir = os.path.join(self.base_path_var.get(), area_name, parameter)
        
        if os.path.exists(output_dir):
            if sys.platform == 'win32':
                os.startfile(output_dir)
            elif sys.platform == 'darwin':  # macOS
                subprocess.call(['open', output_dir])
            else:  # Linux
                subprocess.call(['xdg-open', output_dir])
        else:
            self._custom_messagebox('info', "Information", f"Output directory does not exist yet: {output_dir}")
    
    def _log(self, message):
        """Add a message to the log."""
        self.log_text_widget.config(state=tk.NORMAL)
        self.log_text_widget.insert(tk.END, f"{dt.datetime.now().strftime('%H:%M:%S')} - {message}\n")
        self.log_text_widget.see(tk.END)
        self.log_text_widget.config(state=tk.DISABLED)
        
        # Update the UI
        self.root.update_idletasks()
    
    def _update_status(self, message):
        """Update the status message."""
        self.status_var.set(message)
        self._log(message)
        
        # Update the UI
        self.root.update_idletasks()
    
    def _update_progress(self, value, max_value):
        """Update the progress bar."""
        progress_percentage = (value / max_value) * 100
        self.progress_var.set(progress_percentage)
        
        # Update the UI
        self.root.update_idletasks()
    
    def _start_download(self):
        """Start the download process in a separate thread."""
        if self.download_thread and self.download_thread.is_alive():
            self._custom_messagebox('info', "Info", "Download already in progress!")
            return
        
        # Check if shapefile is selected
        if not self.shapefile_path_var.get():
            self._custom_messagebox('error', "Error", "Please select a boundary shapefile first!")
            return
        
        # Validate output directory
        if not self._validate_output_directory():
            return
        
        # Reset stop flag
        self.stop_download = False
        
        # Start download thread
        self.download_thread = threading.Thread(target=self._download_process)
        self.download_thread.daemon = True
        self.download_thread.start()
    
    def _stop_download(self):
        """Stop the current download process."""
        if self.download_thread and self.download_thread.is_alive():            
            self.stop_download = True
            self._update_status("Stopping download process...")
        else:
            self._custom_messagebox('info', "Info", "No download process is running.")
    def _download_process(self):
        """Main download process using the core downloader."""
        try:
            # Get parameters
            parameter = self.parameter_var.get().split(':')[0]
            start_date = self.start_date_var.get()
            end_date = self.end_date_var.get()
            area_name = self.area_name_var.get()
            base_path = self.base_path_var.get()
            shapefile_path = self.shapefile_path_var.get()
            
            # Get selected station types
            selected_station_types = []
            for station_type, var in self.station_type_vars.items():
                if var.get():
                    selected_station_types.extend(STATION_TYPES[station_type])
            
            if not selected_station_types:
                self._log("Error: Please select at least one station type.")
                return
            
            # Validate parameters
            if not os.path.exists(shapefile_path):
                self._log(f"Error: Shapefile does not exist: {shapefile_path}")
                return
            
            # Prepare options
            options = {
                'save_hourly': self.save_hourly_var.get(),
                'save_daily': self.save_daily_var.get(),
                'create_plots': self.create_plots_var.get(),
                'save_raw': self.save_raw_var.get(),
                'create_aerial_map': self.create_aerial_map_var.get(),
                'save_urls': self.save_urls_var.get()
            }
            
            # Call the core downloader
            self.core_downloader.download_hourly_data(
                parameter, start_date, end_date, base_path, area_name, 
                shapefile_path, selected_station_types, options
            )
            
            # Show completion message
            self._custom_messagebox('info', "Download Complete", f"USGS data download completed successfully!\n\nData saved to: {base_path}")
            
        except Exception as e:
            error_msg = f"Error in download process: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg)
    
    def _daily_download_process(self):
        """Main daily data download process using the core downloader."""
        try:
            # Get parameters
            parameter = self.daily_parameter_var.get().split(':')[0]
            start_date = self.start_date_var.get()
            end_date = self.end_date_var.get()
            area_name = self.area_name_var.get()
            base_path = self.base_path_var.get()
            shapefile_path = self.shapefile_path_var.get()
            
            # Get selected station types
            selected_station_types = []
            for station_type, var in self.station_type_vars.items():
                if var.get():
                    selected_station_types.extend(STATION_TYPES[station_type])
            
            if not selected_station_types:
                self._log("Error: Please select at least one station type.")
                return
            
            # Validate parameters
            if not os.path.exists(shapefile_path):
                self._log(f"Error: Shapefile does not exist: {shapefile_path}")
                return
            
            # Prepare options
            options = {
                'save_data': self.daily_save_data_var.get(),
                'create_plots': self.daily_create_plots_var.get(),
                'create_summary': self.daily_create_summary_var.get(),
                'create_map': self.daily_create_map_var.get(),
                'save_excel': self.daily_save_excel_var.get()
            }
            
            # Call the core downloader
            self.core_downloader.download_daily_data(
                parameter, start_date, end_date, base_path, area_name,
                shapefile_path, selected_station_types, options
            )
            
        except Exception as e:
            error_msg = f"Error in daily download process: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg)
   
    
    def _create_aerial_map(self, stations_gdf, boundary_gdf, output_dir, area_name, parameter):
        """Create a map with aerial imagery showing stations."""
        try:
            # Create figure and axis
            fig, ax = plt.subplots(figsize=(12, 10))
            
            # Ensure both GeoDataFrames have valid CRS and are in the same projection
            # Convert to Web Mercator (EPSG:3857) for contextily compatibility
            try:
                # Check if boundary_gdf has a valid CRS
                if boundary_gdf.crs is None:
                    self._log("Warning: Boundary GeoDataFrame has no CRS, assuming EPSG:4326")
                    boundary_gdf = boundary_gdf.set_crs("EPSG:4326")
                
                if stations_gdf.crs is None:
                    self._log("Warning: Stations GeoDataFrame has no CRS, assuming EPSG:4326")
                    stations_gdf = stations_gdf.set_crs("EPSG:4326")
                
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
            
            # Add street map basemap instead of satellite imagery - use explicit Web Mercator CRS with fallback options
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
            
            # Set title and remove axes
            if parameter == '00060' or parameter=='dv':
                plt.title(f"{area_name} - USGS Stations for Streamflow")
            else: 
                plt.title(f"{area_name} - USGS Stations for {parameter}")
    
            ax.set_axis_off()
            
            # Save the map
            map_file = os.path.join(output_dir, 'Figures', f"{area_name}_stations_map.png")
            plt.savefig(map_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            self._log(f"Created aerial map: {map_file}")
            
        except Exception as e:
            error_msg = f"Error creating aerial map: {str(e)}"
            if self._verbose_errors:
                error_msg += f"\n{traceback.format_exc()}"
            self._log(error_msg)
    
    def _create_interactive_web_map(self, stations_gdf, boundary_gdf, output_dir, area_name, parameter):
        """Create an interactive web map with stations and boundary."""
        try:
            # Calculate center of the map
            center_lat = stations_gdf.geometry.y.mean()
            center_lon = stations_gdf.geometry.x.mean()
            
            # Create a Folium map
            m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
            
            # Add the boundary as a GeoJSON layer
            folium.GeoJson(
                boundary_gdf,
                name='Boundary',
                style_function=lambda x: {
                    'fillColor': 'transparent',
                    'color': 'red',
                    'weight': 2
                }
            ).add_to(m)
            
            # Add each station as a marker
            for idx, row in stations_gdf.iterrows():
                folium.Marker(
                    location=[row.geometry.y, row.geometry.x],
                    popup=f"Station ID: {row['site_no']}<br>Name: {row['station_nm']}<br>Type: {row['site_tp_cd']}",
                    tooltip=row['site_no'],
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            
            # Add measurement tool
            m.add_child(MeasureControl())
            
            # Add layer control
            folium.LayerControl().add_to(m)
            
            # Save to HTML
            map_file = os.path.join(output_dir, 'Figures', f"{area_name}_interactive_map.html")
            m.save(map_file)
            
            self._log(f"Created interactive web map: {map_file}")
            
        except Exception as e:
            error_msg = f"Error creating interactive web map: {str(e)}"
            if self._verbose_errors:
                error_msg += f"\n{traceback.format_exc()}"
            self._log(error_msg)
    
    def _start_daily_download(self):
        """Start the daily data download process in a separate thread."""
        if self.download_thread and self.download_thread.is_alive():
            self._custom_messagebox('info', "Info", "Download already in progress!")
            return
        
        # Check if shapefile is selected
        if not self.shapefile_path_var.get():
            self._custom_messagebox('error', "Error", "Please select a boundary shapefile first!")
            return
        
        # Validate output directory
        if not self._validate_output_directory():
            return
        
        # Reset stop flag
        self.stop_download = False
        
        # Start download thread
        self.download_thread = threading.Thread(target=self._daily_download_process)
        self.download_thread.daemon = True
        self.download_thread.start()
    
    def _daily_download_process(self):
        """Main daily data download process."""
        try:
            # Get parameters
            parameter = self.daily_parameter_var.get().split(':')[0]
            start_date = self.start_date_var.get()
            end_date = self.end_date_var.get()
            area_name = self.area_name_var.get()
            base_path = self.base_path_var.get()
            shapefile_path = self.shapefile_path_var.get()
            
            # Get selected station types
            selected_station_types = []
            for station_type, var in self.station_type_vars.items():
                if var.get():
                    selected_station_types.extend(STATION_TYPES[station_type])
            
            if not selected_station_types:
                self._custom_messagebox('error', "Error", "Please select at least one station type.")
                return
            
            # Validate parameters
            if not os.path.exists(shapefile_path):
                self._custom_messagebox('error', "Error", f"Shapefile does not exist: {shapefile_path}")
                return
            
            # Call the daily download function
            self._download_daily_data(parameter, start_date, end_date, base_path, area_name, shapefile_path, selected_station_types)
            
        except Exception as e:
            error_msg = f"Error in download process: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg)
            self._custom_messagebox('error', "Error", error_msg)
    
    def _download_daily_data(self, parameter, start, end, base_path, area_name, shapefile_path, selected_station_types):
        """Download and process daily USGS data using the modular function."""
        try:
            # Call the modular daily downloader function
            result = download_usgs_daily_data(
                parameter=parameter,
                start=start,
                end=end,
                base_path=base_path,
                area_name=area_name,
                shapefile_path=shapefile_path,
                selected_station_types=selected_station_types,
                save_urls=False,
                stop_download_callback=lambda: self.stop_download,
                update_status_callback=self._update_status,
                update_progress_callback=lambda current, total: self._update_progress(current, total),
                log_callback=self._log,
                verbose_errors=self._verbose_errors
            )
            
            # Check if the download was successful
            if result.get('success', False):
                self._log(f"Daily data download completed successfully!")
                self._log(f"Total stations: {result.get('total_stations', 0)}")
                self._log(f"Successful downloads: {result.get('successful_downloads', 0)}")
                self._log(f"Failed downloads: {result.get('failed_downloads', 0)}")
                if 'data_directory' in result:
                    self._log(f"Data saved to: {result['data_directory']}")
                
                # Show completion message
                self._custom_messagebox('info', "Download Complete", 
                                   f"USGS daily data download completed successfully!\n\n"
                                   f"Total stations: {result.get('total_stations', 0)}\n"
                                   f"Successful downloads: {result.get('successful_downloads', 0)}\n"
                                   f"Data saved to: {result.get('data_directory', base_path)}")
            else:
                error_msg = result.get('error', 'Unknown error occurred')
                self._log(f"Daily data download failed: {error_msg}")
                self._custom_messagebox('error', "Download Failed", f"Daily data download failed:\n{error_msg}")
                
        except Exception as e:
            error_msg = f"Error calling daily downloader: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg)
            self._custom_messagebox('error', "Error", error_msg)
    
    def _open_interactive_map(self):
        """Open the interactive map HTML file in a web browser."""
        current_tab = self.notebook.index(self.notebook.select())
        area_name = self.area_name_var.get()
        
        if not area_name:
            self._custom_messagebox('info', "Information", "Please select a shapefile first.")
            return
        
        # Determine the correct path based on which tab is active
        if current_tab == 0:  # Hourly data tab
            parameter = self.parameter_var.get().split(':')[0]
            map_file = os.path.join(self.base_path_var.get(), area_name, parameter, 'Figures', f"{area_name}_interactive_map.html")
        elif current_tab == 1:  # Daily data tab
            parameter = self.daily_parameter_var.get().split(':')[0]
            map_file = os.path.join(self.base_path_var.get(), area_name, parameter, 'Figures', f"{area_name}_daily_interactive_map.html")
        else:
            self._custom_messagebox('info', "Information", "Interactive map is not available for this tab.")
            return
        
        if os.path.exists(map_file):
            # Open in default web browser
            try:
                if sys.platform == 'win32':
                    os.startfile(map_file)            
                elif sys.platform == 'darwin':  # macOS
                    subprocess.call(['open', map_file])
                else:  # Linux
                    subprocess.call(['xdg-open', map_file])
                
                self._log(f"Opened interactive map: {map_file}")
            except Exception as e:
                self._custom_messagebox('error', "Error", f"Could not open map file:\n{str(e)}")
        else:
            self._custom_messagebox('info', "Information", f"Interactive map not found: {map_file}\nPlease download data first to generate the map.")

    def _browse_shapefile(self):
        """Browse for a shapefile."""
        filepath = filedialog.askopenfilename(
            initialdir=self.base_path_var.get(),
            title="Select Boundary Shapefile",
            filetypes=(("Shapefile", "*.shp"), ("All files", "*.*"))
        )

        if filepath:
            self.shapefile_path_var.set(filepath)
            self._update_area_name_from_shapefile(filepath)
            self._log(f"Selected boundary shapefile: {filepath}")

    def _test_internet_connection(self):
        """Test internet connection by trying to reach a reliable endpoint."""
        try:
            # Test connection to a reliable endpoint with a short timeout
            response = requests.get('https://www.google.com', timeout=5)
            if response.status_code == 200:
                return True, "Connection successful"
            else:
                return False, f"HTTP error: {response.status_code}"
        except requests.exceptions.Timeout:
            return False, "Connection timeout - check your internet connection"
        except requests.exceptions.ConnectionError:
            return False, "Connection error - check your internet connection"
        except requests.exceptions.RequestException as e:
            return False, f"Request error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
    
    def _draw_boundary(self):
        """Open an interactive map for drawing a custom boundary."""
        try:
            # First test internet connection
            connected, message = self._test_internet_connection()
            
            if not connected:
                self._log(f"Internet connection issue: {message}")
                response = self._custom_messagebox('askyesno',
                    "Connection Issue",
                    f"Cannot access required web resources:\n{message}\n\n"
                    "Would you like to try the offline boundary tool instead?\n\n"
                    "Click 'Yes' for offline tool, 'No' to try the map anyway."
                )
                
                if response:
                    if self._create_offline_boundary_tool():
                        self._monitor_and_convert_boundary()
                    return
            
            # Create a temporary HTML file for the drawing interface
            temp_dir = tempfile.mkdtemp()
            html_file = os.path.join(temp_dir, "draw_boundary.html")
            boundary_geojson_file = os.path.join(temp_dir, "boundary.geojson")
              # Create the HTML content with Leaflet drawing capabilities
            html_content = '''
<!DOCTYPE html>
<html>
<head>
    <title>Draw Study Area Boundary</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    
    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" 
          integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
    <!-- Leaflet Draw CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.css" />
    
    <style>
        body { margin: 0; padding: 0; font-family: Arial, sans-serif; }
        #map { height: 90vh; width: 100%; }
        #controls { height: 10vh; padding: 10px; background: #f0f0f0; display: flex; align-items: center; gap: 10px; }
        button { padding: 8px 16px; background: #007cba; color: white; border: none; border-radius: 4px; cursor: pointer; }
        button:hover { background: #005a87; }
        button:disabled { background: #ccc; cursor: not-allowed; }
        #instructions { flex-grow: 1; color: #666; }
        #loading { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 1000; background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
    </style>
</head>
<body>
    <div id="loading">Loading map and drawing tools...</div>
    <div id="controls" style="display: none;">
        <div id="instructions">Draw a polygon on the map to define your study area boundary, then click Save to download it.</div>
        <button id="clearBtn">Clear</button>
        <button id="saveBtn" disabled>Save Boundary</button>
    </div>
    <div id="map"></div>
    
    <!-- Leaflet JS -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" 
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
    <!-- Leaflet Draw JS -->
    <script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>
    
    <script>
        // Error handling
        window.onerror = function(msg, url, lineNo, columnNo, error) {
            console.log('Error: ' + msg + '\\nURL: ' + url + '\\nLine: ' + lineNo + '\\nColumn: ' + columnNo + '\\nError object: ' + JSON.stringify(error));
            document.getElementById('loading').innerHTML = 'Error loading map. Please check your internet connection and try again.';
            return false;
        };
        
        // Wait for all resources to load
        window.addEventListener('load', function() {
            try {
                // Hide loading message
                document.getElementById('loading').style.display = 'none';
                document.getElementById('controls').style.display = 'flex';
                
                // Initialize the map with error handling
                if (typeof L === 'undefined') {
                    throw new Error('Leaflet library not loaded');
                }
                
                var map = L.map('map').setView([39.8283, -98.5795], 4); // Center on USA

                
                // Add base layers with fallbacks
                var osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    attribution: '© OpenStreetMap contributors',
                    maxZoom: 19
                });
                
                var satellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
                    attribution: 'Tiles © Esri — Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
                    maxZoom: 19
                });
                
                // Try to add OSM first, fallback to satellite if it fails
                osm.on('tileerror', function(error) {
                    console.warn('OSM tiles failed to load, trying satellite view');
                    if (map.hasLayer(osm)) {
                        map.removeLayer(osm);
                        satellite.addTo(map);
                    }
                });
                
                // Add default layer
                osm.addTo(map);
                
                // Layer control
                var baseLayers = {
                    "OpenStreetMap": osm,
                    "Satellite": satellite
                };
                L.control.layers(baseLayers).addTo(map);
                
                // Initialize the FeatureGroup to store editable layers
                var drawnItems = new L.FeatureGroup();
                map.addLayer(drawnItems);
                
                // Check if Leaflet.draw is available
                if (typeof L.Control.Draw === 'undefined') {
                    throw new Error('Leaflet Draw plugin not loaded');
                }
                
                // Initialize the draw control
                var drawControl = new L.Control.Draw({
                    position: 'topleft',
                    draw: {
                        polygon: {
                            allowIntersection: false,
                            drawError: {
                                color: '#e1e100',
                                message: '<strong>Error:</strong> shape edges cannot cross!'
                            },
                            shapeOptions: {
                                color: '#007cba',                                weight: 2,
                                fillOpacity: 0.2
                            }
                        },
                        rectangle: {
                            shapeOptions: {
                                color: '#007cba',
                                weight: 2,
                                fillOpacity: 0.2
                            }
                        },
                        circle: false,
                        marker: false,
                        polyline: false,
                        circlemarker: false
                    },
                    edit: {
                        featureGroup: drawnItems,
                        remove: true
                    }
                });
                map.addControl(drawControl);
                
                var currentPolygon = null;
                
                // Handle draw events
                map.on('draw:created', function (e) {
                    var layer = e.layer;
                    
                    // Remove any existing polygon
                    if (currentPolygon) {
                        drawnItems.removeLayer(currentPolygon);
                    }
                    
                    // Add the new polygon
                    drawnItems.addLayer(layer);
                    currentPolygon = layer;
                    
                    // Enable save button
                    document.getElementById('saveBtn').disabled = false;
                    document.getElementById('instructions').textContent = 'Boundary drawn! Click Save to download the boundary file.';
                });
                
                map.on('draw:deleted', function (e) {
                    currentPolygon = null;
                    document.getElementById('saveBtn').disabled = true;
                    document.getElementById('instructions').textContent = 'Draw a polygon on the map to define your study area boundary, then click Save to download it.';
                });
                
                // Clear button functionality
                document.getElementById('clearBtn').addEventListener('click', function() {
                    drawnItems.clearLayers();
                    currentPolygon = null;
                    document.getElementById('saveBtn').disabled = true;
                    document.getElementById('instructions').textContent = 'Draw a polygon on the map to define your study area boundary, then click Save.';
                });
                
                // Save button functionality
                document.getElementById('saveBtn').addEventListener('click', function() {
                    if (currentPolygon) {
                        var geojson = currentPolygon.toGeoJSON();
                        
                        // Create timestamp for unique filename
                        var timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
                        var filename = 'drawn_boundary_' + timestamp + '.geojson';
                        
                        // Save the GeoJSON file
                        var dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(geojson, null, 2));
                        var downloadAnchorNode = document.createElement('a');
                        downloadAnchorNode.setAttribute("href", dataStr);
                        downloadAnchorNode.setAttribute("download", filename);
                        document.body.appendChild(downloadAnchorNode);
                        downloadAnchorNode.click();
                        downloadAnchorNode.remove();
                        
                        // Show success message with instructions
                        alert('Boundary saved as ' + filename + '\\n\\nNext steps:\\n1. Note the location where the file was downloaded\\n2. Close this browser window\\n3. In the main application, the file will be automatically detected and converted to shapefile\\n\\nIf automatic detection fails, you can manually browse for the downloaded file.');
                        
                        // Signal completion by changing the page title and storing filename
                        document.title = 'BOUNDARY_SAVED:' + filename;
                        
                        // Also store in localStorage for the Python app to potentially access
                        try {
                            localStorage.setItem('drawnBoundaryFile', filename);
                            localStorage.setItem('drawnBoundaryTimestamp', Date.now().toString());
                        } catch(e) {
                            // localStorage not available, that's okay
                        }
                    }
                });
                
                // Try to get user's location for better initial map view
                if (navigator.geolocation) {
                    navigator.geolocation.getCurrentPosition(function(position) {
                        var lat = position.coords.latitude;
                        var lon = position.coords.longitude;
                        map.setView([lat, lon], 10);
                    }, function(error) {
                        console.log('Geolocation error:', error);
                        // Continue with default view
                    });
                }
                
                console.log('Map initialized successfully');
                
            } catch (error) {
                console.error('Error initializing map:', error);
                document.getElementById('loading').innerHTML = 'Error initializing map: ' + error.message + '<br>Please refresh the page and try again.';
                document.getElementById('loading').style.display = 'block';
                document.getElementById('controls').style.display = 'none';
            }
        });
    </script>
</body>
</html>
'''
              # Write the HTML content to the temporary file
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Try to start a local HTTP server to avoid file:// protocol issues
            try:
                # Find an available port
                def find_free_port():
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.bind(('', 0))
                        s.listen(1)
                        port = s.getsockname()[1]
                    return port
                
                port = find_free_port()
                
                # Create a simple HTTP server in a separate thread
                class QuietHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
                    def log_message(self, format, *args):
                        pass  # Suppress log messages
                
                def start_server():
                    os.chdir(temp_dir)
                    with socketserver.TCPServer(("", port), QuietHTTPRequestHandler) as httpd:
                        httpd.timeout = 300  # 5 minutes timeout
                        httpd.serve_forever()
                
                server_thread = threading.Thread(target=start_server, daemon=True)
                server_thread.start()
                
                # Give the server a moment to start
                time.sleep(1)
                
                # Open the HTTP URL instead of file://
                url = f"http://localhost:{port}/draw_boundary.html"
                webbrowser.open(url)
                
                self._log(f"Started local server at {url}")
                
            except Exception as server_error:
                self._log(f"Could not start local server: {server_error}, falling back to file:// protocol")
                # Fallback to opening the file directly
                webbrowser.open('file://' + html_file.replace('\\', '/'))
              # Monitor for saved GeoJSON file
            self._monitor_and_convert_boundary()
            
            self._log("Opened interactive boundary drawing tool")
            
        except Exception as e:
            self._log(f"Error opening boundary drawing tool: {str(e)}")
            self._custom_messagebox('error', "Error", f"Could not open boundary drawing tool:\n{str(e)}")

    def _monitor_and_convert_boundary(self):
        """Monitor for saved GeoJSON boundary and convert to shapefile."""
        try:
            def monitor_downloads():
                """Monitor the Downloads folder for the saved GeoJSON file."""
                # Common download directories
                download_dirs = [
                    os.path.join(os.path.expanduser("~"), "Downloads"),
                    os.path.join(os.path.expanduser("~"), "Desktop")
                ]
                
                # File pattern to look for
                target_pattern = "drawn_boundary_"
                max_wait_time = 300  # 5 minutes
                check_interval = 2   # Check every 2 seconds
                
                start_time = time.time()
                found_files = set()  # Track files we've already seen
                
                # Get initial file list
                for download_dir in download_dirs:
                    if os.path.exists(download_dir):
                        for file in os.listdir(download_dir):
                            if file.startswith(target_pattern) and file.endswith('.geojson'):
                                found_files.add(os.path.join(download_dir, file))
                
                while (time.time() - start_time) < max_wait_time:
                    for download_dir in download_dirs:
                        if os.path.exists(download_dir):
                            for file in os.listdir(download_dir):
                                if file.startswith(target_pattern) and file.endswith('.geojson'):
                                    full_path = os.path.join(download_dir, file)
                                    if full_path not in found_files:
                                        # New file found!
                                        self._log(f"Found new boundary file at: {full_path}")
                                        self._handle_found_boundary_file(full_path)
                                        return
                    
                    time.sleep(check_interval)
                
                # Timeout reached
                self._log("Timeout waiting for boundary file. User can manually browse for their file.")
                self._custom_messagebox('info',
                    "Boundary Drawing Complete",
                    "If you've saved your boundary, you can now browse for the downloaded file\n"
                    "to convert it to a shapefile and set it as your boundary.\n\n"
                    "Look for a file starting with 'drawn_boundary_' in your Downloads folder."
                )
              # Start monitoring in a separate thread
            monitor_thread = threading.Thread(target=monitor_downloads, daemon=True)
            monitor_thread.start()
            
        except Exception as e:
            self._log(f"Error setting up boundary monitoring: {str(e)}")
    def _handle_found_boundary_file(self, geojson_path):
        """Handle a found boundary file by prompting user for save location and filename."""
        try:
            # Create a default filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"drawn_boundary_{timestamp}.shp"
            
            # Use a single file save dialog to get both directory and filename
            shapefile_path = filedialog.asksaveasfilename(
                title="Save Boundary Shapefile As",
                initialdir=self.base_path_var.get() or os.path.expanduser("~"),
                initialfile=default_filename,
                defaultextension=".shp",
                filetypes=[
                    ("Shapefile", "*.shp"),
                    ("All files", "*.*")
                ]
            )
            
            if shapefile_path:
                # Extract directory and filename from the selected path
                output_dir = os.path.dirname(shapefile_path)
                filename_with_ext = os.path.basename(shapefile_path)
                
                # Remove .shp extension to get the base filename
                if filename_with_ext.lower().endswith('.shp'):
                    custom_filename = filename_with_ext[:-4]
                else:
                    custom_filename = filename_with_ext
                  # Convert the GeoJSON using the selected path
                self._convert_geojson_to_shapefile_with_path(geojson_path, shapefile_path)
            else:
                self._custom_messagebox('info',
                    "Save Cancelled",
                    f"Boundary file found at:\n{geojson_path}\n\n"
                    "You can manually convert this file to shapefile later."
                )
                
        except Exception as e:
            self._log(f"Error handling found boundary file: {str(e)}")

    def _convert_geojson_to_shapefile_with_path(self, geojson_path, shapefile_path):
        """Convert GeoJSON to shapefile using a full shapefile path."""
        try:
            # Try to read the GeoJSON file directly with geopandas (preferred method)
            try:
                gdf = gpd.read_file(geojson_path)
                self._log("GeoJSON loaded using gpd.read_file()")
            except Exception as e1:
                self._log(f"gpd.read_file() failed, trying alternative method: {str(e1)}")
                # Fallback: read JSON manually and create GeoDataFrame
                with open(geojson_path, 'r') as f:
                    geojson_data = json.load(f)
                
                gdf = gpd.GeoDataFrame.from_features([geojson_data])
                self._log("GeoJSON loaded using from_features()")
            
            # Ensure coordinate reference system is set to WGS84
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326")
                self._log("CRS set to EPSG:4326 using set_crs()")
            elif str(gdf.crs) != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
                self._log("CRS converted to EPSG:4326 using to_crs()")
            else:
                self._log(f"CRS already correct: {gdf.crs}")
            
            # Save as shapefile using the provided path
            gdf.to_file(shapefile_path, driver='ESRI Shapefile')
            
            # Update the GUI
            self.shapefile_path_var.set(shapefile_path)
            self._update_area_name_from_shapefile(shapefile_path)
            
            # Clean up the temporary GeoJSON file
            try:
                os.remove(geojson_path)
            except:
                pass  # Don't worry if we can't delete it
            
            # Get the filename for display
            shapefile_name = os.path.basename(shapefile_path)
            output_dir = os.path.dirname(shapefile_path)
            
            self._log(f"Boundary converted and saved as: {shapefile_path}")
            self._custom_messagebox('info',
                "Boundary Saved Successfully",
                f"Your drawn boundary has been saved as:\n{shapefile_name}\n\n"
                f"Location: {output_dir}\n\n"
                "The boundary shapefile path has been automatically updated."
            )
            
        except Exception as e:
            self._log(f"Error converting GeoJSON to shapefile: {str(e)}")
            self._custom_messagebox('error',
                "Conversion Error",
                f"Could not convert the boundary to shapefile:\n{str(e)}\n\n"
                "Please manually convert the GeoJSON file using QGIS or similar software."
            )

    def _convert_geojson_to_shapefile(self, geojson_path, output_dir, custom_filename=None):
        """Convert GeoJSON to shapefile and update the boundary path."""
        try:
            # Try to read the GeoJSON file directly with geopandas (preferred method)
            try:
                gdf = gpd.read_file(geojson_path)
                self._log("GeoJSON loaded using gpd.read_file()")
            except Exception as e1:
                self._log(f"gpd.read_file() failed, trying alternative method: {str(e1)}")
                # Fallback: read JSON manually and create GeoDataFrame
                with open(geojson_path, 'r') as f:
                    geojson_data = json.load(f)
                
                gdf = gpd.GeoDataFrame.from_features([geojson_data])
                self._log("GeoJSON loaded using from_features()")
            
            # Ensure coordinate reference system is set to WGS84
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326")
                self._log("CRS set to EPSG:4326 using set_crs()")
            elif str(gdf.crs) != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
                self._log("CRS converted to EPSG:4326 using to_crs()")
            else:
                self._log(f"CRS already correct: {gdf.crs}")
            
            # Create shapefile path with custom or default name
            if custom_filename:
                shapefile_name = f"{custom_filename}.shp"
                self._log(f"Using custom filename: {shapefile_name}")
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                shapefile_name = f"drawn_boundary_{timestamp}.shp"
                self._log(f"Using default timestamp filename: {shapefile_name}")
            
            shapefile_path = os.path.join(output_dir, shapefile_name)
            
            # Save as shapefile
            gdf.to_file(shapefile_path, driver='ESRI Shapefile')
            
            # Update the GUI
            self.shapefile_path_var.set(shapefile_path)
            self._update_area_name_from_shapefile(shapefile_path)
            
            # Clean up the temporary GeoJSON file
            try:
                os.remove(geojson_path)
            except:
                pass  # Don't worry if we can't delete it
            
            self._log(f"Boundary converted and saved as: {shapefile_path}")
            self._custom_messagebox('info',
                "Boundary Saved Successfully",
                f"Your drawn boundary has been saved as:\n{shapefile_name}\n\n"
                f"Location: {output_dir}\n\n"
                "The boundary shapefile path has been automatically updated."
            )
            
        except Exception as e:
            self._log(f"Error converting GeoJSON to shapefile: {str(e)}")
            messagebox.showerror(
                "Conversion Error",
                f"Could not convert the boundary to shapefile:\n{str(e)}\n\n"
                "Please manually convert the GeoJSON file using QGIS or similar software."
            )

    def _prompt_for_boundary_filename(self):
        """Prompt user for a custom boundary filename with validation."""
        while True:
            filename = simpledialog.askstring(
                "Boundary Filename",
                "Enter a name for your boundary shapefile:\n\n"
                "• Name should describe your study area\n"
                "• No file extension needed (will add .shp automatically)\n"
                "• Avoid special characters (/ \\ : * ? \" < > |)",
                initialvalue="my_study_area"
            )
            
            if filename is None:  # User clicked Cancel
                return None
              # Clean and validate the filename
            original_input = filename.strip()  # Store the original input for comparison
            filename = original_input
            
            if not filename:
                self._custom_messagebox('warning', "Invalid Name", "Please enter a filename.")
                continue
            
            # Remove file extension if user added one
            if filename.lower().endswith('.shp'):
                filename = filename[:-4]
            
            # Replace invalid characters with underscores
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            filename = re.sub(r'\s+', '_', filename)  # Replace spaces with underscores
            
            # Ensure filename isn't too long (Windows has 255 char limit)
            if len(filename) > 200:
                self._custom_messagebox('warning',
                    "Name Too Long", 
                    f"Filename is too long ({len(filename)} characters).\n"
                    "Please use a shorter name (max 200 characters)."
                )
                continue
            
            # Confirm the cleaned filename with user if it was modified
            if filename != original_input:
                confirmed = self._custom_messagebox('askyesno',
                    "Filename Modified",
                    f"Your filename was cleaned for compatibility:\n\n"
                    f"Original: {original_input}\n"
                    f"Cleaned:  {filename}\n\n"
                    "Use this cleaned filename?"
                )
                if not confirmed:
                    continue
            
            return filename

    def _update_area_name_from_shapefile(self, filepath):
        """Update the area name based on the shapefile name."""
        # Extract filename without extension
        filename = os.path.basename(filepath)
        name_without_ext = os.path.splitext(filename)[0]
        self.area_name_var.set(name_without_ext)

    def _change_base_path(self):
        """Change the base output path."""
        directory = filedialog.askdirectory(initialdir=self.base_path_var.get(), title="Select Output Folder")
        if directory:
            self.base_path_var.set(directory)
            self._log(f"Base path changed to: {directory}")

    def _set_date_range(self, days):
        """Set date range to last X days."""
        end_date = dt.datetime.now()
        start_date = end_date - dt.timedelta(days=days)
        
        self.end_date_var.set(end_date.strftime("%Y-%m-%d"))
        self.start_date_var.set(start_date.strftime("%Y-%m-%d"))

    def _create_status_section(self):
        """Create status section with progress bar."""
        status_frame = ttk.Frame(self.main_frame)
        status_frame.grid(row=3, column=0, sticky="ew", pady=5, padx=5)

        # Configure for proper stretching
        status_frame.columnconfigure(0, weight=0)
        status_frame.columnconfigure(1, weight=1)

        ttk.Label(status_frame, text="Status:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        ttk.Label(status_frame, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=5, pady=5)

        # Progress bar
        self.progress = ttk.Progressbar(status_frame, variable=self.progress_var, mode="determinate")
        self.progress.grid(row=1, column=0, columnspan=2, sticky="ew", padx=5, pady=5)

    def _create_log_section(self):
        """Create log text area."""
        log_frame = ttk.LabelFrame(self.main_frame, text="Log", padding=10)
        log_frame.grid(row=4, column=0, sticky="nsew", pady=5, padx=5)

        # Configure the frame to expand
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        # Scrolled text widget
        self.log_text_widget = tk.Text(log_frame, wrap=tk.WORD, width=80, height=10)
        self.log_text_widget.grid(row=0, column=0, sticky="nsew")

        # Scrollbar
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text_widget.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Configure scrollbar
        self.log_text_widget.config(yscrollcommand=scrollbar.set)
        
        # Set text widget to disabled by default
        self.log_text_widget.config(state=tk.DISABLED)
        self.log_text_widget.configure(yscrollcommand=scrollbar.set)        # Make read-only
        self.log_text_widget.config(state=tk.DISABLED)

    def _open_noaa_output_folder(self):
        """Open the NOAA output folder in file explorer."""
        output_directory = self.base_path_var.get()
        
        if not output_directory:
            self._custom_messagebox('info', "Information", "Please select an output directory first.")
            return
        
        # NOAA data is saved directly to the base output directory in subdirectories
        # Check if any NOAA data folders exist
        noaa_folders = [
            'Real Time Water Level',
            'Real Time Water Level Plots',
            'Verified Hourly Heights',
            'Verified Hourly Plots',
            'Tide Prediction',
            'Tide Prediction Plots',
            'Wind Data',
            'Wind Data Plots',
            'Water Temperature',
            'Water Temperature Plots',
            'Conductivity',
            'Conductivity Plots',
            'Air Temperature Data',
            'Air Temperature Plots',
            'Air Pressure Data',
            'Air Pressure Plots',
            'Humidity Data',
            'Humidity Plots',
            'Visibility Data',
            'Visibility Plots',
            'Salinity Data',
            'Salinity Plots'
        ]
        
        # Check if the output directory exists
        if os.path.exists(output_directory):
            # Check if any NOAA data folders exist
            has_noaa_data = any(os.path.exists(os.path.join(output_directory, folder)) for folder in noaa_folders)
            
            if has_noaa_data or True:  # Always open the directory, let user see it's empty if no data
                if sys.platform == 'win32':
                    os.startfile(output_directory)
                elif sys.platform == 'darwin':  # macOS
                    subprocess.call(['open', output_directory])
                else:  # Linux
                    subprocess.call(['xdg-open', output_directory])
            else:
                messagebox.showinfo("Information", f"No NOAA data found in: {output_directory}\nPlease download some NOAA data first.")
        else:
            messagebox.showinfo("Information", f"Output directory does not exist: {output_directory}\nPlease select a valid output directory.")
    
    def _create_tab4_interface(self):
        """Create the interface for Tab 4 (EPA Water Quality Data Downloader)."""
        # Parameter section
        param_frame = ttk.LabelFrame(self.tab4, text="EPA Water Quality Data Settings", padding=10)
        param_frame.grid(row=0, column=0, sticky="ew", pady=5, padx=5)

        # Configure grid for proper stretching
        param_frame.columnconfigure(0, weight=0)
        param_frame.columnconfigure(1, weight=1)
        param_frame.columnconfigure(2, weight=1)

        # Site Types Frame
        site_types_frame = ttk.LabelFrame(param_frame, text="Select Site Types", padding=10)
        site_types_frame.grid(row=0, column=0, rowspan=6, sticky="nw", padx=5, pady=5)
        
        # EPA Site Types in the requested order
        epa_site_types = [
            "Stream",
            "Lake, Reservoir, Impoundment",
            "Spring",
            "Well",
            "Estuary",
            "Glacier",
            "Land",
            "Aggregate groundwater use",
            "Aggregate water-use establishment",
            "Aggregate surface-water-use",
            "Facility",
            "Atmosphere"
        ]
        
        self.epa_site_type_vars = {}
        for i, site_type in enumerate(epa_site_types):
            # Set Stream and Well to be checked by default
            default_value = True if site_type in ["Stream"] else False
            var = tk.BooleanVar(value=default_value)
            self.epa_site_type_vars[site_type] = var
            ttk.Checkbutton(site_types_frame, text=site_type, variable=var).grid(row=i // 2, column=i % 2, sticky="w", padx=5, pady=2)

        # Sample Media Frame
        sample_media_frame = ttk.LabelFrame(param_frame, text="Select Sample Media", padding=10)
        sample_media_frame.grid(row=0, column=1, rowspan=6, sticky="nw", padx=5, pady=5)
        
        # EPA Sample Media types in the requested order
        epa_sample_media = [
            "Water",
            "Soil",
            "Sediment",
            "Biological",
            "Tissue",
            "Biological Tissue",
            "Habitat",
            "Air",
            "Other",
            "No media"
        ]
        
        self.epa_sample_media_vars = {}
        for i, media in enumerate(epa_sample_media):
            # Set Water to be checked by default
            default_value = True if media == "Water" else False
            var = tk.BooleanVar(value=default_value)
            self.epa_sample_media_vars[media] = var
            ttk.Checkbutton(sample_media_frame, text=media, variable=var).grid(row=i // 2, column=i % 2, sticky="w", padx=5, pady=2)

        # Data Types Frame
        data_types_frame = ttk.LabelFrame(param_frame, text="Data Types to Download", padding=10)
        data_types_frame.grid(row=0, column=2, rowspan=6, sticky="nw", padx=5, pady=5)
        
        ttk.Checkbutton(data_types_frame, text="Download Station Data\n(Site locations and information)", 
                       variable=self.epa_download_stations_var).grid(row=0, column=0, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(data_types_frame, text="Download Result Data\n(Water quality measurements)", 
                       variable=self.epa_download_results_var).grid(row=1, column=0, sticky="w", padx=5, pady=5)

        # Action section
        action_frame = ttk.Frame(self.tab4)
        action_frame.grid(row=1, column=0, sticky="ew", pady=20, padx=8)
        
        # Configure for proper stretching
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=0)
        action_frame.columnconfigure(2, weight=0)
        action_frame.columnconfigure(3, weight=0)
        action_frame.columnconfigure(4, weight=1)
        
        # Buttons
        ttk.Button(action_frame, text="Download EPA Data", command=self._start_epa_download, style="Accent.TButton", width=20).grid(row=0, column=1, padx=5, pady=(0, 10))
        ttk.Button(action_frame, text="Open Output Folder", command=self._open_epa_output_folder, width=18).grid(row=0, column=2, padx=5, pady=(0, 10))
        ttk.Button(action_frame, text="Open Interactive Map", command=self._open_epa_interactive_map, width=22).grid(row=0, column=3, padx=5, pady=(0, 10))

    def _start_epa_download(self):
        """Start the EPA water quality data download process."""
        if not self.shapefile_path_var.get():
            messagebox.showerror("Error", "Please select a boundary shapefile first!")
            return
        
        # Validate output directory
        if not self._validate_output_directory():
            return

        # Check if at least one site type is selected
        selected_site_types = [site_type for site_type, var in self.epa_site_type_vars.items() if var.get()]
        if not selected_site_types:
            messagebox.showwarning("Warning", "Please select at least one site type!")
            return

        # Check if at least one sample media is selected
        selected_sample_media = [media for media, var in self.epa_sample_media_vars.items() if var.get()]
        if not selected_sample_media:
            messagebox.showwarning("Warning", "Please select at least one sample media type!")
            return

        # Check if at least one data type is selected
        if not (self.epa_download_stations_var.get() or self.epa_download_results_var.get()):
            messagebox.showwarning("Warning", "Please select at least one data type to download!")
            return

        # Extract parameters
        shapefile_path = self.shapefile_path_var.get()
        start_date = self.start_date_var.get()
        end_date = self.end_date_var.get()
        output_directory = self.base_path_var.get()

        try:
            self._update_status("Starting EPA water quality data download...")
            
            # Import the EPA downloader class from the existing script
            script_dir = os.path.dirname(os.path.abspath(__file__))
            epa_script_path = os.path.join(script_dir, "Downlaod EPA Water Qulaity Data.py")
            
            # Add the script directory to Python path temporarily
            if script_dir not in sys.path:
                sys.path.insert(0, script_dir)
            
            try:
                # Import the EPA downloader class
                spec = spec_from_file_location("epa_downloader", epa_script_path)
                epa_module = module_from_spec(spec)
                spec.loader.exec_module(epa_module)
                
                EPAWaterQualityDownloader = epa_module.EPAWaterQualityDownloader
                
            except Exception as import_error:
                self._log(f"Error importing EPA downloader: {str(import_error)}")
                self._custom_messagebox('error', "Import Error", f"Could not import EPA downloader:\n{str(import_error)}")
                return

            # Create EPA downloader instance
            epa_downloader = EPAWaterQualityDownloader()
            
            # Read shapefile bounds
            self._log("Reading shapefile bounds...")
            bounds = epa_downloader.read_shapefile_bounds(shapefile_path)
            if bounds is None:
                messagebox.showerror("Error", "Could not read shapefile bounds!")
                return
            
            # Convert date format (YYYY-MM-DD to MM-DD-YYYY for EPA API)
            try:
                start_date_formatted = dt.datetime.strptime(start_date, "%Y-%m-%d").strftime("%m-%d-%Y")
                end_date_formatted = dt.datetime.strptime(end_date, "%Y-%m-%d").strftime("%m-%d-%Y")
            except:
                messagebox.showerror("Error", "Invalid date format. Please use YYYY-MM-DD format.")
                return
            
            # Prepare preferences
            preferences = {
                'site_types': selected_site_types,
                'sample_media': selected_sample_media,
                'start_date': start_date_formatted,
                'end_date': end_date_formatted,
                'providers': ["NWIS", "STORET"]  # Default to both providers
            }
            
            # Prepare data type preferences
            data_type_preferences = {
                'download_stations': self.epa_download_stations_var.get(),
                'download_results': self.epa_download_results_var.get()
            }
            
            # Create output directory
            base_name = os.path.splitext(os.path.basename(shapefile_path))[0]
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            epa_output_dir = os.path.join(output_directory, f"EPA_Data_{base_name}_{timestamp}")
            os.makedirs(epa_output_dir, exist_ok=True)
            
            self._log(f"Output directory: {epa_output_dir}")
            
            # Save metadata
            epa_downloader.save_metadata(epa_output_dir, shapefile_path, bounds, preferences, data_type_preferences)
            
            # Initialize progress
            total_downloads = sum([data_type_preferences['download_stations'], data_type_preferences['download_results']])
            current_download = 0
            self._update_progress(0, total_downloads)
            
            # Download station data if selected
            station_success = False
            if data_type_preferences["download_stations"]:
                self._update_status("Downloading EPA station data...")
                self._log("📍 Downloading station data (site locations and information)")
                
                station_url, station_params = epa_downloader.build_download_url(bounds, preferences, "Station")
                station_file = os.path.join(epa_output_dir, "EPA_Stations.zip")
                
                station_success = epa_downloader.download_data(station_url, station_params, station_file)
                current_download += 1
                self._update_progress(current_download, total_downloads)
                
                if station_success:
                    self._log("✓ Station data download completed successfully")
                else:
                    self._log("✗ Station data download failed")
            
            # Download result data if selected
            result_success = False
            if data_type_preferences["download_results"]:
                self._update_status("Downloading EPA result data...")
                self._log("🔬 Downloading result data (water quality measurements)")
                
                result_url, result_params = epa_downloader.build_download_url(bounds, preferences, "Result")
                result_file = os.path.join(epa_output_dir, "EPA_Results.zip")
                
                result_success = epa_downloader.download_data(result_url, result_params, result_file)
                current_download += 1
                self._update_progress(current_download, total_downloads)
                
                if result_success:
                    self._log("✓ Result data download completed successfully")
                else:
                    self._log("✗ Result data download failed")
            
            # Create station shapefile and plots if station data was downloaded successfully
            if station_success:
                try:
                    self._update_status("Creating station shapefile and plots...")
                    # Look for the extracted CSV file
                    station_csv_path = None
                    for file in os.listdir(epa_output_dir):
                        if file.startswith("station") and file.endswith(".csv"):
                            station_csv_path = os.path.join(epa_output_dir, file)
                            break
                    
                    if station_csv_path and os.path.exists(station_csv_path):
                        epa_downloader.create_station_shapefile_and_plot(station_csv_path, epa_output_dir)
                        self._log("✓ Station shapefile and plots created successfully")
                    else:
                        self._log("⚠️ Station CSV file not found for processing")
                except Exception as e:
                    self._log(f"⚠️ Error creating station shapefile/plots: {str(e)}")
            
            # Calculate sample statistics if result data was downloaded successfully
            if result_success:
                try:
                    self._update_status("Calculating sample statistics...")
                    # Look for the extracted CSV file
                    result_csv_path = None
                    for file in os.listdir(epa_output_dir):
                        if file.startswith("result") and file.endswith(".csv"):
                            result_csv_path = os.path.join(epa_output_dir, file)
                            break
                    
                    if result_csv_path and os.path.exists(result_csv_path):
                        epa_downloader.calculate_sample_statistics(result_csv_path, epa_output_dir)
                        self._log("✓ Sample statistics calculated successfully")
                    else:
                        self._log("⚠️ Result CSV file not found for statistics calculation")
                except Exception as e:
                    self._log(f"⚠️ Error calculating statistics: {str(e)}")
            
            # Summary
            downloads_completed = station_success or result_success
            
            if downloads_completed:
                self._update_status("EPA data download completed!")
                self._log("="*50)
                self._log("EPA DATA DOWNLOAD SUMMARY")
                self._log("="*50)
                self._log(f"Output directory: {epa_output_dir}")
                
                if station_success:
                    self._log("✓ Station data: Successfully downloaded")
                elif data_type_preferences["download_stations"]:
                    self._log("✗ Station data: Download failed")
                
                if result_success:
                    self._log("✓ Result data: Successfully downloaded")
                elif data_type_preferences["download_results"]:
                    self._log("✗ Result data: Download failed")
                
                messagebox.showinfo("Download Complete", 
                                   f"EPA data download completed!\n\n"
                                   f"Output directory:\n{epa_output_dir}\n\n"
                                   f"Check the log for detailed information.")
            else:
                self._update_status("EPA data download failed!")
                self._log("⚠️ No data was successfully downloaded")
                messagebox.showwarning("Download Failed", "No EPA data was successfully downloaded. Check the log for details.")
            
        except Exception as e:
            error_msg = f"Error during EPA data download: {str(e)}"
            self._log(error_msg)
            self._update_status("EPA data download failed!")
            messagebox.showerror("Error", f"Error downloading EPA data:\n{str(e)}")

    def _open_epa_output_folder(self):
        """Open the EPA output folder in file explorer."""
        output_directory = self.base_path_var.get()
        
        if not output_directory:
            messagebox.showinfo("Information", "Please select an output directory first.")
            return
        
        # EPA data is saved in subdirectories with pattern: EPA_Data_{area_name}_{timestamp}
        # Look for EPA data directories
        epa_folders = []
        if os.path.exists(output_directory):
            for item in os.listdir(output_directory):
                if item.startswith("EPA_Data_") and os.path.isdir(os.path.join(output_directory, item)):
                    epa_folders.append(item)
        
        if epa_folders:
            # If there are EPA folders, open the most recent one
            epa_folders.sort(reverse=True)  # Sort by name (timestamp will make newest first)
            latest_epa_folder = os.path.join(output_directory, epa_folders[0])
            
            if sys.platform == 'win32':
                os.startfile(latest_epa_folder)
            elif sys.platform == 'darwin':  # macOS
                subprocess.call(['open', latest_epa_folder])
            else:  # Linux
                subprocess.call(['xdg-open', latest_epa_folder])
            
            self._log(f"Opened EPA output folder: {latest_epa_folder}")
        else:
            # No EPA data found, open base directory
            if os.path.exists(output_directory):
                if sys.platform == 'win32':
                    os.startfile(output_directory)
                elif sys.platform == 'darwin':  # macOS
                    subprocess.call(['open', output_directory])
                else:  # Linux
                    subprocess.call(['xdg-open', output_directory])
                
                messagebox.showinfo("Information", 
                                   f"No EPA data folders found in: {output_directory}\n\n"
                                   "Please download some EPA data first. The output directory has been opened for you.")
            else:
                messagebox.showinfo("Information", 
                                   f"Output directory does not exist: {output_directory}\n\n"
                                   "Please select a valid output directory.")
    
    def _create_noaa_interactive_map(self):
        """Create an interactive map for NOAA stations."""
        try:
            # Check if shapefiles are selected
            if not self.shapefile_path_var.get():
                self._custom_messagebox('error', "Error", "Please select a boundary shapefile first!")
                return None
                
            if not self.noaa_shapefile_var.get():
                self._custom_messagebox('error', "Error", "Please select a NOAA stations shapefile first!")
                return None
            
            # Check if output directory is selected
            if not self.base_path_var.get():
                self._custom_messagebox('error', "Error", "Please select an output directory first!")
                return None
            
            # Read the shapefiles
            boundary_data = gpd.read_file(self.shapefile_path_var.get())
            noaa_stations_data = gpd.read_file(self.noaa_shapefile_var.get())
            
            # Ensure both are in WGS84 for folium
            if boundary_data.crs != 'EPSG:4326':
                boundary_data = boundary_data.to_crs('EPSG:4326')
            if noaa_stations_data.crs != 'EPSG:4326':
                noaa_stations_data = noaa_stations_data.to_crs('EPSG:4326')
            
            # Create intersection to get stations within the boundary
            intersection = gpd.overlay(noaa_stations_data, boundary_data, how='intersection')
            
            if len(intersection) == 0:
                self._custom_messagebox('info', "Information", "No NOAA stations found within the selected boundary.")
                return None
            
            # Calculate center
            center_lat = intersection.geometry.y.mean()
            center_lon = intersection.geometry.x.mean()
            
            # Create map
            m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
            
            # Add boundary
            folium.GeoJson(
                boundary_data.to_json(),
                style_function=lambda x: {
                    'fillColor': 'blue',
                    'color': 'blue',
                    'weight': 2,
                    'fillOpacity': 0.1
                }
            ).add_to(m)
            
            # Add NOAA stations with detailed information
            for idx, row in intersection.iterrows():
                # Create popup content with station information
                popup_content = f"""
                <b>Station ID:</b> {row.get('id', 'N/A')}<br>
                <b>Name:</b> {row.get('name', 'N/A')}<br>
                <b>Type:</b> {row.get('type', 'N/A')}<br>
                <b>State:</b> {row.get('state', 'N/A')}<br>
                <b>Latitude:</b> {row.geometry.y:.6f}<br>
                <b>Longitude:</b> {row.geometry.x:.6f}
                """
                
                # Use simple blue color for all stations
                folium.Marker(
                    location=[row.geometry.y, row.geometry.x],
                    popup=folium.Popup(popup_content, max_width=400),
                    tooltip=f"Station: {row.get('id', 'Unknown')}",
                    icon=folium.Icon(color='blue', icon='tint', prefix='fa')
                ).add_to(m)
            
            # Add controls
            m.add_child(MeasureControl())
            folium.LayerControl().add_to(m)
            
            # Add title and legend
            title_html = '''
            <h3 align="center" style="font-size:20px"><b>NOAA Stations Interactive Map</b></h3>
            '''
            m.get_root().html.add_child(folium.Element(title_html))
            
            # Add legend
            legend_html = '''
            <div style="position: fixed; 
                        top: 60px; left: 20px; width: 180px; height: auto; 
                        background-color: rgba(255, 255, 255, 0.95); 
                        border: 2px solid #333; border-radius: 8px;
                        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                        z-index: 9999; 
                        font-size: 14px; padding: 15px; margin: 5px;
                        font-family: Arial, sans-serif;">
            <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 16px; border-bottom: 2px solid #333; padding-bottom: 5px;">NOAA Stations</p>
            <p style="margin: 5px 0; padding: 2px 0;"><i class="fa fa-tint" style="color:blue; margin-right: 8px; width: 16px;"></i> NOAA Station</p>
            </div>
            '''
            m.get_root().html.add_child(folium.Element(legend_html))
            
            # Save map
            map_file = os.path.join(self.base_path_var.get(), "noaa_interactive_stations_map.html")
            m.save(map_file)
            
            # Save the intersection stations as a shapefile
            try:
                stations_shapefile_path = os.path.join(self.base_path_var.get(), "NOAA_Stations_Used_For_Map.shp")
                intersection.to_file(stations_shapefile_path, driver='ESRI Shapefile')
                self._log(f"NOAA stations used for map saved as shapefile: {stations_shapefile_path}")
            except Exception as shp_error:
                self._log(f"Warning: Could not save stations shapefile: {str(shp_error)}")
            
            self._log(f"Interactive NOAA stations map created: {map_file}")
            return map_file
            
        except Exception as e:
            self._log(f"Error creating NOAA interactive map: {str(e)}")
            self._custom_messagebox('error', "Error", f"Error creating interactive map:\n{str(e)}")
            return None

    def _open_noaa_interactive_map(self):
        """Open the NOAA interactive map HTML file in a web browser."""
        # First try to find an existing map
        output_directory = self.base_path_var.get()
        
        if not output_directory:
            self._custom_messagebox('info', "Information", "Please select an output directory first.")
            return
        
        # Look for existing map first
        map_file = os.path.join(output_directory, "noaa_interactive_stations_map.html")
        
        if not os.path.exists(map_file):
            # Try to create a new map
            self._log("Interactive map not found. Creating new map...")
            map_file = self._create_noaa_interactive_map()
            
            if not map_file:
                return
        
        # Open in default web browser
        try:
            if sys.platform == 'win32':
                os.startfile(map_file)
            elif sys.platform == 'darwin':  # macOS
                subprocess.call(['open', map_file])
            else:  # Linux
                subprocess.call(['xdg-open', map_file])
            
            self._log(f"Opened NOAA interactive map: {map_file}")
        except Exception as e:
            self._custom_messagebox('error', "Error", f"Could not open map file:\n{str(e)}")

    def _open_epa_interactive_map(self):
        """Open the EPA interactive map HTML file in a web browser."""
        output_directory = self.base_path_var.get()
        
        if not output_directory:
            messagebox.showinfo("Information", "Please select an output directory first.")
            return
        
        # EPA data is saved in subdirectories with pattern: EPA_Data_{area_name}_{timestamp}
        # Look for EPA data directories
        epa_folders = []
        if os.path.exists(output_directory):
            for item in os.listdir(output_directory):
                if item.startswith("EPA_Data_") and os.path.isdir(os.path.join(output_directory, item)):
                    epa_folders.append(item)
        
        if epa_folders:
            # If there are EPA folders, look for the interactive map in the most recent one
            epa_folders.sort(reverse=True)  # Sort by name (timestamp will make newest first)
            latest_epa_folder = os.path.join(output_directory, epa_folders[0])
            
            # Look for the interactive map file
            map_file = os.path.join(latest_epa_folder, "interactive_stations_map.html")
            
            if os.path.exists(map_file):
                # Open in default web browser
                if sys.platform == 'win32':
                    os.startfile(map_file)
                elif sys.platform == 'darwin':  # macOS
                    subprocess.call(['open', map_file])
                else:  # Linux
                    subprocess.call(['xdg-open', map_file])
                
                self._log(f"Opened EPA interactive map: {map_file}")
            else:
                messagebox.showinfo("Information", 
                                   f"Interactive map not found in: {latest_epa_folder}\n\n"
                                   "Please download EPA station data first to generate the interactive map.")
        else:
            messagebox.showinfo("Information", 
                               f"No EPA data folders found in: {output_directory}\n\n"
                               "Please download some EPA data first.")
    

if __name__ == "__main__":
    # Setup DPI awareness for high-res displays
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    
    # Apply a modern theme if available
    if ThemedTk:
        root = ThemedTk(theme="arc")
    else:
        root = tk.Tk()
    
    # Hide the main window initially
    root.withdraw()
    
    # Create and show splash screen
    splash = SplashScreen()
    splash.show()
    
    # Update splash messages during initialization
    splash.update_message("Loading interface theme...")
    root.update()  # Allow splash to display
    
    splash.update_message("Initializing application...")
    root.update()
    
    # Initialize the application
    app = USGSDataDownloaderApp(root)
    
    # Close splash screen and show main window
    splash.update_message("Starting application...")
    root.update()
    
    # Schedule splash close and main window show
    def finish_startup():
        splash.destroy()
        root.deiconify()
    
    root.after(1500, finish_startup)  # 1.5 second delay
    
    # Start the main loop
    root.mainloop()

