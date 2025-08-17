# Water Resources Data Hub (WRDH)
A desktop application for downloading and analyzing environmental data from multiple government agencies including USGS, NOAA, and EPA.
<img width="1024" height="1024" alt="WRDH" src="https://github.com/user-attachments/assets/07d18391-fa76-4f14-9f46-0b938d4e69a7" />
## 🌊 Overview
The Water Resources Data Hub (WRDH) is a powerful, user-friendly application designed for researchers, engineers, and environmental professionals who need to access and analyze hydrological and environmental data. It provides a unified interface to download data from multiple government sources and automatically processes it for analysis.

### Key Features

- 🗺️ **Boundary-based Data Filtering** - Use shapefiles or draw custom boundaries on interactive maps
- 📊 **Multi-source Data Integration** - Access USGS, NOAA, and EPA databases through a single interface
- 🎯 **Automated Data Processing** - Automatic quality control, formatting, and visualization
- 📈 **Built-in Visualization** - Generate maps, time series plots, and summary statistics
- 📁 **Organized Output** - Structured file organization with multiple export formats
- 🗺️ **Interactive Mapping** - Station locations on aerial imagery with measurement tools

## 🏛️ Data Sources

### USGS (United States Geological Survey)
- **Stream flow** (discharge in cfs)
- **Water levels** (gage height in ft) 
- **Water temperature** (°C and °F)

### NOAA (National Oceanic and Atmospheric Administration)
- **Water levels** and tide data
- **Meteorological data** (air temperature, pressure, humidity)
- **Water temperature** and **salinity**
- **Wind** speed and direction
- **Tide predictions**

### EPA (Environmental Protection Agency)
- **Water quality** monitoring data
- **Chemical concentrations** and pollutant levels
- **Biological sampling** results
- **Sediment** and **habitat** data

## 🚀 Quick Start

### Prerequisites

```bash
Python 3.8 or higher
Internet connection for data downloads
```

### Installation

1. **Download the executable** (Recommended for most users):
   - Download `WRDH.exe` from the [Releases](https://wrdh.s3.us-east-2.amazonaws.com/WRDH.zip) page
   - No installation required - just run the executable

2. **Run from source** (For developers):
   ```bash
   git clone https://github.com/AfshinShabani/WRDH.git
   cd WRDH
   pip install -r requirements.txt
   python Water_Resources_Data_Hub.py
   ```

### First Time Setup

1. **Launch WRDH** - The application will open with a splash screen
2. **Set Output Directory** - Choose where to save your downloaded data
3. **Select Boundary** - Either browse for an existing shapefile or draw a custom boundary
4. **Choose Date Range** - Set your start and end dates for data collection

## 📖 Usage Examples

### Example 1: Download Stream Flow Data

```python
# Using the GUI:
1. Select "Download USGS Hourly Data" tab
2. Browse for your watershed boundary shapefile
3. Set date range (e.g., 2020-01-01 to 2020-12-31)
4. Keep default parameter "Stream flow (00060)"
5. Click "Download Data"
```

**Output Structure:**
```
MyWatershed/
├── 00060/
│   ├── Data/
│   │   ├── station_01234567_hourly.csv
│   │   ├── station_01234568_hourly.csv
│   │   └── combined_daily_data.csv
│   ├── Figures/
│   │   ├── MyWatershed_stations_map.png
│   │   ├── station_01234567_plot.png
│   │   └── MyWatershed_interactive_map.html
│   └── Raw_Data/
│       └── [Original API responses]
```

### Example 2: EPA Water Quality Data

```python
# Using the GUI:
1. Select "Download EPA Data" tab
2. Choose your study area boundary
3. Select site types: "Stream", "Lake/Reservoir"
4. Select sample media: "Water"
5. Set date range and click "Run EPA Download"
```

**Sample Output Data:**
| Station ID | Parameter | Date | Value | Units |
|------------|-----------|------|-------|-------|
| EPA001 | Temperature | 2020-01-15 | 12.5 | °C |
| EPA001 | pH | 2020-01-15 | 7.2 | units |
| EPA002 | Dissolved Oxygen | 2020-01-15 | 8.9 | mg/L |

### Example 3: Custom Boundary Drawing

```python
# Interactive map workflow:
1. Click "Draw Boundary" button
2. Use drawing tools on the interactive map:
   - Click points to create polygon vertices
   - Double-click to complete the polygon
3. Click "Save Boundary" to download as GeoJSON
4. Application automatically converts to shapefile
```

## 🔧 Configuration Options

### USGS Data Options
- **Parameters**: Stream flow, water temperature, gage height
- **Station Types**: Surface water, groundwater, springs
- **Output Options**: 
  - Save hourly/daily data
  - Generate time series plots
  - Create station maps
  - Export raw API responses

### NOAA Data Settings
- **Datum**: MLLW, MSL, NAVD, etc.
- **Time Zone**: GMT, LST, LST/LDT
- **Units**: Metric or English
- **Intervals**: Hourly, daily, 6-minute

### EPA Data Filters
- **Site Types**: Stream, lake, well, ocean, etc.
- **Sample Media**: Water, sediment, biological tissue
- **Download Options**: Station metadata and/or results data

## 📊 Output Formats

### Data Files
- **CSV** - Comma-separated values for easy analysis
- **Excel** - Summary statistics and formatted data
- **Shapefile** - Station locations for GIS applications

### Visualizations
- **Station Maps** - Locations overlaid on aerial imagery
- **Time Series Plots** - Data trends over time
- **Interactive Maps** - Web-based maps with measurement tools
- **Summary Statistics** - Statistical analysis of downloaded data

## 🗺️ Boundary Options

### 1. Existing Shapefiles
- Browse for `.shp` files defining your study area
- Supports any coordinate reference system
- Automatically reprojects to WGS84 if needed

### 2. Interactive Drawing
- Web-based drawing interface with Leaflet
- Draw polygons or rectangles
- Automatic conversion to shapefile format
- Save custom boundaries for reuse

### 3. Administrative Boundaries
- Use county, state, or watershed boundaries
- Compatible with any vector format supported by GeoPandas

## 🛠️ Technical Details

### System Requirements
- **OS**: Windows 10/11, macOS 10.14+, Ubuntu 18.04+
- **Memory**: 4 GB RAM minimum, 8 GB recommended
- **Storage**: 1 GB free space plus space for data
- **Network**: Internet connection required for downloads

### Dependencies
```python
# Core libraries
pandas >= 1.3.0
geopandas >= 0.10.0
requests >= 2.25.0
matplotlib >= 3.3.0
folium >= 0.12.0
contextily >= 1.1.0

# GUI libraries
tkinter (included with Python)
ttkthemes >= 3.2.0
tkcalendar >= 1.6.0

# Data processing
numpy >= 1.20.0
dataretrieval >= 1.0.0
Pillow >= 8.0.0
```

### Architecture
- **Main GUI**: `Water_Resources_Data_Hub.py` - Tkinter-based interface
- **Core Processing**: `usgs_core_downloader.py` - Data download logic
- **NOAA Module**: `Download_NOAA_Data_CLI.py` - NOAA-specific functions
- **EPA Module**: `Downlaod EPA Water Qulaity Data.py` - EPA data processing

## 🚨 Troubleshooting

### Common Issues

**1. No data found for selected area**
```
Solution: Try expanding your boundary area or adjusting date range
Check: Ensure your shapefile covers areas with monitoring stations
```

**2. Download errors or timeouts**
```
Solution: Check internet connection, try smaller date ranges
Check: Government data servers may have temporary outages
```

**3. Shapefile not loading**
```
Solution: Ensure all shapefile components (.shp, .shx, .dbf) are present
Check: Verify the coordinate reference system is properly defined
```

**4. Memory errors with large datasets**
```
Solution: Use shorter date ranges or smaller geographic areas
Check: Close other applications to free up system memory
```

### Debug Mode
Enable verbose error reporting in the GUI settings for detailed error messages.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Afshin Shabani**
- Email: afshin.shabani@tetratech.com
- Organization: Tetra Tech

## 🙏 Acknowledgments

- **USGS** for providing comprehensive water monitoring data
- **NOAA** for oceanographic and meteorological datasets  
- **EPA** for water quality monitoring information
- **Open Source Community** for the excellent Python geospatial libraries

## 📚 Citation

If you use WRDH in your research, please cite:

```bibtex
@software{shabani2025wrdh,
  title={Water Resources Data Hub (WRDH): A Comprehensive Environmental Data Downloader},
  author={Shabani, Afshin},
  year={2025},
  url={https://github.com/AfshinShabani/WRDH},
  version={1.0.0}
}
```

## 🔗 Related Projects

- [USGS Water Data for the Nation](https://waterdata.usgs.gov/)
- [NOAA Tides and Currents](https://tidesandcurrents.noaa.gov/)
- [EPA Water Quality Portal](https://www.waterqualitydata.us/)
- [HydroTools](https://github.com/NOAA-OWP/hydrotools) - NOAA's hydrological toolkit
- [USGS dataretrieval](https://github.com/DOI-USGS/python-dataretrieval) - Python package for USGS data

---

⭐ **Star this repository if you find WRDH useful!** ⭐
