import cdsapi
from pathlib import Path

def main():
    dataset = "reanalysis-era5-single-levels"
    request_template = {
        "product_type": ["reanalysis"],
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
    }
    
    variables = ["2m_temperature"]
    north = 70.0  # top_latitude
    west = 180.0  # right_longitude
    south = 40.0  # bottom_latitude
    east = 30.0   # left_longitude

    output_dir = Path("era5_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2016, 2023):
        print(f"Processing data for the year {year}...")
        for variable in variables:
            print(f"  Processing variable {variable} for {year}...")
            request = request_template.copy()
            request["year"] = [str(year)]
            request["variable"] = [variable]
            request["area"] = [int(north), int(east), int(south), int(west)]
            output_file = str(output_dir / f"reanalysis-era5-single-levels_{variable}_{year}.nc")
            
            client = cdsapi.Client()
            client.retrieve(dataset, request, target=output_file)
            
            print(f"  Data for the year {year} saved to {output_file}")

if __name__ == "__main__":
    main()
