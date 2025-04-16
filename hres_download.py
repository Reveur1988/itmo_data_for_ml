from pathlib import Path

import xarray as xr
from dask.diagnostics import ProgressBar


def main():
    zarr_path = "gs://weatherbench2/datasets/hres_t0/2016-2022-6h-1440x721.zarr"
    ds = xr.open_zarr(zarr_path, consolidated=True)

    print("Full dataset info:")
    print(ds)

    variables = ["2m_temperature"]
    north = 70.0  # top_latitude
    west = 180.0  # right_longitude
    south = 40.0  # bottom_latitude
    east = 30.0  # left_longitude

    output_dir = Path("hres_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2016, 2023):  # from 2016 to 2022 inclusive
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        print(f"Processing data for the year {year}...")

        ds_year = ds.sel(
            time=slice(start_date, end_date),
            latitude=slice(south, north),
            longitude=slice(east, west),
        )

        for variable in variables:
            print(f"  Processing variable {variable} for {year}...")
            ds_variable = ds_year[[variable]]  # Select the variable as a dataset

            output_file = str(output_dir / f"hres_t0_{variable}_{year}_6h_1440x721.nc")

            with ProgressBar():
                ds_variable.to_netcdf(output_file)

            print(f"  Data for variable {variable} in {year} saved to {output_file}")


if __name__ == "__main__":
    main()
