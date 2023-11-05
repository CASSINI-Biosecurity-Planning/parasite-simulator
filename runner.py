import matplotlib.pyplot as plt
import xarray as xr
import trajan as ta
import opendrift as o
from opendrift.models.sealice import SeaLice
from opendrift.models.oceandrift import OceanDrift
from datetime import datetime, timedelta
import glob

import json
import random

particle_model = OceanDrift(loglevel=20)
# particle_model = SeaLice(loglevel=20)


def get_all_finfish():
    return glob.glob("**/*finfish.json", recursive=1)


def seed_farm_data(data: dict, N: int, r: int, start_time) -> [object]:
    seed_array = []
    for _, x in enumerate(data["features"]):
        seeds = {"type": x["type"],
                 "geometry": x["geometry"], "properties": {}}

        seeds["properties"]["time"] = str(start_time)
        seeds["properties"]["number"] = N
        seeds["properties"]["radius"] = r
        seed_array.append(seeds)

    return seed_array


def get_readers(locations):
    thredds_catalog = {
        'norway': 'https://thredds.met.no/thredds/dodsC/cmems/topaz6/dataset-topaz6-arc-15min-3km-be.ncml',
        'greece': ['https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_med_phy-cur_anfc_4.2km_PT1H-m','https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_med_phy-sal_anfc_4.2km_PT1H-m','https://nrt.cmems-du.eu/thredds/dodsC/cmems_mod_med_phy-tem_anfc_4.2km_PT1H-m' ]
    }
    return thredds_catalog[locations]


def read_json(path):
    with open(path) as f:
        return json.load(f)



def get_current_json_file(country, type: str):
    return glob.glob(f"**/{country}_*_{type}.json", recursive=1)


def run_model(*args, **kwargs):
    """
        runs particle simulation 
        args:
            *args:
                urls
            kwargs:
                duration: timedelta object
                N: number of particles
                r: radius of particles
                start_time: datetime object
                plot: boolean
                outfile: string
    """
    thredds_url = args

    particle_model.add_readers_from_list(thredds_url)
    duration = kwargs.pop('duration', timedelta(10))

    N = kwargs.pop('N', 100)  # 100 particles
    r = kwargs.pop('r', 80)  # 1 km radius
    lat, lon = kwargs.pop('center_pos', (60.1, 4.4))

    start_time = kwargs.pop('start_time', datetime.utcnow() - timedelta(10))
    plot = kwargs.pop('plot', False)

    outfile = kwargs.pop('outfile', f'{random.random()}.nc')

    if  'seed_from_json' in kwargs:
        datasource = kwargs.pop('seed_from_json')
        seed_data = read_json(datasource[-1])
        seed_data = seed_farm_data(seed_data, N, r, start_time)
        print(len(seed_data))
        for item in seed_data:
            particle_model.seed_from_geojson(json.dumps(item))
    else:
        particle_model.seed_elements(
            lon=lon, lat=lat,  radius=r, number=N, time=start_time)

    particle_model.run(duration=duration, outfile=f"out/{outfile}",)
    if plot:
        particle_model.plot(land='mask')
        if outfile:
            plt.savefig(f"figs/{outfile.strip('.nc')}.png")


country = 'greece'
url = get_readers(country)


duration = timedelta(5)
start = datetime.utcnow()  - duration  - timedelta(17)

run_model(*url,
          outfile=f'run_{datetime.utcnow()}.nc',
          start_time=start,
          duration=duration,
          seed_from_json=get_current_json_file(country, 'finfish'),
          N=60,
          r=1000,
          plot=True
          )
