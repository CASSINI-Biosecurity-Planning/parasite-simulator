import xarray as xr, numpy as np
import concurrent.futures as f

HEIGHT = 200
WIDTH = 200

def regrid(filename, name="particle"):

  ds = xr.load_dataset(filename + '.nc')

  TIME = len(ds.time)
  PARTICLES = 100

  new_nd = np.zeros((TIME, WIDTH, HEIGHT))
  lons = np.linspace(ds.lon.minval, ds.lon.maxval, WIDTH)
  lats = np.linspace(ds.lat.minval, ds.lat.maxval, HEIGHT)
  lon_ratio = WIDTH / (ds.lon.maxval - ds.lon.minval)
  lat_ratio = HEIGHT / (ds.lat.maxval - ds.lat.minval)

  print(lon_ratio, lat_ratio)

  def _regrid(t_index):
    for p in range(0, PARTICLES):
      arr = np.zeros((WIDTH, HEIGHT))

      x = int(np.floor((ds.lon[p, t_index] - ds.lon.minval) * lon_ratio))
      y = int(np.floor((ds.lat[p, t_index] - ds.lat.minval) * lat_ratio))

      # print("x: %i, y: %i"%(x,y))
      if x >= WIDTH or y >= HEIGHT: continue

      arr[x,y] = 1

    return (t_index, arr)
  
  with f.ThreadPoolExecutor() as executor:
    futures = [executor.submit(_regrid, t) for t in range(0, TIME)]
    for result in f.as_completed(futures):
      try:
        t, arr = result.result()
        new_nd[t] = arr
      except:
        print("Failure when combining results.")

  new = xr.Dataset(
    coords = { 'time': ds.time, 'latitude': lats, 'longitude': lons },
    data_vars = {
      name: (['time', 'longitude', 'latitude'], new_nd)
    })

  new.coords['time'] = new.time.assign_attrs({
    'long_name': 'time',
    'standard_name': 'time',
  })

  new.coords['latitude'] = new.latitude.assign_attrs({
    'units': 'degrees_north',
    'long_name': 'latitude',
  })

  new.coords['longitude'] = new.longitude.assign_attrs({
    'units': 'degrees_east',
    'long_name': 'longitude',
  })

  new.to_netcdf(filename + '-regridded.nc')
