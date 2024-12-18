from . import access
import contextlib
import warnings
import numpy as np
from math import cos, radians
import osmnx as ox
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import pymysql
import shapely

"""
Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded?
What do columns represent, makes rure they are correctly labeled. How is the data indexed.
Crete visualisation routines to assess the data (e.g. in bokeh).Ensure that date formats are correct and correctly timezoned.
"""



def resultsToGDF(results, geomColumnName="geom", flip_lat_lon=False):
  """Constructs a GeoDataFrame from the results of an SQL query; and transforms to UK metres coordinates"""
  df = gpd.GeoDataFrame(results)
  if flip_lat_lon:
    geom = df.get(geomColumnName).apply(lambda geomString: shapely.ops.transform(lambda x, y: (y, x), shapely.from_wkt(geomString)))
  else:
    geom = df.get(geomColumnName).apply(lambda geomString: shapely.from_wkt(geomString))
  df = df.drop(columns=[geomColumnName])
  return gpd.GeoDataFrame(df, geometry=geom).set_crs("EPSG:4326").to_crs(crs="EPSG:27700")


def load_oa_features(conn, columns):
  """Returns a GeoDataFrame of ([oa_code, column1, column2..., boundary_geom], ...) where at least one specified column is neither null nor zero."""

  if not columns:
    print("Please choose some features to select.")
    return -1

  cur = conn.cursor()
  results = cur.execute(f"SELECT oa, ST_AsText(boundary),{','.join(columns)} FROM census2021_ts062_oa WHERE {' OR '.join(f'({column} IS NOT NULL AND {column} != 0)' for column in columns)}")
  results_df = gpd.GeoDataFrame(cur.fetchall(), columns=["oa_code", "boundary"]+columns).set_index("oa_code")

  return fynesse.assess.resultsToGDF(results_df, geomColumnName="boundary", flip_lat_lon=True)


def get_buildings(north, south, east, west):
  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    buildings = ox.geometries_from_bbox(north, south, east, west, {"building": True})
  buildings["full_addr"] = buildings["addr:housenumber"].notnull() & buildings["addr:street"].notnull() & buildings["addr:postcode"].notnull()
  previous_crs = buildings.crs
  buildings = buildings.to_crs("epsg:3857")
  buildings["area"] = buildings["geometry"].area
  buildings = buildings.to_crs(previous_crs)
  return buildings[["addr:housenumber", "addr:street", "addr:postcode", "full_addr", "area", "geometry"]]



def plot_buildings(north, south, east, west, buildings):
  fig, ax = plt.subplots()
  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    nodes, edges = ox.graph_to_gdfs(ox.graph_from_bbox(north, south, east, west))
  edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")
  ax.set_xlim([west, east])
  ax.set_ylim([south, north])
  ax.set_xlabel("longitude")
  ax.set_ylabel("latitude")

  for i in range(len(buildings.index)):
    geom = buildings.iloc[i].get("geometry")
    if type(geom).__name__ == "MultiPolygon":
      for subgeom in geom.geoms:
        coords = list(zip(*list(subgeom.exterior.coords)))
        ax.fill(*coords, color="green" if buildings.iloc[i].get("full_addr") else "red")
      continue
    elif type(geom).__name__ == "Point":
      coords = geom.coords
    else:
      coords = list(zip(*list(geom.exterior.coords)))
    ax.fill(*coords, color="green" if buildings.iloc[i].get("full_addr") else "red")



def merge_with_prices(addressed_buildings): # this mutates the input, so there is no return
  conn = access.create_connection("admin", "ayT2adBkqim", "database-ads-jd2016.cgrre17yxw11.eu-west-2.rds.amazonaws.com", "ads_2024")
  cur = conn.cursor()
  price_column = [np.NaN]*len(addressed_buildings.index)

  for i in range(len(addressed_buildings.index)):
    current_postcode = addressed_buildings.iloc[i].get("addr:postcode")
    street_name = addressed_buildings.iloc[i].get("addr:street").upper()
    house_number = addressed_buildings.iloc[i].get("addr:housenumber")
    # Attempting to find {house_number}, {street_name}
    with contextlib.redirect_stdout(None):
        
      cur.execute(f"SELECT * FROM pp_data WHERE date_of_transfer >= '2020-01-01' AND (postcode='{current_postcode}')")
      whole_postcode = cur.fetchall()
    if not whole_postcode:
      # Nothing at all found in the postcode
      continue
    matches = []
    for candidate in whole_postcode:
      if street_name == candidate[9]:
        if house_number == candidate[7]:
          matches.append(candidate)
    if len(matches) >= 2:
      # We have multiple matches, weird. This case seems to only happen occasionally
      pass
    elif len(matches) == 1:
      # We have found a likely match
      price_column[i] = matches[0][1]
    else:
      # No exact matches found
      pass

  addressed_buildings["price"] = price_column
  new_columns = list(addressed_buildings.columns)
  new_columns[-1], new_columns[-2] = new_columns[-2], new_columns[-1]
  addressed_buildings = addressed_buildings.reindex(columns=new_columns)
