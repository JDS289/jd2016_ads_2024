# This file contains code for suporting addressing questions in the data

"""# Here are some of the imports we might expect 
import sklearn.model_selection  as ms
import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.naive_bayes as naive_bayes
import sklearn.tree as tree

import GPy
import torch
import tensorflow as tf

# Or if it's a statistical analysis
import scipy.stats"""

"""Address a particular question that arises from the data"""



# Let's make some nice generalised functions for this.

import contextlib
import warnings
import numpy as np
from math import cos, radians
import osmnx as ox
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import pymysql

def initialise_sql():
  %load_ext sql
  %sql mariadb+pymysql://admin:ayT2adBkqim@database-ads-jd2016.cgrre17yxw11.eu-west-2.rds.amazonaws.com?local_infile=1
  %sql USE `ads_2024`;


def make_box(centre_lat, centre_lon, side_length): # side_length in km; returns lat_high, lat_low, lon_high, lon_low
  # note that we additionally divide by two (hence using 222 not 111) because side_length is 2*(distance from centre to side)
  lon_factor = 222*cos(radians(centre_lat))
  return (centre_lat + side_length/222,        centre_lat - side_length/222,
          centre_lon + side_length/lon_factor, centre_lon - side_length/lon_factor)


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


def plot(north, south, east, west, buildings):
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
  price_column = [np.NaN]*len(addressed_buildings.index)

  for i in range(len(addressed_buildings.index)):
    current_postcode = addressed_buildings.iloc[i].get("addr:postcode")
    street_name = addressed_buildings.iloc[i].get("addr:street").upper()
    house_number = addressed_buildings.iloc[i].get("addr:housenumber")
    # Attempting to find {house_number}, {street_name}
    with contextlib.redirect_stdout(None):
      whole_postcode = %sql SELECT * FROM pp_data WHERE date_of_transfer >= "2020-01-01" AND (postcode='{current_postcode}')
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
      price_column[i] = candidate[1]
    else:
      # No exact matches found
      pass

  addressed_buildings["price"] = price_column
  new_columns = list(addressed_buildings.columns)
  new_columns[-1], new_columns[-2] = new_columns[-2], new_columns[-1]
  addressed_buildings = addressed_buildings.reindex(columns=new_columns)



def price_area_correlation(latitude, longitude):
  north, south, east, west = make_box(latitude, longitude, 2)
  buildings = get_buildings(north, south, east, west)
  plot(north, south, east, west, buildings)
  addressed_buildings = buildings[buildings["full_addr"]==True][["addr:housenumber", "addr:street", "addr:postcode", "area", "geometry"]]
  merge_with_prices(addressed_buildings)
  pna_buildings = addressed_buildings[addressed_buildings["price"].notnull()]
  if len(pna_buildings.index) < 2:
    return "Not enough data (less than 2 relevant samples)"
  area, price = list(pna_buildings.get("area")), list(pna_buildings.get("price"))
  return pearsonr(area, price)

