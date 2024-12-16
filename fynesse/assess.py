from .config import *

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

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""



def get_locations(source, tags):
  """Returns a list of (lat, lon) of objects from source (.osm.pbf) that have any the given tags.
     Successful for correctly-labeled nodes, ways, and multipolygon relations.
     Selects the location of an arbitrary node on the border; do not use if very high accuracy needed,
     or for very large areas; but completely sufficient for these purposes."""
  
  fp = osmium.FileProcessor(source).with_filter(osmium.filter.TagFilter(*tags.items()))
  filtered_path = "filtered.osm.pbf"

  with osmium.BackReferenceWriter(filtered_path, ref_src=source, overwrite=True) as writer:
    for obj in tqdm(fp):
      writer.add(obj)

  fp = osmium.FileProcessor(filtered_path).with_locations()
  locations = set()
  way_locations = {}

  for obj in fp:
    if not(obj.tags) or all(obj.tags.get(key) != value for key, value in dict(tags).items()):
      if obj.is_way():
        way_locations[obj.id] = (obj.nodes[0].lat, obj.nodes[0].lon)
      elif obj.is_relation():
        print(f"Ignored this Relation: probably part of a non-Multipolygon relation: {obj}")
      continue

    if obj.is_node():
      locations.add((obj.lat, obj.lon))

    if obj.is_way():
      first_node = obj.nodes[0]
      way_locations[obj.id] = (first_node.lat, first_node.lon)
      obj_locs = set((node.lat, node.lon) for node in obj.nodes)
      if locations & obj_locs:
        # some sub-nodes (probably mistakenly tagged) already added; let's fix:
        locations -= obj_locs
      locations.add((first_node.lat, first_node.lon))

    elif obj.is_relation():
      if obj.tags["type"] != "multipolygon":
        # While there were a couple of University accommodations with type:site, this is uncommon and poorly documented,
        # so I'm choosing to omit these (and they make up less than <0.3%).
        continue
      
      obj_way_locs = set(way_locations[mem.ref] for mem in obj.members)
      if locations & obj_way_locs:
        # some sub-ways (probably mistakenly tagged) already added; let's fix:
        locations -= obj_way_locs
      locations.add(way_locations[list(obj.members)[0].ref])

  return locations



def resultsToGDF(results, geomColumnName="geom"):
  # from the results of an SQL query; and transforms to UK metres coordinates
  df = gpd.GeoDataFrame(results)
  geom = df.get(geomColumnName).apply(lambda geomString: shapely.from_wkt(geomString))
  df = df.drop(columns=[geomColumnName])
  return gpd.GeoDataFrame(df, geometry=geom).set_crs("EPSG:4326").to_crs(crs="EPSG:27700")



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
