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




def resultsToGDF(results, geomColumnName="geom", flip_lat_lon=False, columns=None):
  """Constructs a GeoDataFrame from the results of an SQL query; and transforms to UK metres coordinates.
     Either results should be a query only for the geometry column, in which case an index will be created,
     or the first column of results will be used as the index.
     If passing a `%sql SELECT...`, column names will be inferred from the header.
     Instead, you can pass the column names in `columns`.
     If passing a cur.fetchall() without header, geomColumnName should be an int."""


  if len(results[0]) == 1:
    gdf = gpd.GeoDataFrame(results)
    geomColumnName = 0
  else:
    gdf = gpd.GeoDataFrame(results, columns=columns).set_index(0 if columns is None else columns[0])

  if flip_lat_lon:
    gdf.loc[:, geomColumnName] = gdf.loc[:, geomColumnName].apply(
                    lambda geomString: shapely.ops.transform(lambda x, y: (y, x), shapely.from_wkt(geomString)))
  else:
    gdf.loc[:, geomColumnName] = gdf.loc[:, geomColumnName].apply(lambda geomString: shapely.from_wkt(geomString))

  return gdf.set_geometry(col=geomColumnName).set_crs("EPSG:4326").to_crs(crs="EPSG:27700")



def load_oa_features(conn, columns):
  """Returns a GeoDataFrame of ([oa_code, boundary_geom, total, l15, prop_moved, column1, column2...,], ...)
     where at least one specified column is neither null nor zero."""
  # total, l15, prop_moved, and boundary_geom are frequently used, so included by default;
  # additionally they are in general non-null and non-zero - note the difference of behavior if `columns` simply included them.

  if not columns:
    print("Please choose some features to select.")
    return -1

  cur = conn.cursor()
  results = cur.execute(f"""SELECT oa,ST_AsText(boundary),total,l15,prop_moved,{','.join(columns)} FROM census2021_ts062_oa
                            WHERE {' OR '.join(f'({column} IS NOT NULL AND {column} != 0)' for column in columns)}""")
  gdf = resultsToGDF(cur.fetchall(), geomColumnName="boundary", flip_lat_lon=True,
                     columns=["ons_id", "boundary", "total", "l15", "prop_moved"]+columns)
  return gdf




pcd_year_delimiters = {2024: 34, 2023: 393244,   2022: 1179664,  2021: 2293759,  2020: 3604459,  2019: 4718554,
                                 2018: 5767114,  2017: 6815674,  2016: 7929769,  2015: 8978329,  2014: 10944379,
                                 2013: 11992939, 2012: 12844894, 2011: 13565779, 2010: 14679874, 2009: 15400759}

def mean_price_by_constituency(conn, year):
  """Returns the mean price of a house-sale in a given constituency, for a given year.
     The constituency boundaries to be used are the ones which were in place for the most
     recent election before the end of `year`."""
  
  if year < 2010:
    print("Currently not functional for pre-2010 constituency boundaries.")
    return None
  
  if year > 2024:  # (just in case)
    print("Currently we have no price-paid data in years after 2024.")
    return None
  
  if year==2024:
    boundary_category = "2024"
  else:
    boundary_category = "2010_to_2019"

  cur = conn.cursor()

  cur.execute(f"""
      SELECT p.ons_id, mean_price, ST_AsText(geometry) as geom FROM 
         (SELECT ons_id{boundary_category} as ons_id, AVG(price) as mean_price FROM prices_coordinates_data
          WHERE db_id BETWEEN {pcd_year_delimiters[year]} AND {pcd_year_delimiters[year-1]-1}
          AND ons_id{boundary_category} IS NOT NULL
          GROUP BY ons_id{boundary_category}) p
      JOIN boundaries{boundary_category} b ON b.ONS_ID = p.ons_id""")

  priceResults = cur.fetchall()
  priceGDF = resultsToGDF(priceResults, geomColumnName=2).rename(columns={1:"mean_price", 2:"geom"})
  priceGDF.index.name = "ons_id"
  return priceGDF



def green_proportion_by_constituency(conn, year):
  """Returns the proportion of valid votes for each constituency that were for the Green party,
     for a given election year."""
  if year not in range(2010, 2025):
    print("Currently not functional for pre-2010, or post-2024.")
    return None

  if year not in (2010, 2015, 2017, 2019, 2024):
    print(f"Error: {year} was not a UK election year.")
    return None

  if year==2024:
    boundary_category = "2024"
  else:
    boundary_category = "2010_to_2019"

  cur = conn.cursor()
  cur.execute(f"""SELECT g.ONS_ID as ons_id, g.proportion{year} as green_proportion, ST_AsText(geometry) as geom
                  FROM boundaries{boundary_category} b JOIN green_proportion{boundary_category} g ON b.ONS_ID = g.ONS_ID""")
  greenResults = cur.fetchall()
  greenGDF = resultsToGDF(greenResults, geomColumnName=2).rename(columns={1:"green_proportion", 2:"geom"})
  greenGDF.index.name = "ons_id"
  return greenGDF

def adjust_zeros(series):
  return series.apply(lambda x: max(x, series[series>0].min()))



def num_sales_by_constituency(conn, year):
  """Returns the total number of house sales in a given constituency, in a given year.
     The constituency boundaries to be used are the ones which were in place for the most
     recent election before the end of `year`."""
  
  if year < 2010:
    print("Currently not functional for pre-2010 constituency boundaries.")
    return None
  
  if year > 2024:  # (just in case)
    print("Currently we have no price-paid data in years after 2024.")
    return None
  
  if year==2024:
    boundary_category = "2024"
  else:
    boundary_category = "2010_to_2019"

  cur = conn.cursor()

  cur.execute(f"""
      SELECT p.ons_id, num_sales, ST_AsText(geometry) as geom FROM 
         (SELECT ons_id{boundary_category} as ons_id, COUNT(*) as num_sales FROM prices_coordinates_data
          WHERE db_id BETWEEN {pcd_year_delimiters[year]} AND {pcd_year_delimiters[year-1]-1}
          AND ons_id{boundary_category} IS NOT NULL
          GROUP BY ons_id{boundary_category}) p
      JOIN boundaries{boundary_category} b ON b.ONS_ID = p.ons_id""")

  numSalesResults = cur.fetchall()
  numSalesGDF = resultsToGDF(numSalesResults, geomColumnName=2).rename(columns={1:"num_sales", 2:"geom"})
  numSalesGDF.index.name = "ons_id"
  return numSalesGDF


def price_variance_by_constituency(conn, year):
  """Returns the variance in the prices of house-sales in a given constituency, for a given year.
     The constituency boundaries to be used are the ones which were in place for the most
     recent election before the end of `year`."""
  
  if year < 2010:
    print("Currently not functional for pre-2010 constituency boundaries.")
    return None
  
  if year > 2024:  # (just in case)
    print("Currently we have no price-paid data in years after 2024.")
    return None
  
  if year==2024:
    boundary_category = "2024"
  else:
    boundary_category = "2010_to_2019"

  cur = conn.cursor()

  cur.execute(f"""
      SELECT p.ons_id, price_variance, ST_AsText(geometry) as geom FROM 
         (SELECT ons_id{boundary_category} as ons_id, VARIANCE(price) as price_variance FROM prices_coordinates_data
          WHERE db_id BETWEEN {pcd_year_delimiters[year]} AND {pcd_year_delimiters[year-1]-1}
          AND ons_id{boundary_category} IS NOT NULL
          GROUP BY ons_id{boundary_category}) p
      JOIN boundaries{boundary_category} b ON b.ONS_ID = p.ons_id""")

  priceVarianceResults = cur.fetchall()
  priceVarianceGDF = resultsToGDF(priceVarianceResults, geomColumnName=2).rename(columns={1:"price_variance", 2:"geom"})
  priceVarianceGDF.index.name = "ons_id"
  return priceVarianceGDF
  



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
  conn = access.create_connection_default()
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
