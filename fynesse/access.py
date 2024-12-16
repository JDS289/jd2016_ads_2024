from .config import *
import requests
import pymysql
import csv
import time
import osmnx as ox
from math import cos, radians
import os
import warnings
from pyproj import Transformer

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """




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

  return list(locations)



def EsNs_to_LatLng(eastings_northings):
    """Converts the coordinate-system of a point from Eastings-and-Northings to Latitude-and-Longitude."""
    if not hasattr(EsNs_to_LatLng, "transformer"):
        EsNs_to_LatLng.transformer = Transformer.from_crs("epsg:27700", "epsg:4326", always_xy=True)
    eastings, northings = eastings_northings
    latLng = EsNs_to_LatLng.transformer.transform(eastings, northings)
    return [round(latLng[1], 6), round(latLng[0], 6)]


def deep_map_coord_conversion(conversion, geom):
  """Applies a coordinate conversion all the way through a nested data structure.
     Geom should be in geojson format."""
  def ring_map(ring):
    return list(map(lambda pair: conversion(pair), ring))
  
  def polygon_map(poly):
    return list(map(lambda ring: ring_map(ring), poly))
  
  def multiPolygon_map(multiPoly):
    return list(map(lambda poly: polygon_map(poly), multiPoly))

  if geom['type'] == 'Polygon':  # any element after the first in a polygon specifies a "hole" 
    geom['coordinates'] = polygon_map(geom['coordinates'])
  elif geom['type'] == 'MultiPolygon':
    geom['coordinates'] = multiPolygon_map(geom['coordinates'])
  elif geom['type'] == 'Point':
    geom['coordinates'] = conversion(geom['coordinates'])
  else:  # For the oa_boundaries geojson, every geom is either Polygon or MultiPolygon.
    raise NotImplementedError

  return geom


def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database name
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database
                               )
        print(f"Connection established!")
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


NSSEC_key = {'l123': 'L1, L2 and L3 Higher managerial, administrative and professional occupations', 'l456': 'L4, L5 and L6 Lower managerial, administrative and professional occupations', 'l7': 'L7 Intermediate occupations', 'l89': 'L8 and L9 Small employers and own account workers', 'l1011': 'L10 and L11 Lower supervisory and technical occupations', 'l12': 'L12 Semi-routine occupations', 'l13': 'l13 Routine occupations', 'l14': 'L14.1 and L14.2 Never worked and long-term unemployed', 'l15': 'L15 Full-time students'}


def make_box(centre_lat, centre_lon, side_length): # side_length in km; returns lat_high, lat_low, lon_high, lon_low
  # note that we additionally divide by two (hence using 222 not 111) because side_length is 2*(distance from centre to side)
  lon_factor = 222*cos(radians(centre_lat))
  return (centre_lat + side_length/222,        centre_lat - side_length/222,
          centre_lon + side_length/lon_factor, centre_lon - side_length/lon_factor)


def count_pois_near_coordinates(latitude: float, longitude: float, tags: dict, distance_km: float = 1.0) -> dict:  # maybe move to assess
    """
    Count Points of Interest (POIs) near a given pair of coordinates within a specified distance.
    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        tags (dict): A dictionary of OSM tags to filter the POIs (e.g., {'amenity': True, 'tourism': True}).
        distance_km (float): The distance around the location in kilometers. Default is 1 km.
    Returns:
        dict: A dictionary where keys are the OSM tags and values are the counts of POIs for each tag.
    """

    poi_dict = {}
    north, south, east, west = make_box(latitude, longitude, distance_km*2)
    for tag_key, tag_val in tags.items():  # NOTE: I believe {some_tag: True} matches any non-null value, and {some_tag: some_list} matches where the val is in some_list
      try:
        with warnings.catch_warnings():
          warnings.simplefilter("ignore")
          count = len(ox.geometries_from_bbox(north, south, east, west, {tag_key: tag_val}).index)
      except ox._errors.InsufficientResponseError:
        count = 0

      poi_dict[tag_key] = count

    return poi_dict


def download_csv(url):
  """Downloads a CSV file from the given URL and returns the path to the downloaded file."""
  counter = 1
  while os.path.exists(f"file{counter}.csv"):
    counter += 1
  csv_path = f"file{counter}.csv"

  with open(csv_path, "wb") as file:
    file.write(requests.get(url).content)

  return file.name


def download_price_paid_data(year_from, year_to):
    # Base URL where the dataset is stored 
    base_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    """Download UK house price data for given year range"""
    # File name with placeholders
    file_name = "/pp-<year>-part<part>.csv"
    for year in range(year_from, (year_to+1)):
        print (f"Downloading data for year: {year}")
        for part in range(1,3):
            url = base_url + file_name.replace("<year>", str(year)).replace("<part>", str(part))
            response = requests.get(url)
            if response.status_code == 200:
                with open("." + file_name.replace("<year>", str(year)).replace("<part>", str(part)), "wb") as file:
                    file.write(response.content)


def housing_upload_join_data(conn, year):
  start_date = str(year) + "-01-01"
  end_date = str(year) + "-12-31"

  cur = conn.cursor()
  print('Selecting data for year: ' + str(year))
  cur.execute(f'SELECT pp.price, pp.date_of_transfer, po.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, po.country, po.latitude, po.longitude FROM (SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county FROM pp_data WHERE date_of_transfer BETWEEN "' + start_date + '" AND "' + end_date + '") AS pp INNER JOIN postcode_data AS po ON pp.postcode = po.postcode')
  rows = cur.fetchall()

  csv_file_path = 'output_file.csv'

  # Write the rows to the CSV file
  with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write the data rows
    csv_writer.writerows(rows)
  print('Storing data for year: ' + str(year))
  cur.execute(f"LOAD DATA LOCAL INFILE '" + csv_file_path + "' INTO TABLE `prices_coordinates_data` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '\"' LINES STARTING BY '' TERMINATED BY '\n';")
  conn.commit()
  print('Data stored for year: ' + str(year))
