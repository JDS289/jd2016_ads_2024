import json

with open('OA_boundaries.geojson') as f:
    d = json.load(f)

for key, value in list(d.items())[:2]:
  print(f"{key}: {value}")

print(len(list(d.values())[2]))

key = list(d.keys())[2]  # this is the key of BASICALLY ALL the data in d.

feature_dict = list(d.values())[2][0]  # This returns all of the first Output Area, I believe.

def format_pair(pair):
    key, value = pair
    return f"{key}: {value}"

print(f"{key}:")
print("    " + "\n    ".join(map(format_pair, list(feature_dict.items()))))
print("    .\n"*3)

from pyproj import Transformer
transformer = Transformer.from_crs("epsg:27700", "epsg:4326", always_xy=True)

def EsNs_to_LatLng(eastings, northings):  # this also rounds a tiny amount - my quick calculations suggest this loses < 5cm
    latLng = transformer.transform(eastings, northings)
    return [round(latLng[1], 6), round(latLng[0], 6)]

all_polygons = []

counter = 0
for oa in list(d.values())[2]:
  if oa['geometry']['type'] == 'Polygon':
    oa['geometry']['coordinates'] = list(map(lambda ring: list(map(lambda pair: EsNs_to_LatLng(*pair), ring)), oa['geometry']['coordinates']))
  elif oa['geometry']['type'] == 'MultiPolygon':
    oa['geometry']['coordinates'] = list(map(lambda poly: list(map(lambda ring: list(map(lambda pair: EsNs_to_LatLng(*pair), ring)), poly)),
                                             oa['geometry']['coordinates']))

# remember polygons can contain "holes" (this is where len(coordinates) > 1; in most cases it's just "[[[..], ..., [..]]]")

print(*list(d.values())[2][:100], sep="\n")

for oa in list(d.values())[2]:
  oa_code = oa['properties']['OA21CD']
  geometry = oa['properties']['geometry']