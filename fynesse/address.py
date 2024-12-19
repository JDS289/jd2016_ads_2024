# This file contains code for suporting addressing questions in the data

from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from . import assess
import geopandas as gpd



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
  priceGDF = assess.resultsToGDF(priceResults, geomColumnName=2).rename(columns={1:"mean_price", 2:"geom"})
  priceGDF.index.name = "ons_id"
  return priceGDF



def scatter(ax, predictions, actual, xlabel="", ylabel=""):
  """`predictions` and `actual` should be DataFrames"""
  
  ax.scatter(predictions, actual)

  tester_model = LinearRegression()
  tester_fitted = tester_model.fit(predictions.to_numpy().reshape(-1, 1), actual.to_numpy())
  m, c2 = tester_fitted.coef_[0], tester_fitted.intercept_
  ax.plot([0, (max_f:=max(predictions))], [c2, m*max_f + c2], color="red")
  ax.set_xlabel(xlabel)
  ax.set_ylabel(ylabel)
  ax.set_title(f"Correlation: {pearsonr(predictions, actual)[0]}", fontsize=10)
