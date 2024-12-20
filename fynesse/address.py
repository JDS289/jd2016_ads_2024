# This file contains code for suporting addressing questions in the data

from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from . import assess





def greenProportion_join_meanPrice(conn, year):
  greenGDF = assess.green_proportion_by_constituency(conn, year)
  priceDF = assess.mean_price_by_constituency(conn, year).drop(columns=["geom"]) # we don't need two identical geoms
  return priceDF.join(greenGDF, how="inner").astype({"green_proportion":float, "mean_price":float})


def greenProportion_join_numSales(conn, year):
  greenGDF = assess.green_proportion_by_constituency(conn, year)
  numSalesDF = assess.num_sales_by_constituency(conn, year).drop(columns=["geom"]) # we (again) don't need two identical geoms
  return numSalesDF.join(greenGDF, how="inner").astype({"green_proportion":float, "num_sales":int})


def greenProportion_join_priceStDev(conn, year):
  greenGDF = assess.green_proportion_by_constituency(conn, year)
  priceStDevDF = assess.price_stdev_by_constituency(conn, year).drop(columns=["geom"]) # we (still) don't need two identical geoms
  return priceStDevDF.join(greenGDF, how="inner").astype({"green_proportion":float, "price_stdev":float})




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
