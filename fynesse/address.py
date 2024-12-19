# This file contains code for suporting addressing questions in the data

from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression



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
