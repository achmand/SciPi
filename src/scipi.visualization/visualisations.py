"""Class which visualises Scipi data found on different sources (CassandraDB)."""

###### importing dependencies #############################################
import pandas as pd
import cufflinks as cf
import plotly.graph_objs as go
from cassandra.cluster import Cluster
from IPython.core.display import display, HTML
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)

# TODO: write comments for methods and init  + close session !!!!
# TODO: style tables 

class ScipiVisual():

    # only one keyspace is used to interact with scipi datae
    _keyspace = "scipi"

    # CassandraDB tables 
    cassandra_tbls = {
        "yrdist": "yrwisedist", # holds year-wise distribution: single author vs co-authored
        "topkw": "topkw", # holds the top 100 keywords by count
    }

    def __init__(self, cassandra_points):
        
        # connecting to cluster & setting up session
        cluster = Cluster(cassandra_points)
        self.session = cluster.connect() 
        self.session.set_keyspace(self._keyspace) # setting up keyspace

        # set a custom row factory 
        self.session.row_factory = self.pandas_factory

        # set default fetch size to unlimited 
        self.session.default_fetch_size = None            

    def plot_bubble_topics(self):
        table = self.cassandra_tbls["topkw"]
        df = self.session.execute("SELECT keyword, count FROM " + table + ";",
                                  timeout=None)._current_rows
        
        display(df)

    def plot_yr_dist_pub(self, years, figsize=(15,8)):

        # get results from year-wise distribution as a pandas dataframe 
        table = self.cassandra_tbls["yrdist"]
        df = self.session.execute("SELECT year, single, joint, total, single_perc, joint_perc FROM " + table + ";",
                                  timeout=None)._current_rows
        
        # set year column as index
        df.set_index("year", inplace=True)
        
        # sort dataframe by year 
        df.sort_values(by=["year"], 
                       ascending=False, 
                       inplace=True)

        # format percentages and round to two decimal places 
        df["single_perc"] = round(df["single_perc"]*100.00, 2)
        df["joint_perc"] = round(df["joint_perc"]*100.00, 2)

        # set column names 
        df.columns = ["Single authored", 
                      "Joint authored", 
                      "Total Publications", 
                      "% of Single authored publications", 
                      "% of Joint authored publications"]

        # get only the latest n years 
        result = df.head(years).sort_values(by=["year"], 
                                            ascending=True).copy()

        # set table title 
        first_year = result.head(1).index.values[0]
        last_year = result.tail(1).index.values[0]
        tbl_title = "Single Authored vs co-authored publications from {} to {}.".format(first_year, last_year)

        trace = go.Table(
                header=dict(values=["year"] + list(result.columns),
                fill = dict(color='#C2D4FF'),
                align = ['left'] * 5),
        cells=dict(values=[result.index.values, 
                          result["Single authored"],
                          result["Joint authored"],
                          result["Total Publications"],
                          result["% of Single authored publications"],
                          result["% of Joint authored publications"]],
               fill = dict(color='#F5F8FF'),
               align = ['left'] * 5))

        table_result = [trace] 
        iplot(table_result, filename = "pandas_table")

        # plot single authored vs joint authored (total publications)
        iplot(result[["Single authored", "Joint authored"]].iplot(asFigure=True,
                                       kind="scatter",
                                       xTitle="Years",
                                       yTitle="Total Publications",
                                       title=tbl_title))     

        # plot single authored vs joint authored (percentage)
        iplot(result[["% of Single authored publications",
                      "% of Joint authored publications"]].iplot(asFigure=True,
                                       kind="scatter",
                                       xTitle="Years",
                                       yTitle="(%) Publications",
                                       title=tbl_title))       

    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

