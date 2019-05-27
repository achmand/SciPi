"""Class which visualises Scipi data found on different sources (CassandraDB)."""

###### importing dependencies #############################################
import pandas as pd
import matplotlib.pyplot as plt 
from cassandra.cluster import Cluster
from IPython.core.display import display, HTML

# TODO: write comments for methods and init  + close session !!!!
# TODO: style tables 

class ScipiVisual():

    # only one keyspace is used to interact with scipi datae
    _keyspace = "scipi"

    # CassandraDB tables 
    cassandra_tbls = {
        "yrdist": "yrwisedist" # holds year-wise distribution: single author vs co-authored
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

        # set table style and render in html
        result_html = (
            result.style
            .set_caption(tbl_title)
            .render()
        )
        
        # display dataframe result  
        display(HTML(result_html))

        # plot single authored vs joint authored 
        result["Single authored"].plot(label="Single authored", 
                                       figsize=figsize, 
                                       fontsize=12,
                                       c="r")
        
        result["Joint authored"].plot(label="Co-authored", 
                                      c="g")

        plt.title(tbl_title)
        plt.xticks(range(years), result.index.values, fontsize=12, rotation=45)
        plt.ylabel("Total Publications", fontsize=14)
        plt.xlabel("Year", fontsize=14)
        plt.legend()
        plt.show()                

    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

