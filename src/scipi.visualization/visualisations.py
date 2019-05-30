"""Class which visualises Scipi data found on different sources (CassandraDB)."""

###### importing dependencies #############################################
import pandas as pd
import cufflinks as cf
import plotly.graph_objs as go
from cassandra.cluster import Cluster
from IPython.core.display import display, HTML
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)

# TODO: Check this out: https://www.natureindex.com/news-blog/paper-authorship-goes-hyper
# TODO: write comments for methods and init  + close session !!!!
# TODO: style tables 

class ScipiVisual():

    # only one keyspace is used to interact with scipi datae
    _keyspace = "scipi"

    # CassandraDB tables 
    cassandra_tbls = {
        "topkw": "topkw", # holds the top 100 keywords by count
        "authorship": "authorptrn", # holds authorship patterns 
        "yrdist": "yrwisedist", # holds year-wise distribution: single author vs co-authored
        "avgauthors" : "aap" # holds the avg number of authors per paper (AAP)
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

    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    def plot_bubble_topics(self):
        table = self.cassandra_tbls["topkw"]
        df = self.session.execute("SELECT keyword, count FROM " + table + ";",
                                  timeout=None)._current_rows
        
        display(df)

    def plot_authorship_ptrn(self, cutoff):
        
        # get results from authorship patterns as a pandas dataframe 
        table = self.cassandra_tbls["authorship"]
        df = self.session.execute("SELECT author_unit, no_articles, no_authors FROM " + table + ";",
                                  timeout=None)._current_rows
    
        # set author (unit) as index 
        df.set_index("author_unit", inplace=True)

         # sort dataframe by author (unit) 
        df.sort_values(by=["author_unit"], 
                       ascending=True, 
                       inplace=True)


        # set column names & index name 
        df.index.names = ["No. authors (unit)"]
        df.columns = ["No. of publications",
                      "No. of authors"]

        # table result 
        table_result = df.head(cutoff) 

        # compute stats for greater than cutoff 
        cutoff_result = df[df.index>cutoff].copy()
        cutoff_publications = cutoff_result["No. of publications"].sum()
        cutoff_authors = cutoff_result["No. of authors"].sum()
        table_result.loc[cutoff + 1] = [cutoff_publications, cutoff_authors]
        
        # set index
        index_list = table_result.index.tolist()
        index_list[cutoff] = ">" + str(cutoff)
        table_result.index = index_list

        # compute and set % of total articles 
        total_articles = table_result["No. of publications"].sum()
        table_result["% of total publications"] = round((table_result["No. of publications"] / total_articles) * 100, 2)

        # compute cum % of total publications
        table_result["Cum.(%) of total publications"] = round(table_result["% of total publications"].cumsum(),1)

        # reset index name 
        table_result.index.names = ["No. authors (unit)"]

        # show authorship patterns table using pyplot 
        trace = go.Table(
                header=dict(values=["No. authors (unit)"] + list(table_result.columns),
                fill = dict(color='#C2D4FF'),
                align = ['left'] * 5),
        cells=dict(values=[table_result.index.values, 
                          table_result["No. of publications"],
                          table_result["No. of authors"],
                          table_result["% of total publications"],
                          table_result["Cum.(%) of total publications"]],
               fill = dict(color='#F5F8FF'),
               align = ['left'] * 5))

        table_plot_result = [trace] 
        iplot(table_plot_result, filename = "pandas_table")

        # plot authorship patterns 
        iplot(table_result[["% of total publications"]].iplot(
                                       asFigure=True,
                                       kind="scatter",
                                       xTitle="No. of authors (unit)",
                                       yTitle="(%) Publications",
                                       title="Authorship Pattern")) 

    def plot_yr_dist(self, years, figsize=(15,8)):

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
        tbl_title = "Single Authored vs Co-Authored Publications from {} to {}.".format(first_year, last_year)

        # show table using pyplot 
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
        iplot(result[["Single authored", 
                      "Joint authored"]].iplot(
                                       asFigure=True,
                                       kind="scatter",
                                       xTitle="Years",
                                       yTitle="Total Publications",
                                       title=tbl_title))     

        # plot single authored vs joint authored (percentage)
        iplot(result[["% of Single authored publications",
                      "% of Joint authored publications"]].iplot(
                                       asFigure=True,
                                       kind="scatter",
                                       xTitle="Years",
                                       yTitle="(%) Publications",
                                       title=tbl_title))       

    def plot_aap(self, years):

        # get results from aap as a pandas dataframe 
        table = self.cassandra_tbls["avgauthors"]
        df = self.session.execute("SELECT year, no_articles, no_authors, avg_author_paper FROM " + table + ";",
                                  timeout=None)._current_rows
        
        # set year column as index
        df.set_index("year", inplace=True)
        
         # sort dataframe by year 
        df.sort_values(by=["year"], 
                       ascending=False, 
                       inplace=True)

        # round AAP to 2 decimal places 
        df["avg_author_paper"] = round(df["avg_author_paper"], 2)

        # set column names 
        df.columns = ["Total no. of papers (P)", 
                      "Total no. of authorship (A)", 
                      "Avg. no of authors per paper (AAP = A/P)"]

        # get only the latest n years 
        result = df.head(years).sort_values(by=["year"], 
                                            ascending=True).copy()

        # set table title 
        first_year = result.head(1).index.values[0]
        last_year = result.tail(1).index.values[0]
        tbl_title = "Avg. Number of Authors per Paper (AAP) from {} to {}.".format(first_year, last_year)

        # show table using pyplot 
        trace = go.Table(
                header=dict(values=["year"] + list(result.columns),
                fill = dict(color='#C2D4FF'),
                align = ['left'] * 5),
        cells=dict(values=[result.index.values, 
                          result["Total no. of papers (P)"],
                          result["Total no. of authorship (A)"],
                          result["Avg. no of authors per paper (AAP = A/P)"]],
               fill = dict(color='#F5F8FF'),
               align = ['left'] * 5))

        layout = dict(width=800, height=700)
        table_result = [trace] 
        fig = dict(data=table_result, layout=layout)
        iplot(fig, filename = "pandas_table")

        # plot avg. number of authors per paper (AAP)
        iplot(result[["Avg. no of authors per paper (AAP = A/P)"]].iplot(
                                       asFigure=True,
                                       kind="scatter",
                                       xTitle="Years",
                                       yTitle="AAP",
                                       title=tbl_title))    