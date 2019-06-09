"""Class which visualises Scipi data found on different sources (CassandraDB)."""

###### importing dependencies #############################################
import csv
import subprocess
import pandas as pd
import networkx as nx
import cufflinks as cf
from ipywidgets import widgets
import plotly.graph_objs as go
from sklearn import preprocessing
from IPython.display import display
from cassandra.cluster import Cluster
from IPython.core.display import display, HTML
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)

# TODO: Check this out: https://www.natureindex.com/news-blog/paper-authorship-goes-hyper
# TODO: write comments for methods and init  

class ScipiVisual():

    # only one keyspace is used to interact with scipi datae
    _keyspace = "scipi"

    # CassandraDB tables 
    cassandra_tbls = {
        "authorship": "authorptrn",       # holds authorship patterns 
        "yrdist": "yrwisedist",           # holds year-wise distribution: single author vs co-authored
        "avgauthors": "aap",              # holds the avg number of authors per paper (AAP)
        "hyperauthor": "hyper_authorship" # holds the hyper authorship count (publications with more than 100 authors)
    }

    # vertex type symbols 
    _v_symbols = {
        "AUTHOR": "circle",
        "PAPER": "diamond",
        "VENUE": "triangle-up",
        "PUBLISHER": "pentagon",
        "KEYWORD": "hexagram-dot"
    }

    def __init__(self, cassandra_points, scipi_path, is_local):
        
        # connecting to cluster & setting up session
        self.cluster = Cluster(cassandra_points)
        self.session = self.cluster.connect() 
        self.session.set_keyspace(self._keyspace) # setting up keyspace

        # set a custom row factory 
        self.session.row_factory = self.pandas_factory

        # set default fetch size to unlimited 
        self.session.default_fetch_size = None     

        # set path where scipi is located 
        self.scipi_path = scipi_path     

        # set whether executing on local or cloud environment 
        self.is_local = is_local

    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

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

    def plot_hyper_authorship(self, earliest):

        # get results from aap as a pandas dataframe 
        table = self.cassandra_tbls["hyperauthor"]
        df = self.session.execute("SELECT hyper_authorship_year, hyper_authorship_count FROM " + table + ";",
                                  timeout=None)._current_rows

        # convert year to numeric 
        df[["hyper_authorship_year"]] = df[["hyper_authorship_year"]].apply(pd.to_numeric) 
        
        # get only records which are greater or equal to the year passed
        df = df[(df["hyper_authorship_year"] >= earliest)]

        # set column names 
        df.columns = ["year", "No. of Publications"]

        # set year column as index
        df.set_index("year", inplace=True)

        # sort dataframe by year 
        df.sort_values(by=["year"], 
                       ascending=False, 
                       inplace=True)
        
        # set table title 
        first_year = df.head(1).index.values[0]
        last_year = df.tail(1).index.values[0]
        tbl_title = "Publications with >= 100 Authors from {} to {}.".format(first_year, last_year)

        # show table using pyplot 
        trace = go.Table(
                header=dict(values=["year"] + list(df.columns),
                            fill = dict(color='#C2D4FF'),
                            align = ['left'] * 5),
                cells=dict(values=[df.index.values, 
                                   df["No. of Publications"]],
               fill = dict(color='#F5F8FF'),
               align = ['left'] * 5))

        layout = dict(width=800, height=300)
        table_result = [trace] 
        fig = dict(data=table_result, layout=layout)
        iplot(fig, filename = "pandas_table")

        # plot authorship goes hyper
        iplot(df[["No. of Publications"]].iplot(
                                       asFigure=True,
                                       kind="scatter",
                                       xTitle="Year",
                                       yTitle="No. of Publications",
                                       title=tbl_title))    

    # apply community detection using field of study 
    def apply_community_detection_fos(self, sender):
        
        # get src for bash according to execution env
        bash_src = "/src/scripts/local_flink_community_fos.sh" if self.is_local else "/src/scripts/cloud_flink_community_fos.sh"
        script_path = "{}{}".format(self.scipi_path, bash_src)
        print("Executing {}".format(script_path))

        # apply community detection using flink 
        print("Applying community detection using Domain/s: {}".format(sender.value))
        self.apply_community_detection(values=sender.value, 
                                       script_path=script_path)

    # apply community detection using keywords 
    def apply_community_detection_kw(self, sender):
        
        # get src for bash according to execution env
        bash_src = "/src/scripts/local_flink_community_kw.sh" if self.is_local else "/src/scripts/cloud_flink_community_kw.sh"
        script_path = "{}{}".format(self.scipi_path, bash_src)
        print("Executing {}".format(script_path))

        # apply community detection using flink 
        print("Applying community detection using Keywords/s: {}".format(sender.value))
        self.apply_community_detection(values=sender.value, 
                                       script_path=script_path)

    def apply_community_detection(self, values, script_path):

        # get results path 
        results_path = "{}{}".format(self.scipi_path + "/results", self.community_result_path)
        
        # script_path = "../scripts/local_flink_community_kw.sh"
        shellscript = subprocess.Popen([script_path, 
                                        self.scipi_path, 
                                        values, 
                                        results_path], stdin=subprocess.PIPE)
        
        # blocks until shellscript is done
        shellscript.stdin.close()
        shellscript.wait()   

        # plot results 
        # plot dense communities in publication network
        # plot network using twopi layout for community #1
        self.plot_community(layout="twopi", 
                            community_colors=self.community_colors,
                            result_path=results_path)

        # print job completed
        print("Job Completed")

    def community_detection(self, community_colors, result_path):
        print("IMPORTANT: Wait for print complete to continue and press ENTER ONCE on one text box\n")
        
        # set community variables
        self.community_colors = community_colors
        self.community_result_path = result_path

        # input if you want to use keywords 
        print("Apply Community Detection using keywords (Comma Separated) - PRESS ENTER")
        keywords_text = widgets.Text()
        display(keywords_text)
        keywords_text.on_submit(self.apply_community_detection_kw)

        # input if you want to use field of study 
        print("Apply Community Detection using fields of study (Comma Separated) - PRESS ENTER")
        domain_text = widgets.Text()
        display(domain_text)
        domain_text.on_submit(self.apply_community_detection_fos)

    def apply_association_analysis(self, sender):

        # get src for bash according to execution env
        bash_src = "/src/scripts/local_flink_association.sh" if self.is_local else "/src/scripts/cloud_flink_association.sh"
        script_path = "{}{}".format(self.scipi_path, bash_src)
        print("Executing {}".format(script_path))

        # get results path 
        results_path = "{}{}".format(self.scipi_path + "/results", self.association_result_path)

        shellscript = subprocess.Popen([script_path,
                                        self.scipi_path,
                                        sender.value, 
                                        results_path], stdin=subprocess.PIPE)
        
        # blocks until shellscript is done
        shellscript.stdin.close()
        shellscript.wait()      

        # show plots 
        # plot association between authors & keywords based on Title 
        self.plot_author_keyword(layout="neato")

        # plot author clustering based on keyword/potential collaborators 
        g_layout_dot="dot" # hierarchical
        self.plot_author_clustering(layout=g_layout_dot) # plot potential collaborators network

        # print job completed
        print("Job Completed")

    def author_association(self, result_path):
        print("IMPORTANT: Wait for print complete to continue and press ENTER ONCE on one text box\n")

        self.association_result_path = result_path

        # input for keywords to apply association/correlation analysis 
        print("Keywords to apply association/correlation analysis (Comma Separated) - PRESS ENTER")
        association_text = widgets.Text()
        display(association_text)
        association_text.on_submit(self.apply_association_analysis)

    def plot_community(self, layout, community_colors, result_path):
        
        # get edges result 
        path = "{}{}".format(result_path, "/communitySample.csv")
        with open(path) as f:
            data=[tuple(line) for line in csv.reader(f)]

        # create a new graph 
        G = nx.DiGraph()

        # create a dictionary to hold colors for each community 
        colors_dist = {}

        # loop in all edges 
        for edge in data:

            vertex_a = edge[0]
            vertex_a_type = edge[1]
            vertex_a_label = edge[2]
            G.add_node(vertex_a, type=vertex_a_type, community=vertex_a_label)    
            
            if vertex_a_label not in colors_dist:
                colors_dist[vertex_a_label] = community_colors[len(colors_dist)]

            vertex_b = edge[3]
            vertex_b_type = edge[4]
            vertex_b_label = edge[5]
            G.add_node(vertex_b, type=vertex_b_type, community=vertex_b_label)    

            if vertex_b_label not in colors_dist:
                colors_dist[vertex_b_label] = community_colors[len(colors_dist)]

            G.add_edge(vertex_a, vertex_b)

        pos = nx.nx_agraph.graphviz_layout(G, prog=layout)
        for n, p in pos.items():
            G.node[n]["pos"] = p

        # create the node trace 
        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode="markers",
            hoverinfo="text",
            marker=dict(
                color=[],
                size=12,
                symbol = [],
                line=dict(width=2)))

        # set node properties to node trace 
        for node in G.nodes():

            # get current node 
            tmp_node = G.node[node]
            
            # set positions in trace
            x, y = tmp_node["pos"]
            node_trace["x"] += tuple([x])
            node_trace["y"] += tuple([y])

            # set color in trace
            c = colors_dist[tmp_node["community"]]
            node_trace["marker"]["color"] += tuple([c])

            # set info in trace 
            # NODE TYPE: NAME 
            node_type = tmp_node["type"]
            node_info = "{}<br>{}".format(node_type, node)
            node_trace["text"]+=tuple([node_info])

            # set marker for each node 
            s = self._v_symbols[node_type]
            node_trace["marker"]["symbol"] += tuple([s])
        
        # create edge trace 
        edge_trace = go.Scatter(
            x=[],
            y=[],
            line=dict(width=0.5, color="#888"),
            hoverinfo='none',
            mode='lines')

        # set edge properties to edge trace 
        for edge in G.edges():
            x0, y0 = G.node[edge[0]]["pos"]
            x1, y1 = G.node[edge[1]]["pos"]
            edge_trace["x"] += tuple([x0, x1, None])
            edge_trace["y"] += tuple([y0, y1, None])

        # plot graph network 
        title ="<br>Dense Communities in Publications Network (Layout: {})".format(layout)
        fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                title=title,
                titlefont=dict(size=16),
                showlegend=False,
                hovermode="closest",
                margin=dict(b=20,l=5,r=5,t=40),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

        # show plot 
        iplot(fig, filename="networkx")

        # print total nodes & edges
        print("No. Nodes: {}\nNo. Edges: {}".format(G.number_of_nodes(), G.number_of_edges()))

        # show all communities and it's count 
        path = "{}{}".format(result_path, "/communityLabelCount.csv")
        community_count = pd.read_csv(path,header=None)
        community_count.columns = ["Labels", "Count"]

        # print stats
        print("\n\nCommunity Detection Stats\n##############################")
        print("Total No. of Communities with >= 500 entities (dense communities): {}".format(community_count.shape[0]))
        total_entites = community_count["Count"].sum() 
        
        print("Total Entities in these Communities: {}".format(total_entites))
        
        community_count["Count"] = pd.to_numeric(community_count["Count"], 
                                                 downcast="float")

        count_sum = community_count["Count"].sum()       
        community_count["Weighted score"] = round(community_count["Count"] * (community_count["Count"] / count_sum), 2)
        collaboration_strength = community_count["Weighted score"].sum() / 100
        print("Strength between these Communities using weighted avg. : {:.2f}".format(collaboration_strength))

    def plot_author_keyword(self, layout):
        
        # path for result
        path = "{}{}{}".format(self.scipi_path + "/results", self.association_result_path, "/authorKwSample.csv")
        
        # get author to keywords edges result 
        with open(path) as f:
            data=[tuple(line) for line in csv.reader(f)]

        # create a new graph 
        G = nx.Graph()

        # loop in all edges (author -> keyword)
        for edge in data:
            
            # get author & add vertex 
            vertex_a = edge[1]
            G.add_node(vertex_a, type="AUTHOR")    
            
            # get keyword & add vertex 
            vertex_b = edge[0]
            G.add_node(vertex_b, type="KEYWORD")    

            # get weight for edge
            edge_weight = edge[2]
            
            # add an edge between the author and keyword  
            G.add_edge(vertex_a, vertex_b, weight=edge_weight)

        # generate position for vertices according to the layout defined
        pos = nx.nx_agraph.graphviz_layout(G, prog=layout)
        for n, p in pos.items():
            G.node[n]["pos"] = p

        # create the node trace 
        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode="markers",
            hoverinfo="text",
            marker=dict(
                color="red",
                size=12,
                symbol = [],
                line=dict(width=2)))

        # set node properties to node trace 
        for node in G.nodes():

            # get current node 
            tmp_node = G.node[node]
            
            # set positions in trace
            x, y = tmp_node["pos"]
            node_trace["x"] += tuple([x])
            node_trace["y"] += tuple([y])

            # set info in trace 
            # NODE TYPE: NAME 
            node_type = tmp_node["type"]
            node_info = "{}<br>{}".format(node_type, node)
            node_trace["text"]+=tuple([node_info])

            # set marker for each node 
            s = self._v_symbols[node_type]
            node_trace["marker"]["symbol"] += tuple([s])

        # create edge trace 
        edge_trace = go.Scatter(
            x=[],
            y=[],
            line=dict(width=0.5, color="#888"),
            hoverinfo="none",
            mode="lines")

        # set edge properties to edge trace 
        for edge in G.edges():
            x0, y0 = G.node[edge[0]]["pos"]
            x1, y1 = G.node[edge[1]]["pos"]
            edge_trace["x"] += tuple([x0, x1, None])
            edge_trace["y"] += tuple([y0, y1, None])

        # plot graph network 
        title ="<br>Author and keyword associations (Layout: {})".format(layout)
        fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                title=title,
                titlefont=dict(size=16),
                showlegend=False,
                hovermode="closest",
                margin=dict(b=20,l=5,r=5,t=40),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

        # show plot 
        iplot(fig, filename="networkx")

        # print total nodes & edges
        print("No. Nodes: {}\nNo. Edges: {}".format(G.number_of_nodes(), G.number_of_edges()))

    def plot_author_clustering(self, layout):
        
        # path for result
        path = "{}{}{}".format(self.scipi_path + "/results", self.association_result_path, "/authorsCollabSample.csv")
        
        # get author edges result 
        with open(path) as f:
            data=[tuple(line) for line in csv.reader(f)]

        # create a new graph 
        G = nx.Graph()

        # loop in all edges (author -> author)
        for edge in data:
            
            # get author & add vertex 
            vertex_a = edge[0]
            G.add_node(vertex_a)    
            
            # get second author & add vertex 
            vertex_b = edge[1]
            G.add_node(vertex_b)    

            # add an edge between the two authors 
            G.add_edge(vertex_a, vertex_b)

        # generate position for vertices according to the layout defined
        pos = nx.nx_agraph.graphviz_layout(G, prog=layout)
        for n, p in pos.items():
            G.node[n]["pos"] = p

        # create potential collaborators table 
        collab_list = []
        for node, adjacencies in G.adjacency():
            authors = " | ".join(list(adjacencies.keys()))
            collab_list.append((node, authors, len(adjacencies)))

        # create dataframe 
        columns = ["Author", "Potential Collaborators", "No. Collaborators"]
        df = pd.DataFrame([x for x in collab_list], columns=columns)

        # show table using pyplot 
        # show table using pyplot 
        trace = go.Table(
                header=dict(values=list(df.columns),
                            fill = dict(color='#C2D4FF'),
                            align = ['left'] * 5),
                cells=dict(values=[df["Author"],
                                   df["Potential Collaborators"],
                                   df["No. Collaborators"]],
               fill = dict(color='#F5F8FF'),
               align = ['left'] * 5))

        tbl_layout = dict(width=900, height=700)
        table_result = [trace] 
        fig = dict(data=table_result, layout=tbl_layout)
        iplot(fig, filename = "pandas_table")

        # create the node trace for the plot
        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode="markers",
            hoverinfo="text",
            marker=dict(
                showscale=True,
                color=[],
                reversescale=True,
                colorscale="Electric",
                size=12,
                colorbar=dict(
                    thickness=15,
                    title="Potential Collaborators",
                    xanchor="left",
                    titleside="right"),
                line=dict(width=2)))

        # set node properties to node trace 
        for node in G.nodes():

            # get current node 
            tmp_node = G.node[node]
            
            # set positions in trace
            x, y = tmp_node["pos"]
            node_trace["x"] += tuple([x])
            node_trace["y"] += tuple([y])

         # create edge trace 
        edge_trace = go.Scatter(
            x=[],
            y=[],
            line=dict(width=0.5, color="#888"),
            hoverinfo="none",
            mode="lines")

        # set edge properties to edge trace 
        for edge in G.edges():
            x0, y0 = G.node[edge[0]]["pos"]
            x1, y1 = G.node[edge[1]]["pos"]
            edge_trace["x"] += tuple([x0, x1, None])
            edge_trace["y"] += tuple([y0, y1, None])

        # set info each node 
        # author name => no. of potential collab
        for node, adjacencies in G.adjacency():
            ajd_len = len(adjacencies)
            node_trace["marker"]["color"]+=tuple([ajd_len])
            node_info = "{}<br># of potential collaborators:{}".format(node ,str(ajd_len))
            node_trace['text']+=tuple([node_info])
            
        # plot potential collaborators network 
        title ="<br>Potential Collaborators in Publications Network (Layout: {})".format(layout)
        fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                title=title,
                titlefont=dict(size=16),
                showlegend=False,
                hovermode="closest",
                margin=dict(b=20,l=5,r=5,t=40),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

        # show plot 
        iplot(fig, filename="networkx")

        # print total nodes & edges
        print("No. Nodes: {}\nNo. Edges: {}".format(G.number_of_nodes(), G.number_of_edges()))
        
    def close(self):

        # closes connections/sessions with cassandraDB cluster 
        self.cluster.shutdown()