import pandas as pd
import networkx as nx
import streamlit as st
from pyvis.network import Network
import community as community_louvain
from collections import Counter
import ast
import tempfile
import os

# -----------------------------
# CONFIG STREAMLIT
# -----------------------------
st.set_page_config(page_title="Social Network Analysis Dashboard", layout="wide")
st.title("🕸️ Social Network Analysis & Dashboard")

# -----------------------------
# LOAD DATA
# -----------------------------
@st.cache_data
def load_data():
    df = pd.read_csv("processed_news.csv")
    df["keywords"] = df["keywords"].fillna("")
    df["author"] = df["author"].fillna("Unknown")
    return df

df = load_data()

st.sidebar.header("📊 Filters")
selected_keyword = st.sidebar.selectbox(
    "Filter by keyword",
    ["All"] + sorted(set(",".join(df["keywords"]).split(",")))
)

# -----------------------------
# FILTER DATA
# -----------------------------
if selected_keyword != "All":
    df = df[df["keywords"].str.contains(selected_keyword, case=False)]

# -----------------------------
# BUILD GRAPH (Author ↔ Keyword)
# -----------------------------
G = nx.Graph()

for _, row in df.iterrows():
    author = row["author"]
    keywords = [k.strip() for k in row["keywords"].split(",") if k.strip()]

    G.add_node(author, node_type="author")

    for kw in keywords:
        G.add_node(kw, node_type="keyword")
        if G.has_edge(author, kw):
            G[author][kw]["weight"] += 1
        else:
            G.add_edge(author, kw, weight=1)

# -----------------------------
# CENTRALITY MEASURES
# -----------------------------
degree_centrality = nx.degree_centrality(G)
betweenness_centrality = nx.betweenness_centrality(G)

nx.set_node_attributes(G, degree_centrality, "degree")
nx.set_node_attributes(G, betweenness_centrality, "betweenness")

# -----------------------------
# COMMUNITY DETECTION
# -----------------------------
partition = community_louvain.best_partition(G)
nx.set_node_attributes(G, partition, "community")

# -----------------------------
# NETWORK VISUALIZATION
# -----------------------------
st.subheader("🌐 Network Graph")

net = Network(height="600px", width="100%", bgcolor="#0e1117", font_color="white")

for node, data in G.nodes(data=True):
    size = 20 + degree_centrality.get(node, 0) * 100
    color = "#1f77b4" if data["node_type"] == "author" else "#ff7f0e"

    net.add_node(
        node,
        label=node,
        size=size,
        color=color,
        title=f"""
        Degree: {degree_centrality.get(node, 0):.3f}
        Betweenness: {betweenness_centrality.get(node, 0):.3f}
        Community: {partition.get(node)}
        """
    )

for u, v, data in G.edges(data=True):
    net.add_edge(u, v, value=data["weight"])

with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
    net.save_graph(tmp.name)
    st.components.v1.html(open(tmp.name, "r", encoding="utf-8").read(), height=650)

# -----------------------------
# TOP NODES TABLE
# -----------------------------
st.subheader("🏆 Top Influential Nodes")

top_nodes = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]

top_df = pd.DataFrame(top_nodes, columns=["Node", "Degree Centrality"])
top_df["Betweenness Centrality"] = top_df["Node"].map(betweenness_centrality)
top_df["Community"] = top_df["Node"].map(partition)

st.dataframe(top_df, use_container_width=True)

# -----------------------------
# STATS
# -----------------------------
st.subheader("📈 Network Statistics")

col1, col2, col3 = st.columns(3)

col1.metric("Number of Nodes", G.number_of_nodes())
col2.metric("Number of Edges", G.number_of_edges())
col3.metric("Number of Communities", len(set(partition.values())))

st.success("✅ Social Network Analysis completed successfully!")
