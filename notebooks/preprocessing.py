# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext sparkmagic.magics

# %%
import os
from IPython import get_ipython
username = os.environ['RENKU_USERNAME']
server = "http://iccluster029.iccluster.epfl.ch:8998"

# set the application name as "<your_gaspar_id>-final"
get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-final", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G"}}""".format(username)
)

# %%
get_ipython().run_line_magic(
    "spark", "add -s {0}-final -l python -u {1} -k".format(username, server)
)

# %% language="spark"
# print('We are using Spark %s' % spark.version)

# %% [markdown]
# ### Load data

# %% language="spark"
# idtdaten = spark.read.orc('/data/sbb/orc/istdaten')
# idtdaten.printSchema()

# %% language="spark"
# #idtdaten.summary().show()

# %% [markdown]
# ### Preprocess Stations data (allstops)
# - We only consider departure and arrival stops in a 15km radius of Zürich's train station, `Zürich HB (8503000)`, (lat, lon) = `(47.378177, 8.540192)`.
# - We only consider stops in the 15km radius that are reachable from Zürich HB. If needed stops may be reached via transfers through other stops outside the 15km area.

# %% language="spark"
# allstops = spark.read.orc('/data/sbb/orc/allstops')
# allstops.printSchema()

# %% language="spark"
# print(allstops.count())
# allstops.show(5)

# %% language="spark"
# import pyspark.sql.functions as functions
# from math import sin, cos, sqrt, atan2, radians
#
# # input lat/lon in degree
# # output distance in KM
# # equation from https://www.movable-type.co.uk/scripts/latlong.html
# def lat_lon_to_distance(lat1, lon1, lat2, lon2):
#     # earth radius in KM
#     R = 6371.0
#
#     lat1 = radians(lat1)
#     lon1 = radians(lon1)
#     lat2 = radians(lat2)
#     lon2 = radians(lon2)
#
#     dlat = lat2 - lat1
#     dlon = lon2 - lon1
#     
#     a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#     c = 2 * atan2(sqrt(a), sqrt(1-a))
#
#     return R * c
#
# # test
# assert(int(lat_lon_to_distance(20, 30, 40, 50)) == 2927)

# %%
