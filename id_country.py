from pyspark.sql import SparkSession
from pyspark import *
import ipaddress
#Numpy needed to parse the data through the ipaddress function
import numpy as np
#Pandas need to get the data back to DataFrame formt for import to MySql
#required running: pip install pandas
import pandas as pd

sparkSession = SparkSession.builder.appName("idCountry").getOrCreate()

#Import our parsed IPs
parsedIP = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="parsedips", keyspace="projectip").load()

#Import our GeoLite2 Network data
geoliteNetwork = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="geonetworks", keyspace="projectip").load()

#Import our GeoLite2 Countries data
geoliteCountry = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="geocountries", keyspace="projectip").load()


#Join our geolite network and countries
geoliteJoined = geoliteNetwork.join(geoliteCountry, geoliteNetwork.geoname_id == geoliteCountry.geoname_id)
geoliteJoined.createOrReplaceTempView("geoJoined")


#Create two data frames containg only the required data
geoliteData = sparkSession.sql("SELECT network,country_name FROM geoJoined")
topIPs = parsedIP.filter(parsedIP['ipcount'] > 8)


#Create two NumPy arrays to then process the data using the ipaddress library
ipArray = np.array(topIPs.select("ipaaddress").collect())
geoArray = np.array(geoliteData.select("network", "country_name").collect())

#Create a Pandas DataFrame to accept the results for sending back to the database later.
pdOutput = pd.DataFrame(columns=['Country'])
networkCounter=0


for ip in ipArray[:,0]:
    networkCounter = 0
    for network in geoArray[:,0]:
        #There are a few stray non IP details in the logs, rather than crashing out I'm am passing over them
        try:
            if ipaddress.ip_address(ip) in ipaddress.ip_network(network):
                #print ip
                #print geoArray[networkCounter,1]
                pdOutput = pdOutput.append({'Country':geoArray[networkCounter,1]}, ignore_index=True)
                #print pdOutput
                networkCounter = 0
            else:
                networkCounter = networkCounter + 1
        except:
            pass

#Transfer the Pandas DataFrame to a Spark DataFrame for submission to the MySql DB
sparkOutput = sparkSession.createDataFrame(pdOutput, schema=['Country'])

#Insert sparkOutput DataFrame content into a MySql DB for later visualising in Tableau
sparkOutput.write.format('jdbc').options(url='jdbc:mysql://localhost/ipproject',driver='com.mysql.jdbc.Driver',dbtable='spark_output',user='ipdbuser',password='hadoop.2018').mode('overwrite').save()