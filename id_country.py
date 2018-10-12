from pyspark.sql import SparkSession
from pyspark import *
import ipaddress
import numpy as np

sparkSession = SparkSession.builder.appName("idCountry").getOrCreate()

#Import our parsed IPs
parsedIP = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="parsedips", keyspace="projectip").load()

#Import our GeoLite3 Network data
geoliteNetwork = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="geonetworks", keyspace="projectip").load()

#Import our GeoLite3 Countries data
geoliteCountry = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="geocountries", keyspace="projectip").load()


#Join our geolite network and countries
geoliteJoined = geoliteNetwork.join(geoliteCountry, geoliteNetwork.geoname_id == geoliteCountry.geoname_id)
geoliteJoined.createOrReplaceTempView("geoJoined")



geoliteData = sparkSession.sql("SELECT network,country_name FROM geoJoined")
geoliteData.printSchema()
parsedIP.printSchema()

topIPs = parsedIP.filter(parsedIP['ipcount'] > 10)
topIPs.printSchema()


ipArray = np.array(topIPs.select("ipaaddress").collect())
#print(ipArray)
geoArray = np.array(geoliteData.select("network", "country_name").collect())
#ipData = sparkSession.sql("SELECT ipaddress FROM parsedIP WHERE ipcount > 6")

print(np.shape(ipArray))
print(np.shape(geoArray))

for ip in ipArray[:,1]:
    for network in geoArray[:,1]:
        ipaddress.ip_address(ip) in ipaddress.ip_network(network)