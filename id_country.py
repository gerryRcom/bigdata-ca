#https://docs.python.org/3/library/ipaddress.html
import ipaddress
#from netaddr import IPNetwork, IPAddress
if ipaddress.IPv4Address('192.168.2.1') in ipaddress.IPv4Network('192.168.0.0/24'):
    print("Match")
else:
    print("No Match")
print("nothing")