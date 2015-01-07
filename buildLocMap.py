nodeDict = {}
with open('node_city_dict.csv', 'r') as src:
    for content in src:
        line = content.strip()
        node = line.split(" ")[0]
        city = line.split(" ")[1]
        nodeDict[node] = city

lastNode = ""
with open('cellDict.csv', 'r') as src:
    with open('gdLocMap.csv', 'w') as dst:
        for content in src:
            line = content.strip()
            eles = line.split(",")
            if len(eles[1]) > 0:
                lastNode = eles[1]
            cgi = eles[0]
            city = nodeDict[lastNode]
            cityID = city
            node = lastNode
            nodeID = node
            longitude = eles[2]
            latitude = eles[3]
            target = cgi + "," + city + " " + cityID + " " + node  + \
                    " " + nodeID + " " + longitude + " " + latitude
            dst.write(target + "\n")
