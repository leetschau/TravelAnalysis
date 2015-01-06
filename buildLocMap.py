nodeDict = {}
with open('nodedict.csv', 'r') as src:
    for content in src:
        line = content.strip()
        city = line.split(" ")[0]
        node = line.split(" ")[1]
        travel_type = line.split(" ")[2]
        nodeDict[node] = city + " " + travel_type

lastNode = ""
with open('gdNode.csv', 'r') as src:
    with open('gdLocMap.csv', 'w') as dst:
        for content in src:
            line = content.strip()
            if line != '""':
                lastNode = line
            city_travelType = nodeDict[lastNode]
            city = city_travelType.split(" ")[0]
            travel_type = city_travelType.split(" ")[1]
            target = city + " " + city + " " + lastNode + " " + lastNode +  " " + travel_type + "\n"
            dst.write(target)
