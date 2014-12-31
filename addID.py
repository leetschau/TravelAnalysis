import chnInit
import sys

target = sys.argv[1]
with open(target, 'r') as src:
    with open('loc_map.csv', 'w') as dst:
        for content in src:
            line = content.strip()
            cgi = line.split(' ,')[0]
            node = line.split(' ,')[1].split(' ')[0]
            city = line.split(' ,')[1].split(' ')[1]
            nodeID = chnInit.get_word_initial(node)
            cityID = chnInit.get_word_initial(city)
            dst.write(cgi + ' ,' + city + ' ' + cityID + ' ' + node + ' ' + nodeID + '\n')
