import psycopg2
import Utils

class FindNode(object):
    def pre_links(self):
        road_links = {}
        roads = []
        print "Connecting to database ......"
        self.conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
        if self.conn_to:
            print "Connected."
        self.cursor = self.conn_to.cursor() 

        sql = "select road_name, direction, from_postmile, to_postmile,link_id from \"SS_SENSOR_MAPPING_ALL\""
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for road_name, direction, from_postmile, to_postmile, link_id in results:
            section = from_postmile / 3
            if link_id not in road_links:
                road_links[link_id] = {}
                road_links[link_id]['road_name']=road_name
                road_links[link_id]['direction']=direction
                road_links[link_id]['section']=section
                if road_name not in roads:
                    roads.append(road_name)
                
        return  roads, road_links
        
    def pre_nodes(self, roads, road_links):
        print "Init links"
        nodes = {}
        for road_name in roads:
            sql = "select link_id, from_node_id, to_node_id from links where name_default like '%"+road_name+"%'"
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for link_id, from_node_id, to_node_id in results:
                if link_id in road_links:
                    road_links[link_id]['from_node'] = from_node_id
                    road_links[link_id]['to_node'] = to_node_id
                    if from_node_id not in nodes:
                        nodes[from_node_id] = {}
                        nodes[from_node_id]['link'] = []
                    nodes[from_node_id]['link'].append(link_id)
                    if to_node_id not in nodes:
                        nodes[to_node_id] = {}
                        nodes[to_node_id]['link'] = []
                    nodes[to_node_id]['link'].append(link_id)
        
        print "Init nodes"

        sql = "select node_id, ST_AsText(geom) from nodes"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for node_id, geom in results:
            if node_id in nodes:
                node_loc = Utils.extract_loc_from_geometry(geom)
                nodes[node_id]['loc'] = node_loc
            
        return road_links, nodes
    
    def find_link(self, nodes, road_links, lon, lat, direction):
        nodes_dist = []
        for node_id in nodes:
            dist = Utils.map_dist(nodes[node_id]['loc'][0], nodes[node_id]['loc'][1], lon, lat)
            n = [node_id, dist]
            nodes_dist.append(n)
            
        nodes_dist.sort(key = lambda x:x[1], reverse=False)
        
        for node in nodes_dist[:3]:
            node_id = node[0]
            for link in nodes[node_id]['link']:
                if not road_links[link]['direction'] == direction:
                    continue
                from_node = road_links[link]['from_node']
                to_node = road_links[link]['to_node']
                from_lon, from_lat = nodes[from_node]['loc']
                to_lon, to_lat = nodes[to_node]['loc']
                if direction < 2:
                    m = min(from_lat, to_lat)
                    x = max(from_lat, to_lat)
                    cpr = (lat >= m and lat <= x)
                else:
                    m = min(from_lon, to_lon)
                    x = max(from_lon, to_lon)
                    cpr = (lon >= m and lon <= x)
                
                if cpr:
                    print "node_id:",node_id
                    print "dist:", node[1]
                    print "location:", nodes[node_id]['loc'][1], nodes[node_id]['loc'][0]
                    print "link_id:", link
                    print "start_loc:", from_lat, from_lon
                    print "end_loc:", to_lat, to_lon
                    print "road_name:", road_links[link]['road_name']
                    print "link_direction:", road_links[link]['direction']
                    print "post_mile of section:",3*road_links[link]['section'], "-", 3*road_links[link]['section']+3
                    print "\n\n"
                    
    def display_all(self, points):
        roads, road_links = fd.pre_links()
        road_links, nodes = fd.pre_nodes(roads, road_links)
        for lon,lat,direction in points:
            print "Search Result of Point", points.index((lon, lat,direction))+1, ":", lat, lon
            fd.find_link(nodes, road_links, lon, lat, direction)
            print "\n\n\n\n"

if __name__ == '__main__':
    fd = FindNode()
    
    points = [
        (34.007892, -117.825872, 3)
    ]
    
    fd.display_all(points)
    
    
    
    
    