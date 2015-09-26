import psycopg2
import Utils

class Map(object):
    def __init__(self):
        self.init_db()
    
    def init_db(self):
        print "Connecting to database ......"
        self.conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
        if self.conn_to:
            print "Connected."
        self.cursor = self.conn_to.cursor()
        
    def close_db(self):
        self.conn_to.close()
        
    def locate_links(self, road_name, function_class_numeric):
    #find (lon, lat) of (from_node, to_node) of all links on the certain highway
    
        print "Begin locating links on " + road_name
        
        link_loc = {}
    
        if function_class_numeric == 1:
            sql = "select link_id, from_node_id, to_node_id from links where function_class_numeric=1 and name_default like '%" + road_name + "%'"
        else:
            sql = "select link_id, from_node_id, to_node_id from links where function_class_numeric in (3,4) and upper(name_default) like '%" + road_name + "%'"
        self.cursor.execute(sql)
        nodes = self.cursor.fetchall()
        for (link_id, from_node_id, to_node_id) in nodes:
            sql = "select ST_AsText(geom) from nodes where node_id =" + str(from_node_id)
            self.cursor.execute(sql)
            from_node_pos = self.cursor.fetchall()[0][0]
            from_node_loc = Utils.extract_loc_from_geometry(from_node_pos)
            
            sql = "select ST_AsText(geom) from nodes where node_id =" + str(to_node_id)
            self.cursor.execute(sql)
            to_node_pos = self.cursor.fetchall()[0][0]
            to_node_loc = Utils.extract_loc_from_geometry(to_node_pos)
            
            link_loc[link_id] = (from_node_loc, to_node_loc)
        
        print "Link locating finished, there are " + str(len(link_loc)) + " links on " + road_name
        return link_loc
    
    def filter_bearing(self, link_loc, direction):
    #filter all links which have the certain direction
    #direction: 0-North 1-South 2-East 3-West
    
        print "Begin filtering links with right direction"
        
        bearing_links = []
        for link_id in link_loc:
            heading = Utils.bearing(link_loc[link_id][0][0], link_loc[link_id][0][1] , link_loc[link_id][1][0], link_loc[link_id][1][1])
            if direction == 0 and (heading > 270 or heading < 90):
                bearing_links.append(link_id)
            elif direction == 1 and (heading > 90 and heading < 270):
                bearing_links.append(link_id)
            elif direction == 2 and (heading > 180 and heading < 360):
                bearing_links.append(link_id)
            elif direction == 3 and (heading > 0 and heading < 180):
                bearing_links.append(link_id)
                
        print "After bearing filtering, there are " + str(len(bearing_links)) + " links left"
        return bearing_links
    
    def find_start_end_link(self, link_loc, bearing_links, start_lon, start_lat, end_lon, end_lat):
    #According to approximate (lon,lat) find closest node and then find the first and the last link
    
        print "Begin finding the first and the last links on the section"
    
        start_closest_dist = 99999
        end_closest_dist = 99999
        for link_id in bearing_links:
            from_node_loc = link_loc[link_id][0]
            to_node_loc = link_loc[link_id][1]
            start_dist = Utils.map_dist(from_node_loc[0], from_node_loc[1], start_lon, start_lat)
            if start_dist < start_closest_dist:
                start_link = link_id
                start_closest_dist = start_dist
                
            end_dist = Utils.map_dist(to_node_loc[0], to_node_loc[1], end_lon, end_lat)
            if end_dist < end_closest_dist:
                end_link = link_id
                end_closest_dist = end_dist
        
        print "The start node is ", link_loc[start_link][0][1], link_loc[start_link][0][0] , " and the end node is ", link_loc[end_link][1][1], link_loc[end_link][1][0]
        
        return start_link, end_link
    
    def fill_path(self, link_loc, bearing_links, start_link, end_link, direction):
    #Find all links between start_link and end_link
    
        print "Begin filling the whole section"
    
        path = []
        start_node_loc = link_loc[start_link][0]
        end_node_loc = link_loc[end_link][1]
        for link_id in bearing_links:
            if direction == 0 and link_loc[link_id][0][1] >= start_node_loc[1] and link_loc[link_id][1][1] <= end_node_loc[1]:
                path.append(link_id)
            elif direction == 1 and link_loc[link_id][0][1] <= start_node_loc[1] and link_loc[link_id][1][1] >= end_node_loc[1]:
                path.append(link_id)
            elif direction == 2 and link_loc[link_id][0][0] >= start_node_loc[0] and link_loc[link_id][1][0] <= end_node_loc[0]:
                path.append(link_id)
            elif direction == 3 and link_loc[link_id][0][0] <= start_node_loc[0] and link_loc[link_id][1][0] >= end_node_loc[0]:
                path.append(link_id)
                
        print "Section filling finished"
        
        return path

'''
if __name__ == '__main__':
    lamap = Map()
    paths = []
    path_file = open('path.txt', 'w')
    
    #First Section
    road_name = "I-105"
    direction = 2    #0:N 1:S 2:E 3:W
    start_loc = (-118.321807, 33.925263) #Crenshaw Blvd
    end_loc = (-118.259024, 33.927349) #Central Avenue
    
    link_loc = lamap.locate_links(road_name)
    bearing_links = lamap.filter_bearing(link_loc, direction)
    start_link, end_link = lamap.find_start_end_link(link_loc, bearing_links, start_loc[0], start_loc[1], end_loc[0], end_loc[1])
    path = lamap.fill_path(link_loc, bearing_links, start_link, end_link, direction)
    for link in path:
        path_file.write(str(link_loc[link][0][1])+','+str(link_loc[link][0][0])+'   '+str(link_loc[link][1][1])+','+str(link_loc[link][1][0])+'\n')
    paths += path
    
    #Second Section
    road_name = "I-405"
    direction = 0    #0:N 1:S 2:E 3:W
    start_loc = (-118.369430, 33.961703) #W Manchester Blvd
    end_loc = (-118.408466, 33.999652) #Culver Blvd
    
    link_loc = lamap.locate_links(road_name)
    bearing_links = lamap.filter_bearing(link_loc, direction)
    start_link, end_link = lamap.find_start_end_link(link_loc, bearing_links, start_loc[0], start_loc[1], end_loc[0], end_loc[1])
    path = lamap.fill_path(link_loc, bearing_links, start_link, end_link, direction)
    for link in path:
        path_file.write(str(link_loc[link][0][1])+','+str(link_loc[link][0][0])+'   '+str(link_loc[link][1][1])+','+str(link_loc[link][1][0])+'\n') 
    paths += path
    
    #Third Section
    road_name = "CA 134"
    direction = 3    #0:N 1:S 2:E 3:W
    start_loc = (-118.162360, 34.146591) #W Colorado Blvd
    end_loc = (-118.240473, 34.155208) #Cordova Ave
    
    link_loc = lamap.locate_links(road_name)
    bearing_links = lamap.filter_bearing(link_loc, direction)
    start_link, end_link = lamap.find_start_end_link(link_loc, bearing_links, start_loc[0], start_loc[1], end_loc[0], end_loc[1])
    path = lamap.fill_path(link_loc, bearing_links, start_link, end_link, direction)
    for link in path:
        path_file.write(str(link_loc[link][0][1])+','+str(link_loc[link][0][0])+'   '+str(link_loc[link][1][1])+','+str(link_loc[link][1][0])+'\n') 
    paths += path
        
    #Fourth Section
    road_name = "US 101"
    direction = 1    #0:N 1:S 2:E 3:W
    start_loc = (-118.370611, 34.146952) #Vineland Ave
    end_loc = (-118.338808, 34.120084) #Cahuenga Blvd W
    
    link_loc = lamap.locate_links(road_name)
    bearing_links = lamap.filter_bearing(link_loc, direction)
    start_link, end_link = lamap.find_start_end_link(link_loc, bearing_links, start_loc[0], start_loc[1], end_loc[0], end_loc[1])
    path = lamap.fill_path(link_loc, bearing_links, start_link, end_link, direction)
    for link in path:
        path_file.write(str(link_loc[link][0][1])+','+str(link_loc[link][0][0])+'   '+str(link_loc[link][1][1])+','+str(link_loc[link][1][0])+'\n')
    paths += path
    
    path_file.close()
'''