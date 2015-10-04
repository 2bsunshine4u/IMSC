import psycopg2
import Utils

class Map(object):
    def __init__(self):
        self.init_db()
        self.link_loc = {}
        
    def init_db(self):
        print "Connecting to database ......"
        self.conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
        if self.conn_to:
            print "Connected."
        self.cursor = self.conn_to.cursor()
        
    def close_db(self):
        self.conn_to.commit()
        self.conn_to.close()
        
    def locate_links(self, road_name, function_class_numeric):
    #find (lon, lat) of (from_node, to_node) of all links on the certain highway
    
        #print "Begin locating links on",road_name
        
        if road_name in self.link_loc:
            return self.link_loc[road_name]
        
        link_loc = {}
    
        if function_class_numeric == 1:
            sql = "select link_id, from_node_id, to_node_id, name_default from links where function_class_numeric=1 and name_default like '%" + road_name + "%'"  
        self.cursor.execute(sql)
        nodes = self.cursor.fetchall()
        
        false_name = []
        true_name = []
        for (link_id, from_node_id, to_node_id, name_default) in nodes:
            if (len (name_default) >( name_default.index(road_name) + len(road_name))):
                if name_default[name_default.index(road_name)-1].isdigit() or name_default[name_default.index(road_name)+len(road_name)].isdigit():
                    if name_default not in false_name:
                        false_name.append(name_default)
                    continue
                if name_default.find(';') >= 0:
                    if name_default.index(';') < name_default.index(road_name):
                        if name_default not in false_name:
                            false_name.append(name_default)
                        continue
            else:
                if name_default[name_default.index(road_name)-1].isdigit():
                    if name_default not in false_name:
                        false_name.append(name_default)
                    continue
                if name_default.find(';') >= 0:
                    if name_default.index(';') < name_default.index(road_name):
                        if name_default not in false_name:
                            false_name.append(name_default)
                        continue
                    
            if name_default not in true_name:
                true_name.append(name_default)
                    
            sql = "select ST_AsText(geom) from nodes where node_id =" + str(from_node_id)
            self.cursor.execute(sql)
            from_node_pos = self.cursor.fetchall()[0][0]
            from_node_loc = Utils.extract_loc_from_geometry(from_node_pos)
            
            sql = "select ST_AsText(geom) from nodes where node_id =" + str(to_node_id)
            self.cursor.execute(sql)
            to_node_pos = self.cursor.fetchall()[0][0]
            to_node_loc = Utils.extract_loc_from_geometry(to_node_pos)
            
            link_loc[link_id] = (from_node_loc, to_node_loc, from_node_id, to_node_id)
        
        print "Wrong name:", false_name
        print "Right name:", true_name
        
        print "Link locating finished, there are " + str(len(link_loc)) + " links on " + road_name
        
        self.link_loc[road_name] = link_loc
        
        return link_loc
    
    def filter_range_bearing(self, link_loc, min_lon, max_lon, min_lat, max_lat, direction):
    #direction: 0-North 1-South 2-East 3-West
    
        print "Begin filtering links in region with right direction"
        
        filtered_links = []
        for link_id in link_loc:
            if link_loc[link_id][0][0] >= min_lon and link_loc[link_id][0][1] >= min_lat and link_loc[link_id][0][0] <  max_lon and link_loc[link_id][0][1] < max_lat:
                heading = Utils.bearing(link_loc[link_id][0][0], link_loc[link_id][0][1] , link_loc[link_id][1][0], link_loc[link_id][1][1])
                if direction == 0 and (heading >= 270 or heading < 90):
                    filtered_links.append(link_id)
                elif direction == 1 and (heading >= 90 and heading < 270):
                    filtered_links.append(link_id)
                elif direction == 2 and (heading >= 180 and heading < 360):
                    filtered_links.append(link_id)
                elif direction == 3 and (heading >= 0 and heading < 180):
                    filtered_links.append(link_id)
                
        return filtered_links
    
    def sort_links(self, link_loc, filtered_links, direction):
    
        print "Begin Sorting links"
    
        if direction == 0:
            filtered_links.sort(key=lambda x:link_loc[x][0][1],reverse=False)

        elif direction == 1:
            filtered_links.sort(key=lambda x:link_loc[x][0][1],reverse=True)
                    
        elif direction == 2:
            filtered_links.sort(key=lambda x:link_loc[x][0][0],reverse=False)
                    
        elif direction == 3:
            filtered_links.sort(key=lambda x:link_loc[x][0][0],reverse=True)

        print "After range and bearing filtering, there are " + str(len(filtered_links)) + " links left"
        return filtered_links
    
    def fill_path(self, link_loc, filtered_links, section_len):
    
        print "Begin filling the whole road and divide into sections"
    
        path = {}
        path[0] = []
        cur_idx = 0
        cur_sec = 0
        sec_start = link_loc[filtered_links[cur_idx]][0]
        while cur_idx < len(filtered_links): 
            cur_lon = link_loc[filtered_links[cur_idx]][0][0]
            cur_lat = link_loc[filtered_links[cur_idx]][0][1]
            if Utils.map_dist(cur_lon, cur_lat, sec_start[0], sec_start[1]) >= section_len:
                cur_sec += 1
                path[cur_sec] = []
                sec_start = link_loc[filtered_links[cur_idx-1]][1]
            while Utils.map_dist(cur_lon, cur_lat, sec_start[0], sec_start[1]) >= section_len:
                cur_sec += 1
                path[cur_sec] = []
                mid_lon = sec_start[0]+(cur_lon-sec_start[0]) * section_len /  Utils.map_dist(cur_lon, cur_lat, sec_start[0], sec_start[1])
                mid_lat = sec_start[1]+(cur_lat-sec_start[1]) * section_len /  Utils.map_dist(cur_lon, cur_lat, sec_start[0], sec_start[1])
                sec_start = [mid_lon, mid_lat]

            path[cur_sec].append(filtered_links[cur_idx])
            
            cur_idx += 1
        
        if len(path[cur_sec]) <= 3:
            print "The last section is meaningless, del it"
            del(path[cur_sec])
                
        print "Section filling finished"
        
        return path
    
    def process_road(self, road_name, function_class_number, direction, t_direction, min_lon, max_lon, min_lat, max_lat, section_len,turn):
        print "Begin processing road:", road_name,"direction",direction
        link_loc = self.locate_links(road_name,        function_class_numeric)
        
        if direction == t_direction:
            filtered_links = self.filter_range_bearing(link_loc, min_lon, max_lon, min_lat, max_lat, direction)
            filtered_links = self.sort_links(link_loc, filtered_links, direction)
        else:
            tn = turn[road_name][direction]
            link_loc = self.locate_links(road_name,        function_class_numeric)
            filtered_links1 = self.filter_range_bearing(link_loc, tn['min_lon1'], tn['max_lon1'], tn['min_lat1'], tn['max_lat1'], direction)
            filtered_links1 = self.sort_links(link_loc, filtered_links1, direction)
            filtered_links2 = self.filter_range_bearing(link_loc, tn['min_lon2'], tn['max_lon2'], tn['min_lat2'], tn['max_lat2'], t_direction)
            filtered_links2 = self.sort_links(link_loc, filtered_links2, t_direction)
            filtered_links = filtered_links1 + filtered_links2
            
        path = self.fill_path(link_loc, filtered_links, section_len)
        
        for i in path:
            print i, ':'
            for j in path[i]:
                    print link_loc[j][0][1], link_loc[j][0][0], ','
        
        return path
    
if __name__ == '__main__':
    lamap = Map()
    lamap.init_db()
    
    hwy_secs = {}
    
    min_lon = -119.4370 
    max_lon = -116.7240
    min_lat = 33.2980
    max_lat = 34.5830 
    section_len = 3.0 * 1609.344#meters
    function_class_numeric = 1
    turn = {}
    
    turn['14'] = {2:{'min_lon1':min_lon,'max_lon1':-118.1396245,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':-118.1396245,'max_lon2':max_lon,'min_lat2':min_lat,'max_lat2':max_lat}, 1:{'min_lon1':-118.1396245,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':-118.1396245,'min_lat2':min_lat,'max_lat2':max_lat}}
    '''
    #0:N 1:S 2:E 3:W
    hwy_set = [
        ("1", 2, 2), 
        ("1", 3, 3),
        ("2", 2, 2),
        ("2", 3, 3),
        ("5", 0, 0),
        ("5", 1, 1),
        ("10",2, 2),
        ("10",3, 3),
        ("14", 2, 0)
        ("14", 1, 3)
        ("15", 0, 3),
        ("15", 1, 1),
        ("22", 2, 2),
        
        
              ]   
    '''
    hwy_set = [("22", 3, 3)]
    for hwy in hwy_set:
        road_name = hwy[0]
        direction = hwy[1]
        t_direction = hwy[2]
        hwy_secs[hwy] = lamap.process_road(road_name, function_class_numeric, direction, t_direction, min_lon, max_lon, min_lat, max_lat, section_len, turn)
    
    