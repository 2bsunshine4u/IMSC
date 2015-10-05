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
            elif name_default.index(road_name) > 0:
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
    
        #print "Begin filtering links in region with right direction"
        
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
    
        #print "Begin Sorting links"
    
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
    
        #print "Begin filling the whole road and divide into sections"
    
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
        '''
        if len(path[cur_sec]) <= 3:
            print "The last section is meaningless, del it"
            del(path[cur_sec])
            cur_sec -= 1
            while len(path[cur_sec]) == 0:
                del(path[cur_sec])
                cur_sec -= 1
        '''        
        #print "Section filling finished"
        
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
            
            if road_name == '405':
                filtered_links3 = self.filter_range_bearing(link_loc, tn['min_lon3'], tn['max_lon3'], tn['min_lat3'], tn['max_lat3'], direction)
                filtered_links3 = self.sort_links(link_loc, filtered_links3, direction)
                filtered_links = filtered_links1 + filtered_links2 + filtered_links3
                 
        path = self.fill_path(link_loc, filtered_links, section_len)
        '''
        for i in path:
            print i, ':'
            for j in path[i]:
                    print link_loc[j][0][1], link_loc[j][0][0], ','
        '''
        return path
    
class Sensor(object):
    def __init__(self, cursor):
        self.cursor = cursor
    
    def find_all_sensors(self, road_name, direction, t_direction):
    #find all sensors on hwys
        sql = "select distinct sensor_id, ST_AsText(start_lat_long), onstreet from highway_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and onstreet like '%" + road_name + "%' and (direction = '" + str(direction) +"' or direction = '"+str(t_direction)+"')"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        sensors = []
        for sensor_id, loc, onstreet in results:
            if (len(onstreet) >(onstreet.index(road_name) + len(road_name))):
                if onstreet[onstreet.index(road_name)-1].isdigit() or onstreet[onstreet.index(road_name)+len(road_name)].isdigit():
                    continue
            elif onstreet.index(road_name) > 0:
                if onstreet[onstreet.index(road_name)-1].isdigit():
                    continue
                        
            t = [sensor_id, Utils.extract_loc_from_geometry(loc)]
            if t not in sensors:
                sensors.append(t)
                
        print "number of all sensors:", len(sensors)
        
        return sensors
    
    def dict_road(self, link_loc, path, sensors):
    #build the dictionary of sensors on roads
        dict_road = {}
        used_s = []
        for section in path:
            dict_road[section] = {}
            for link in path[section]:
                dict_road[section][link] = []

                lon1, lat1 = link_loc[link][0]
                lon2, lat2 = link_loc[link][1]
                for sensor in sensors:
                    lon_sen, lat_sen = sensor[1]
                    if Utils.is_in_bbox(lon1,lat1,lon2,lat2,lon_sen,lat_sen):
                        if Utils.map_dist(lon1,lat1,lon_sen,lat_sen) < 5000:
                            #print "find sensor",lat_sen,lon_sen,"on link:",link_loc[link][:2]
                            dict_road[section][link].append(sensor[0])
                            if sensor not in used_s:
                                used_s.append(sensor)
                    else:
                        if Utils.map_dist(lon1,lat1,lon_sen,lat_sen) < 75 or Utils.map_dist(lon2,lat2,lon_sen,lat_sen) < 75:
                            #print "find sensor",lat_sen,lon_sen,"near link:",link_loc[link][:2]
                            dict_road[section][link].append(sensor[0])
                            if sensor not in used_s:
                                used_s.append(sensor)     
            
        print "number of used_sensors:", len(used_s)

                    
        return dict_road
        
    
    def map_sensor_highway(self, road_name, path, direction, t_direction, link_loc):
        sensors = self.find_all_sensors(road_name, direction, t_direction)
        dict_sensors_roads = self.dict_road(link_loc, path, sensors)
 
        return dict_sensors_roads
    
if __name__ == '__main__':
    lamap = Map()
    lasensor = Sensor(lamap.cursor)

    min_lon = -119.4370 
    max_lon = -116.7240
    min_lat = 33.2980
    max_lat = 34.5830 
    section_len = 3.0 * 1609.344#meters
    function_class_numeric = 1
    turn = {}
    
    turn['14'] = {2:{'min_lon1':min_lon,'max_lon1':-118.1396245,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':-118.1396245,'max_lon2':max_lon,'min_lat2':min_lat,'max_lat2':max_lat}, 1:{'min_lon1':-118.1396245,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':-118.1396245,'min_lat2':min_lat,'max_lat2':max_lat}}
    turn['101'] = {2:{'min_lon1':min_lon,'max_lon1':-118.377508,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':-118.377508,'max_lon2':max_lon,'min_lat2':min_lat,'max_lat2':max_lat}, 0:{'min_lon1':-118.377508,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':-118.377508,'min_lat2':min_lat,'max_lat2':max_lat}}
    turn['405'] = {1:{'min_lon1':min_lon,'max_lon1':max_lon,'min_lat1':33.897262,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':max_lon,'min_lat2':33.644662,'max_lat2':33.897262,'min_lon3':min_lon,'max_lon3':max_lon,'min_lat3':min_lat,'max_lat3':33.644662}, 0:{'min_lon1':min_lon,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':33.644662,'min_lon2':min_lon,'max_lon2':max_lon,'min_lat2':33.644662,'max_lat2':33.897262,'min_lon3':min_lon,'max_lon3':max_lon,'min_lat3':33.897262,'max_lat3':max_lat}}
    
    #0:N 1:S 2:E 3:W
    hwy_set = [
        ("2", 2, 2),
        ("2", 3, 3),
        ("5", 0, 0),
        ("5", 1, 1),
        ("10",2, 2),
        ("10",3, 3),
        ("14", 2, 0),
        ("14", 1, 3),
        ("15", 0, 0),
        ("15", 1, 1),
        ("22", 2, 2),
        ("22", 3, 3),
        ("23", 0, 0),
        ("23", 1, 1),
        ("33", 0, 0),
        ("33", 1, 1),
        ("47", 0, 0),
        ("47", 1, 1),
        ("55", 0, 0),
        ("55", 1, 1),
        ("57", 0, 0),
        ("57", 1, 1),
        ("60", 2, 2), 
        ("60", 3, 3),
        ("71", 0, 0), 
        ("71", 1, 1),
        ("73", 0, 0), 
        ("73", 1, 1),
        ("90", 2, 2), 
        ("90", 3, 3),
        ("91", 2, 2), 
        ("91", 3, 3),
        ("101", 0, 3),
        ("101", 2, 1),
        ("105", 2, 2), 
        ("105", 3, 3),
        ("110", 0, 0), 
        ("110", 1, 1),
        ("118", 2, 2), 
        ("118", 3, 3),
        ("126", 2, 2), 
        ("126", 3, 3),
        ("133", 0, 0), 
        ("133", 1, 1),
        ("134", 2, 2), 
        ("134", 3, 3),
        ("170", 0, 0), 
        ("170", 1, 1),
        ("210", 2, 2), 
        ("210", 3, 3),
        ("215", 0, 0), 
        ("215", 1, 1),
        ("241", 0, 0), 
        ("241", 1, 1),
        ("405", 1, 2), 
        ("405", 0, 3),
        ("605", 0, 0), 
        ("605", 1, 1),
        ("710", 0, 0), 
        ("710", 1, 1)
              ]   
    
    for hwy in hwy_set:
        road_name = hwy[0]
        direction = hwy[1]
        t_direction = hwy[2]
        path = lamap.process_road(road_name, function_class_numeric, direction, t_direction, min_lon, max_lon, min_lat, max_lat, section_len, turn)
        mapping = lasensor.map_sensor_highway(road_name, path, direction, t_direction, lamap.link_loc[road_name])
        
        for section in mapping:
            from_postmile = int(section) * 3
            to_postmile = int(section) * 3 + 3
            for link in mapping[section]:
                if len(mapping[section][link]) == 0:
                    sql = "insert into \"SS_SENSOR_MAPPING\" (road_name,direction,from_postmile,to_postmile,link_id) values (%s,%d,%d,%d,%d)"%(road_name,direction,from_postmile,to_postmile,link)
                    lamap.cursor.execute(sql)
                    print road_name, section, link, "no sensor"
                else:
                    for sensor in mapping[section][link]:
                        sql = "insert into \"SS_SENSOR_MAPPING\" (road_name,direction,from_postmile,to_postmile,link_id,sensor_id) values (%s,%d,%d,%d,%d,%d)"%(road_name,direction,from_postmile,to_postmile,link,sensor)
                        lamap.cursor.execute(sql)
                        print road_name, section, link, sensor
        
    lamap.close_db()
    
    