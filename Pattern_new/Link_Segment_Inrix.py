import psycopg2
import cx_Oracle
import Utils

class Map(object):
    def __init__(self, nodes_table, links_table, min_lon, max_lon, min_lat, max_lat):
        self.nodes_table = nodes_table
        self.links_table = links_table

        self.min_lon = min_lon 
        self.max_lon = max_lon 
        self.min_lat = min_lat 
        self.max_lat = max_lat
        
        self.init_db()
        self.link_loc = {}
        self.roads = []
        self.nodes = {}
        
    def init_db(self):
        print "Connecting to database ......"
        self.conn = cx_Oracle.connect('shuai/shuai2015pass@gd.usc.edu/ADMS')        
        if self.conn:
            print "Connected."
        self.cursor = self.conn.cursor()


    def query_oracle(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()

        return results

    def insert_oracle(self, sql, data):
        self.cursor.execute(sql, data)
        
    def close_db(self):
        self.conn.commit()
        self.conn.close()
        
    def locate_node(self, node_id):
        #locate the node
        if node_id in self.nodes:
            return self.nodes[node_id]

        sql = "select node_id, t.geom.SDO_POINT.x, t.geom.SDO_POINT.y from "+self.nodes_table + " t where node_id = "+node_id
        results = self.query_oracle(sql)
        for node_id, longitude, latitude in results:
            if node_id not in self.nodes:
                self.nodes[node_id] = (longitude, latitude)
                return (longitude, latitude)

    def init_roads(self):
        #find all roads' name
        print "roads initiation"
        sql = "select distinct name_default from "+self.links_table
        results = self.query_oracle(sql)
        roads = []
        for (road_name,) in results:
            if road_name and road_name not in roads and road_name.find("5"):
                roads.append(road_name)

        for road_name in roads:
            print "initiate", road_name
            link_loc = self.locate_links(road_name, "All")

            N = 0
            S = 0
            E = 0
            W = 0
            NE = 0
            SW = 0
            NW = 0
            SE = 0
            for link in link_loc:
                heading = Utils.bearing(from_node_loc[0], from_node_loc[1] , to_node_loc[0], to_node_loc[1])
                if heading >= 270 or heading < 90:
                    N += 1
                elif heading >= 90 and heading < 270:
                    S += 1
                elif heading >= 0 and heading < 180:
                    E += 1
                elif heading >= 180 and heading < 360:
                    W += 1

                if heading >= 315 or heading < 135:
                    NE += 1
                elif heading >= 45 and heading < 225:
                    SE += 1
                elif heading >= 135 and heading < 315:
                    SW += 1
                elif heading >= 225 or heading < 45:
                    NW += 1
                    
            if max(N+S, E+W, NE+SW, NW+SE) == N+S:
                self.roads.append((road_name, 0))
                self.roads.append((road_name, 1))
            elif max(N+S, E+W, NE+SW, NW+SE) == E+W:
                self.roads.append((road_name, 2))
                self.roads.append((road_name, 3))
            elif max(N+S, E+W, NE+SW, NW+SE) == NE+SW:
                self.roads.append((road_name, 4))
                self.roads.append((road_name, 6))
            elif max(N+S, E+W, NE+SW, NW+SE) == NW+SE:
                self.roads.append((road_name, 5))
                self.roads.append((road_name, 7))

        return self.roads
        
    def locate_links(self, road_name, function_class_numeric):
    #find (lon, lat) of (from_node, to_node) of all links on the certain highway
    
        #print "Begin locating links on",road_name
        
        if road_name in self.link_loc:
            return self.link_loc[road_name]
        
        link_loc = {}
    
        print function_class_numeric
        if function_class_numeric == "All":
            sql = "select link_id, from_node_id, to_node_id, way_id, name_default from "+self.links_table+" where name_default = '" + road_name + "%'"  

        results = self.query_oracle(sql)
        
        for (link_id, from_node_id, to_node_id, way_id, name_default) in results:
            from_node_loc = self.locate_node(from_node_id)
            to_node_loc = self.locate_node(to_node_id)
            
            if from_node_loc[0] >= self.min_lon and from_node_loc[1] >= self.min_lat and to_node_loc[0] <  self.max_lon and to_node_loc[1] < self.max_lat:
                link_loc[link_id] = (from_node_loc, to_node_loc, from_node_id, to_node_id, way_id)
        
        print "Link locating finished, there are " + str(len(link_loc)) + " links on " + road_name
        
        self.link_loc[road_name] = link_loc
        
        return link_loc
    
    def filter_bearing(self, link_loc, direction):
    #direction: 0-North 1-South 2-East 3-West 4-NorthEast 5-SouthEast 6-SouthWest 7-NorthWest
    
        #print "Begin filtering links in region with right direction"
        
        filtered_links = []
        for link_id in link_loc:
                heading = Utils.bearing(link_loc[link_id][0][0], link_loc[link_id][0][1] , link_loc[link_id][1][0], link_loc[link_id][1][1])
                if direction == 0 and (heading >= 270 or heading < 90):
                    filtered_links.append(link_id)
                elif direction == 1 and (heading >= 90 and heading < 270):
                    filtered_links.append(link_id)
                elif direction == 2 and (heading >= 0 and heading < 180):
                    filtered_links.append(link_id)
                elif direction == 3 and (heading >= 180 and heading < 360):
                    filtered_links.append(link_id)
                elif direction == 4 and (heading >= 315 or heading < 135):
                    filtered_links.append(link_id)
                elif direction == 5 and (heading >= 45 and heading < 225):
                    filtered_links.append(link_id)
                elif direction == 6 and (heading >= 135 and heading < 315):
                    filtered_links.append(link_id)
                elif direction == 7 and (heading >= 225 or heading < 45):
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
            
        elif direction == 4:
            filtered_links.sort(key=lambda x:link_loc[x][0][0]+link_loc[x][0][1],reverse=False)
            
        elif direction == 5:
            filtered_links.sort(key=lambda x:link_loc[x][0][0]-link_loc[x][0][1],reverse=False)
            
        elif direction == 6:
            filtered_links.sort(key=lambda x:link_loc[x][0][0]+link_loc[x][0][1],reverse=True)
            
        elif direction == 7:
            filtered_links.sort(key=lambda x:link_loc[x][0][0]-link_loc[x][0][1],reverse=True)

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
    
    def process_road(self, road_name, function_class_number, direction, section_len):
        print "Begin processing road:", road_name,"direction",direction
        link_loc = self.locate_links(road_name, function_class_numeric)
        
        filtered_links = self.filter_bearing(link_loc, direction)
        filtered_links = self.sort_links(link_loc, filtered_links, direction)
                 
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
        set_dir = []
        if direction == 4:
            set_dir.append('0')
            set_dir.append('2')
        elif direction == 5:
            set_dir.append('1')
            set_dir.append('2')
        elif direction == 6:
            set_dir.append('1')
            set_dir.append('3')
        elif direction == 7:
            set_dir.append('0')
            set_dir.append('3')
        else:
            set_dir.append(str(direction))
            
        if t_direction == 4:
            set_dir.append('0')
            set_dir.append('2')
        elif t_direction == 5:
            set_dir.append('1')
            set_dir.append('2')
        elif t_direction == 6:
            set_dir.append('1')
            set_dir.append('3')
        elif t_direction == 7:
            set_dir.append('0')
            set_dir.append('3')
        else:
            set_dir.append(str(t_direction))
        
        set_dir = tuple(set(set_dir))
        if len(set_dir) > 1:
            sql = "select distinct sensor_id, ST_AsText(start_lat_long), onstreet from highway_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and onstreet like '%" + road_name + "%' and direction in " + str(set_dir)
        else:
            sql = "select distinct sensor_id, ST_AsText(start_lat_long), onstreet from highway_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and onstreet like '%" + road_name + "%' and direction = '" + set_dir[0] + "'"
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
            if t[0] not in map(lambda x:x[0], sensors):
                sensors.append(t)
                
        print "number of all sensors:", len(sensors)
        
        return sensors
    
    def dict_road(self, link_loc, path, sensors, direction):
    #build the dictionary of sensors on roads
        dict_road = {}
        used_s = []
        for section in path:
            dict_road[section] = {}
            for link in path[section]:
                dist1 = 99999
                dist2 = 99999
                dict_road[section][link] = []

                lon1, lat1 = link_loc[link][0]
                lon2, lat2 = link_loc[link][1]
                for sensor in sensors:
                    lon_sen, lat_sen = sensor[1]                
                    if (Utils.is_in_bbox(lon1,lat1,lon2,lat2,lon_sen,lat_sen) and Utils.point2line(lon_sen,lat_sen,lon1,lat1,lon2,lat2) < 200):
                        #print "find sensor",lat_sen,lon_sen,"on link:",link_loc[link][:2]
                        dict_road[section][link].append((sensor[0], "TRUE"))
                        if sensor not in used_s:
                            used_s.append(sensor)
                        continue
                    elif len(dict_road[section][link]) == 0:
                        if direction == 0 or direction == 1:
                            d = Utils.map_dist(lon_sen, lat_sen, lon1, lat1)
                            if d < dist1 and (lat_sen-lat1)*(lat1-lat2) > 0:
                                sen1 = sensor
                                dist1 = d

                            d = Utils.map_dist(lon_sen, lat_sen, lon2, lat2)
                            if  d < dist2 and (lat_sen-lat2)*(lat2-lat1) > 0:
                                sen2 = sensor
                                dist2 = d                        
                        else:
                            d = Utils.map_dist(lon_sen, lat_sen, lon1, lat1)
                            if d < dist1 and (lon_sen-lon1)*(lon1-lon2) >0:
                                sen1 = sensor
                                dist1 = d

                            d = Utils.map_dist(lon_sen, lat_sen, lon2, lat2)
                            if  d < dist2 and (lon_sen-lon2)*(lon2-lon1) >0:
                                sen2 = sensor
                                dist2 = d

                if len(dict_road[section][link]) == 0:
                    if dist1 < 450:
                        dict_road[section][link].append((sen1[0], "FALSE"))
                        if sen1 not in used_s:
                            used_s.append(sen1)

                    if dist2 < 450:
                        dict_road[section][link].append((sen2[0], "FALSE"))
                        if sen2 not in used_s:
                            used_s.append(sen2)  
            
        print "number of used_sensors:", len(used_s)
        
        print "Unused sensors:"
        for sensor in sensors:
            if sensor not in used_s:
                if sensor[1][0] >= -119.4370 and sensor[1][0] <= -116.7240 and sensor[1][1] >= 33.2980 and sensor[1][1] <= 34.5830:
                    print sensor[0], sensor[1][::-1]

                    
        return dict_road
        
    
    def map_sensor_highway(self, road_name, path, direction, t_direction, link_loc):
        sensors = self.find_all_sensors(road_name, direction, t_direction)
        dict_sensors_roads = self.dict_road(link_loc, path, sensors, direction)
 
        return dict_sensors_roads, sensors
    
if __name__ == '__main__':
    min_lon = -119.4370 
    max_lon = -116.7240
    min_lat = 33.2980
    max_lat = 34.5830 
    section_len = 3.0 * 1609.344#meters
    function_class_numeric = "All"
    
    #0:N 1:S 2:E 3:W 4:NE 5:SE 6:SW 7:NW

    lamap = Map("nodes", "links", min_lon, max_lon, min_lat, max_lat)
    lasensor = Sensor(lamap.cursor)

    roads_set = lamap.init_roads()
    print "roads:", lamap.roads
    
    print "Table has been emptied!!"
    sql = "truncate inrix_mapping"
    lamap.oracle_cursor.execute(sql)
    lamap.conn_oracle.commit()
    
    for road in roads_set:
        road_name = road[0]
        direction = road[1]
        show_dir = direction
        path = lamap.process_road(road_name, function_class_numeric, direction, section_len)
        '''mapping, sensors = lasensor.map_sensor_highway(road_name, path, direction, t_direction, lamap.link_loc[road_name])
        
        for section in mapping:
            from_postmile = int(section) * 3
            to_postmile = int(section) * 3 + 3
            for link in mapping[section]:
                start_nodeid = lamap.link_loc[road_name][link][2]
                end_nodeid = lamap.link_loc[road_name][link][3]
                start_loc = "POINT("+str(lamap.link_loc[road_name][link][0][0])+" "+str(lamap.link_loc[road_name][link][0][1])+")"
                end_loc = "POINT("+str(lamap.link_loc[road_name][link][1][0])+" "+str(lamap.link_loc[road_name][link][1][1])+")"
                length = Utils.map_dist(lamap.link_loc[road_name][link][0][0],lamap.link_loc[road_name][link][0][1],lamap.link_loc[road_name][link][1][0],lamap.link_loc[road_name][link][1][1])
                wayid = str(lamap.link_loc[road_name][link][4])
                
                if road_name == '33' and section > 40:
                    continue
                if len(mapping[section][link]) == 0:
                    sql = "insert into ss_highway_mapping (road_name,direction,from_postmile,to_postmile,link_id,start_nodeid, start_loc,end_nodeid,end_loc,length, wayid) values (%s,%d,%d,%d,%d,%d,ST_GeomFromText('%s', 4326),%d,ST_GeomFromText('%s',4326),%f,%s)"%(road_name,show_dir,from_postmile,to_postmile,link,start_nodeid,start_loc,end_nodeid,end_loc,length, wayid)
                    lamap.cursor.execute(sql)
                    #print road_name, show_dir, section, link, "no sensor"
                else:
                    #print road_name, show_dir, section, link, mapping[section][link]
                    for sensor, on_edge_flag in mapping[section][link]:
                        senloc = filter(lambda x:x[0]==sensor, sensors)[0][1]
                        sensor_loc = "POINT("+str(senloc[0])+" "+str(senloc[1])+")"
                        sql = "insert into ss_highway_mapping (road_name,direction,from_postmile,to_postmile,link_id,start_nodeid,start_loc,end_nodeid,end_loc,length,wayid,sensor_id,sensor_loc,on_edge_flag) values (%s,%d,%d,%d,%d,%d,ST_GeomFromText('%s',4326),%d,ST_GeomFromText('%s',4326),%f,%s,%d,ST_GeomFromText('%s',4326),%s)"%(road_name,show_dir,from_postmile,to_postmile,link,start_nodeid,start_loc,end_nodeid,end_loc,length,wayid,sensor,sensor_loc,on_edge_flag)
                        lamap.cursor.execute(sql)'''
        
    lamap.close_db()
    
    