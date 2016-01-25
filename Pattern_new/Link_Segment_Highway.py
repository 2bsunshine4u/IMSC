import psycopg2
import cx_Oracle
import Utils

class Map(object):
    def __init__(self, nodes_table, links_table):
        self.nodes_table = nodes_table
        self.links_table = links_table
        
        self.init_db()
        self.link_loc = {}
        self.nodes = {}
        
    def init_db(self):
        print "Connecting to database ......"
        self.conn = cx_Oracle.connect('shuai/shuai2015pass@gd.usc.edu/ADMS')        
        if self.conn:
            print "Connected."
        self.cursor = self.conn.cursor()
        
    def close_db(self):
        self.conn.commit()
        self.conn.close()

    def query_oracle(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()

        return results

    def insert_oracle(self, sql, data):
        self.cursor.execute(sql, data)
        
    def locate_node(self, node_id):
        #locate the node
        if node_id in self.nodes:
            return self.nodes[node_id]

        sql = "select node_id, t.geom.SDO_POINT.x, t.geom.SDO_POINT.y from "+self.nodes_table + " t where node_id = "+str(node_id)
        results = self.query_oracle(sql)
        for node_id, longitude, latitude in results:
            if node_id not in self.nodes:
                self.nodes[node_id] = (longitude, latitude)
                return (longitude, latitude)
        
    def locate_links(self, road_name, function_class_numeric):
    #find (lon, lat) of (from_node, to_node) of all links on the certain highway
    
        #print "Begin locating links on",road_name
        
        if road_name in self.link_loc:
            return self.link_loc[road_name]
        
        link_loc = {}
    
        if function_class_numeric == 1:
            sql = "select link_id, from_node_id, to_node_id, way_id, name_default from "+self.links_table+" where function_class_numeric=1 and ramp <> 't' and name_default like '%" + road_name + "%'"  
        results = self.query_oracle(sql)
        
        false_name = []
        true_name = []
        for (link_id, from_node_id, to_node_id, way_id, name_default) in results:
            #exclude road_names with ';' and onramp
            if (len (name_default) >( name_default.index(road_name) + len(road_name))):
                if name_default[name_default.index(road_name)-1].isdigit() or name_default[name_default.index(road_name)+len(road_name)].isdigit() or name_default.find(';') >= 0 or name_default.find('Onramp')>= 0 or name_default.find('Ramp')>= 0:
                    if name_default not in false_name:
                        false_name.append(name_default)
                    continue
            elif name_default.index(road_name) > 0:
                if name_default[name_default.index(road_name)-1].isdigit() or name_default.find(';') >= 0 or name_default.find('Onramp')>= 0 or name_default.find('Ramp')>= 0:
                    if name_default not in false_name:
                        false_name.append(name_default)
                    continue
                    
            if name_default not in true_name:
                true_name.append(name_default)
                    
            from_node_loc = self.locate_node(from_node_id)
            to_node_loc = self.locate_node(to_node_id)
            
            link_loc[link_id] = (from_node_loc, to_node_loc, from_node_id, to_node_id, way_id)
        
        print "Wrong name:", false_name
        print "Right name:", true_name
        
        print "Link locating finished, there are " + str(len(link_loc)) + " links on " + road_name
        
        self.link_loc[road_name] = link_loc
        
        return link_loc
    
    def filter_range_bearing(self, link_loc, min_lon, max_lon, min_lat, max_lat, direction):
    #direction: 0-North 1-South 2-East 3-West 4-NorthEast 5-SouthEast 6-SouthWest 7-NorthWest
    
        #print "Begin filtering links in region with right direction"
        
        filtered_links = []
        for link_id in link_loc:
            if link_loc[link_id][0][0] >= min_lon and link_loc[link_id][0][1] >= min_lat and link_loc[link_id][0][0] <  max_lon and link_loc[link_id][0][1] < max_lat:
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
    
    def process_road(self, road_name, function_class_number, direction, t_direction, min_lon, max_lon, min_lat, max_lat, section_len,turn):
        print "Begin processing road:", road_name,"direction",direction
        link_loc = self.locate_links(road_name,        function_class_numeric)
        
        if road_name not in turn:
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
            
            if road_name == '405' or road_name == '5':
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
    
class Segment(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def query_oracle(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()

        return results

    def insert_oracle(self, sql, data):
        self.cursor.execute(sql, data)
    
    def find_all_segments(self, road_name, direction):
    #find all segments on hwys
        if direction == 0:
            d = "N"
        elif direction == 1:
            d = "S"
        elif direction == 2:
            d = "E"
        elif direction == 3:
            d = "W"
        sql = "select segment_id, length_kms, start_lon, start_lat, end_lon, end_lat, road_list from inrix_section_config where road_list like '%"+road_name+" "+d+"'"
        results = self.query_oracle(sql)
        segments = {}
        for segment_id, length, start_lon, start_lat, end_lon, end_lat, onstreet in results:
            if onstreet.index(road_name) > 0:
                if onstreet[onstreet.index(road_name)-1].isdigit():
                    continue
                        
            if segment_id not in segments:
                segments[segment_id] = [(start_lon, start_lat), (end_lon, end_lat), length, onstreet]
                
        print "number of all segments:", len(segments)
        
        return segments
    
    def dict_road(self, link_loc, path, segments, direction):
    #build the dictionary of segments on roads
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
                for segment_id in segments:
                    lon_s1, lat_s1 = segments[segment_id][0]
                    lon_s2, lat_s2 = segments[segment_id][1]
                    if Utils.line2line(lon_s1,lat_s1, lon_s2, lat_s2, lon1, lat1, lon2, lat2) < 100:
                        #print "find segment",lat_sen,lon_sen,"on link:",link_loc[link][:2]
                        dict_road[section][link].append((segment_id, "t"))
                        if segment_id not in used_s:
                            used_s.append(segment_id)
                        continue

                    elif len(dict_road[section][link]) == 0:
                        if direction == 0 or direction == 1:
                            d = Utils.map_dist(lon_s1, lat_s1, lon1, lat1)
                            if d < dist1 and (lat_s1-lat1)*(lat1-lat2) > 0:
                                seg1 = segment_id
                                dist1 = d

                            d = Utils.map_dist(lon_s1, lat_s1, lon2, lat2)
                            if  d < dist2 and (lat_s1-lat2)*(lat2-lat1) > 0:
                                seg2 = segment_id
                                dist2 = d                        
                        else:
                            d = Utils.map_dist(lon_s1, lat_s1, lon1, lat1)
                            if d < dist1 and (lon_s1-lon1)*(lon1-lon2) >0:
                                seg1 = segment_id
                                dist1 = d

                            d = Utils.map_dist(lon_s1, lat_s1, lon2, lat2)
                            if  d < dist2 and (lon_s1-lon2)*(lon2-lon1) >0:
                                seg2 = segment_id
                                dist2 = d

                if len(dict_road[section][link]) == 0:
                    if dist1 < 450:
                        dict_road[section][link].append((seg1, "f"))
                        if seg1 not in used_s:
                            used_s.append(seg1)

                    if dist2 < 450:
                        dict_road[section][link].append((seg2, "f"))
                        if seg2 not in used_s:
                            used_s.append(seg2)  
            
        print "number of used_segments:", len(used_s)
        
        print "Unused segments:"
        for segment_id in segments:
            if segment_id not in used_s:
                if segments[segment_id][0][0] >= -119.4370 and segments[segment_id][0][0] <= -116.7240 and segments[segment_id][0][1] >= 33.2980 and segments[segment_id][0][1] <= 34.5830:
                    print segment_id, segments[segment_id][0][::-1]

                    
        return dict_road
        
    
    def map_segment_highway(self, road_name, path, direction, t_direction, show_direction, link_loc):
        segments = self.find_all_segments(road_name, show_direction)
        dict_segments_roads = self.dict_road(link_loc, path, segments, direction)
 
        return dict_segments_roads, segments
    
if __name__ == '__main__':
    lamap = Map("nodes", "links")
    lasegment = Segment(lamap.cursor)

    min_lon = -119.4370 
    max_lon = -116.7240
    min_lat = 33.2980
    max_lat = 34.5830 
    section_len = 3.0 * 1609.344#meters
    function_class_numeric = 1
    turn = {}
    
    turn['14'] = {4:{'min_lon1':min_lon,'max_lon1':-118.1396245,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':-118.1396245,'max_lon2':max_lon,'min_lat2':min_lat,'max_lat2':max_lat}, 1:{'min_lon1':-118.1396245,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':-118.1396245,'min_lat2':min_lat,'max_lat2':max_lat}}
    turn['101'] = {2:{'min_lon1':min_lon,'max_lon1':-118.377508,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':-118.377508,'max_lon2':max_lon,'min_lat2':min_lat,'max_lat2':max_lat}, 0:{'min_lon1':-118.377508,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':-118.377508,'min_lat2':min_lat,'max_lat2':max_lat}}
    turn['405'] = {1:{'min_lon1':min_lon,'max_lon1':max_lon,'min_lat1':33.897262,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':max_lon,'min_lat2':33.644662,'max_lat2':33.897262,'min_lon3':min_lon,'max_lon3':max_lon,'min_lat3':min_lat,'max_lat3':33.644662}, 0:{'min_lon1':min_lon,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':33.644662,'min_lon2':min_lon,'max_lon2':max_lon,'min_lat2':33.644662,'max_lat2':33.897262,'min_lon3':min_lon,'max_lon3':max_lon,'min_lat3':33.897262,'max_lat3':max_lat}}
    turn['5'] = {1: {'min_lon1':min_lon,'max_lon1':max_lon,'min_lat1':34.021955,'max_lat1':max_lat,'min_lon2':min_lon,'max_lon2':max_lon,'min_lat2':34.017499,'max_lat2':34.021955,'min_lon3':min_lon,'max_lon3':max_lon,'min_lat3':min_lat,'max_lat3':34.017499}, 0:{'min_lon1':min_lon,'max_lon1':max_lon,'min_lat1':min_lat,'max_lat1':34.017499,'min_lon2':min_lon,'max_lon2':max_lon,'min_lat2':34.017499,'max_lat2':34.021955,'min_lon3':min_lon,'max_lon3':max_lon,'min_lat3':34.021955,'max_lat3':max_lat}}
    
    #0:N 1:S 2:E 3:W 4:NE 5:SE 6:SW 7:NW
    hwy_set = [
        ("2", 4, 4, 0),
        ("2", 6, 6, 1),
        ("5", 0, 3),
        ("5", 1, 2),
        ("10",2, 2),
        ("10",3, 3),
        ("14", 4, 0, 0),
        ("14", 1, 6, 1),
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
        ("71", 5, 5, 1), 
        ("71", 7, 7, 0),
        ("73", 5, 5, 1), 
        ("73", 7, 7, 0),
        ("90", 2, 2), 
        ("90", 3, 3),
        ("91", 2, 2), 
        ("91", 3, 3),
        ("101", 0, 3),
        ("101", 2, 1, 1),
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
        ("210", 5, 5, 2), 
        ("210", 7, 7, 3),
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
    
    print "Table has been emptied!!"
    sql = "truncate table segment_mapping_highway"
    lamap.cursor.execute(sql)
    sql = "truncate table pattern_highway"
    lamap.cursor.execute(sql)
    lamap.conn.commit()
    
    for hwy in hwy_set:
        road_name = hwy[0]
        direction = hwy[1]
        t_direction = hwy[2]
        if len(hwy) == 4:
            show_dir = hwy[3]
        else:
            show_dir = direction
        path = lamap.process_road(road_name, function_class_numeric, direction, t_direction, min_lon, max_lon, min_lat, max_lat, section_len, turn)
        mapping, segments = lasegment.map_segment_highway(road_name, path, direction, t_direction, show_dir, lamap.link_loc[road_name])
        
        
        for section in mapping:
            from_postmile = int(section) * 3
            to_postmile = int(section) * 3 + 3
            sql = "insert into pattern_highway (road_name,direction,from_postmile,to_postmile,weekday) values (:1,:2,:3,:4,:5)"
            days = ["'Monday'", "'Tuesday'", "'Wednesday'", "'Thursday'", "'Friday'", "'Saturday'", "'Sunday'"]
            data = []
            for d in days:
                data = (road_name,show_dir,from_postmile,to_postmile,d)
                lamap.insert_oracle(sql, data)

            for link in mapping[section]:
                start_nodeid = lamap.link_loc[road_name][link][2]
                end_nodeid = lamap.link_loc[road_name][link][3]
                start_loc = (lamap.link_loc[road_name][link][0][0], lamap.link_loc[road_name][link][0][1])
                end_loc = (lamap.link_loc[road_name][link][1][0], lamap.link_loc[road_name][link][1][1])
                length = Utils.map_dist(lamap.link_loc[road_name][link][0][0],lamap.link_loc[road_name][link][0][1],lamap.link_loc[road_name][link][1][0],lamap.link_loc[road_name][link][1][1]) / 1000.0
                wayid = str(lamap.link_loc[road_name][link][4])
                
                if road_name == '33' and section > 40:
                    continue
                if len(mapping[section][link]) == 0:
                    sql = "insert into segment_mapping_highway (road_name,direction,from_postmile,to_postmile,link_id,start_nodeid, start_loc,end_nodeid,end_loc,length, wayid) values (:1,:2,:3,:4,:5,:6,SDO_GEOMETRY(2001,NULL,SDO_POINT_TYPE(:7, :8, NULL),NULL,NULL),:9,SDO_GEOMETRY(2001,NULL,SDO_POINT_TYPE(:10, :11, NULL),NULL,NULL),:12,:13)"
                    data = (road_name,show_dir,from_postmile,to_postmile,link,start_nodeid,start_loc[0],start_loc[1],end_nodeid,end_loc[0],end_loc[1],length, wayid)
                    lamap.insert_oracle(sql,data)
                else:
                    #print road_name, show_dir, section, link, mapping[section][link]
                    for segment, on_edge_flag in mapping[section][link]:
                        seg_start = segments[segment][0]
                        seg_end = segments[segment][1]
                        seg_len = segments[segment][2]
                        seg_onstreet = segments[segment][3]
                        sql = "insert into segment_mapping_highway (road_name,direction,from_postmile,to_postmile,link_id,start_nodeid,start_loc,end_nodeid,end_loc,length,wayid,segment_id,segment_start,segment_end, segment_len, segment_onstreet, on_edge_flag) values (:1,:2,:3,:4,:5,:6,SDO_GEOMETRY(2001,NULL,SDO_POINT_TYPE(:7, :8, NULL),NULL,NULL),:9,SDO_GEOMETRY(2001,NULL,SDO_POINT_TYPE(:10, :11, NULL),NULL,NULL),:12,:13,:14,SDO_GEOMETRY(2001,NULL,SDO_POINT_TYPE(:15, :16, NULL),NULL,NULL),SDO_GEOMETRY(2001,NULL,SDO_POINT_TYPE(:17, :18, NULL),NULL,NULL), :19,:20,:21)"
                        data = (road_name,show_dir,from_postmile,to_postmile,link,start_nodeid,start_loc[0],start_loc[1],end_nodeid,end_loc[0],end_loc[1],length,wayid,segment,seg_start[0],seg_start[1],seg_end[0],seg_end[1],seg_len,seg_onstreet,on_edge_flag)
                        lamap.insert_oracle(sql,data)
        lamap.conn.commit()
    
        
    lamap.close_db()
    
    