import HighwayEdges
import Utils
import psycopg2

class Sensor(object):
    def __init__(self, cursor, fileout):
        self.cursor = cursor
        self.fileout = fileout
        
    def find_first_last_sensor(self, onstreet, start_street, end_street, direction):
    #find the first and the last sensor then enlarge the range by 0.5 mile for each end to include all possible sensors
        sql = "select postmile from highway_congestion_config where onstreet like '%"+onstreet+"%' and fromstreet like '%"+start_street+"%' and direction = '" + str(direction) + "' order by last_seen_at desc limit 1"
        self.cursor.execute(sql)
        start_postmile = self.cursor.fetchall()[0][0]
        
        sql = "select postmile from highway_congestion_config where onstreet like '%"+onstreet+"%' and fromstreet like '%"+end_street+"%' and direction = '" + str(direction) + "' order by last_seen_at desc limit 1"
        self.cursor.execute(sql)
        end_postmile = self.cursor.fetchall()[0][0]

        if start_postmile > end_postmile:
            start_postmile += 0.5
            end_postmile -= 0.5
        else:
            start_postmile -= 0.5
            end_postmile += 0.5

        print "For " + onstreet + " Start_postmile is " + str(start_postmile)+ " and end_postmile is " + str(end_postmile)
        
        return start_postmile, end_postmile
    
    def find_all_sensors(self, onstreet, start_postmile, end_postmile, direction):
    #find all sensors between these two postmiles
        sql = "select distinct sensor_id, ST_AsText(start_lat_long) from highway_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and onstreet like '%" + onstreet + "%' and postmile >= " + str(min(start_postmile, end_postmile)) + " and postmile <= " + str(max(start_postmile, end_postmile)) + " and direction = '" + str(direction) +"'"
        self.cursor.execute(sql)
        sensors = self.cursor.fetchall()
        new_sensors = []
        for s in sensors:
            t = [s[0], Utils.extract_loc_from_geometry(s[1])]
            if t not in new_sensors:
                new_sensors.append(t)
        
        return new_sensors
    
    def find_arterial_sensors(self, onstreet, direction):
    #find all sensors on arterials
        sql = "select distinct sensor_id, ST_AsText(start_lat_long) from arterial_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and upper(onstreet) like '%" + onstreet + "%' and direction = '" + str(direction) +"'"
        self.cursor.execute(sql)
        sensors = self.cursor.fetchall()
        new_sensors = []
        for s in sensors:
            t = [s[0], Utils.extract_loc_from_geometry(s[1])]
            if t not in new_sensors:
                new_sensors.append(t)
        
        return new_sensors
    
    def dict_road(self, link_loc, path, sensors, direction):
    #build the dictionary of sensors on roads
        dict_road = {}
        used_s = []
        for link in path:
            dict_road[link] = []
            dist1 = 99999
            dist2 = 99999
            
            lon1, lat1 = link_loc[link][0]
            lon2, lat2 = link_loc[link][1]
            for sensor in sensors:
                lon_sen, lat_sen = sensor[1]
                if Utils.is_in_bbox(lon1,lat1,lon2,lat2,lon_sen,lat_sen, direction) and Utils.map_dist(lon1,lat1,lon_sen,lat_sen) < 5000 and len(dict_road[link]) == 0:
                    dict_road[link].append(sensor)
                    if sensor not in used_s:
                        used_s.append(sensor)
                    continue 
                
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
            
            if len(dict_road[link]) == 0:
                if dist1 < 8000:
                    dict_road[link].append(sen1)
                    if sen1 not in used_s:
                        used_s.append(sen1)

                if dist2 < 8000:
                    dict_road[link].append(sen2)
                    if sen2 not in used_s:
                        used_s.append(sen2)
            
        for s in used_s:
            self.fileout.write(str(s[1][1])+','+ str(s[1][0])+'\n')
            
        for s in used_s:
            self.fileout.write(str(s[0])+'\n')
            
                
        print "There are ", len(used_s), " sensors"
                    
        return dict_road
        
    
    def map_sensor_highway(self, lamap, road_name, function_class_numeric, direction, start_loc, end_loc, start_street, end_street):
        link_loc = lamap.locate_links(road_name, function_class_numeric)
        bearing_links = lamap.filter_bearing(link_loc, direction)
        start_link, end_link = lamap.find_start_end_link(link_loc, bearing_links, start_loc[0], start_loc[1], end_loc[0], end_loc[1])
        path = lamap.fill_path(link_loc, bearing_links, start_link, end_link, direction)
        
        for p in path:
            self.fileout.write(str(link_loc[p][0][1])+','+str(link_loc[p][0][0])+'\n')        
        self.fileout.write("\n\n")
        for p in path:
            self.fileout.write(str(p)+'\n')        
        self.fileout.write("\n\n")
        
        print "There are " + str(len(path)) + " links"
        start_postmile, end_postmile = self.find_first_last_sensor(road_name, start_street, end_street, direction)
        sensors = self.find_all_sensors(road_name, start_postmile, end_postmile, direction)
        dict_sensors_roads = self.dict_road(link_loc, path, sensors, direction)
        '''
        for link in dict_sensors_roads:
            for s in dict_sensors_roads[link]:
                self.fileout.write(str(link_loc[link][0][1])+','+str(link_loc[link][0][0])+'   '+str(link_loc[link][1][1])+','+str(link_loc[link][1][0])+';  '+str(s[1][1])+','+str(s[1][0])+'\n')
        '''
        return dict_sensors_roads
        
    def map_sensor_arterial(self, lamap, road_name, function_class_numeric, direction, start_loc, end_loc):
        link_loc = lamap.locate_links(road_name, function_class_numeric)
        bearing_links = lamap.filter_bearing(link_loc, direction)
        start_link, end_link = lamap.find_start_end_link(link_loc, bearing_links, start_loc[0], start_loc[1], end_loc[0], end_loc[1])
        path = lamap.fill_path(link_loc, bearing_links, start_link, end_link, direction)
        
        for p in path:
            self.fileout.write(str(link_loc[p][0][1])+','+str(link_loc[p][0][0])+'\n')
        self.fileout.write("\n\n")
        for p in path:
            self.fileout.write(str(p)+'\n')
        self.fileout.write("\n\n")
        
        print "There are " + str(len(path)) + " links"
        sensors = self.find_arterial_sensors(road_name, direction)
        dict_sensors_roads = self.dict_road(link_loc, path, sensors, direction)
        '''
        for link in dict_sensors_roads:
            for s in dict_sensors_roads[link]:
                self.fileout.write(str(link_loc[link][0][1])+','+str(link_loc[link][0][0])+'   '+str(link_loc[link][1][1])+','+str(link_loc[link][1][0])+';  '+str(s[1][1])+','+str(s[1][0])+'\n')
        '''   
        return dict_sensors_roads
    
if __name__ == '__main__':
    lamap = HighwayEdges.Map()
    fileout = open('path.txt', 'w')
    lasensor = Sensor(lamap.cursor, fileout)
    '''
    #First Highway Section
    road_name = "I-105"
    function_class_numeric = 1
    direction = 2    #0:N 1:S 2:E 3:W
    start_loc = (-118.321807, 33.925263) #Crenshaw Blvd
    end_loc = (-118.25863, 33.92744) #Central Avenue
    start_street = "CRENSHAW"
    end_street = "CENTRAL"
    
    dict_sen = lasensor.map_sensor_highway(lamap, road_name, function_class_numeric, direction, start_loc, end_loc, start_street, end_street)
    
    for link in dict_sen:
            for sen in dict_sen[link]:
                if len(dict_sen[link]) == 1:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_one, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 1'")
                    lasensor.cursor.execute(sql)
                else:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_many, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 1'")
                    lasensor.cursor.execute(sql)

    
    #Second Highway Section
    road_name = "I-405"
    function_class_numeric = 1
    direction = 0    #0:N 1:S 2:E 3:W
    start_loc = (-118.474060, 34.188839) #Victory Blvd
    end_loc = (-118.472978, 34.220024) #Roscoe Blvd
    start_street = "VICTORY"
    end_street = "ROSCOE"
    
    dict_sen = lasensor.map_sensor_highway(lamap, road_name, function_class_numeric, direction, start_loc, end_loc, start_street, end_street)
    
    for link in dict_sen:
            for sen in dict_sen[link]:
                if len(dict_sen[link]) == 1:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_one, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 2'")
                    lasensor.cursor.execute(sql)
                else:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_many, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 2'")
                    lasensor.cursor.execute(sql)

    
    #Third Highway Section
    road_name = "I-710"
    function_class_numeric = 1
    direction = 0    #0:N 1:S 2:E 3:W
    start_loc = (-118.177954, 33.932878) #Imperial Hwy
    end_loc = (-118.168525, 33.965665) #Florence Ave
    start_street = "IMPERIAL"
    end_street = "FLORENCE"
    
    dict_sen = lasensor.map_sensor_highway(lamap, road_name, function_class_numeric, direction, start_loc, end_loc, start_street, end_street) 
    
    for link in dict_sen:
            for sen in dict_sen[link]:
                if len(dict_sen[link]) == 1:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_one, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 3'")
                    lasensor.cursor.execute(sql)
                else:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_many, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 3'")
                    lasensor.cursor.execute(sql)


    
    #First arterial Section
    road_name = "NORMANDIE"
    function_class_numeric = 3
    direction = 1    #0:N 1:S 2:E 3:W
    start_loc = (-118.300218, 34.032798) #W Adams Blvd
    end_loc = (-118.300300, 34.010890) #Martin Luther King Jr Blvd
    
    dict_sen = lasensor.map_sensor_arterial(lamap, road_name, function_class_numeric, direction, start_loc, end_loc) 
    
    for link in dict_sen:
            for sen in dict_sen[link]:
                if len(dict_sen[link]) == 1:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_one, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 4'")
                    lasensor.cursor.execute(sql)
                else:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_many, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 4'")
                    lasensor.cursor.execute(sql)
    
    
    #Second arterial Section
    road_name = "BEVERLY"
    function_class_numeric = 3
    direction = 3    #0:N 1:S 2:E 3:W
    start_loc = (-118.338534, 34.076152) #N Highland Ave
    end_loc = (-118.361478, 34.076148) #Fairfax Ave
    
    dict_sen = lasensor.map_sensor_arterial(lamap, road_name, function_class_numeric, direction, start_loc, end_loc)
    
    for link in dict_sen:
            for sen in dict_sen[link]:
                if len(dict_sen[link]) == 1:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_one, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 5'")
                    lasensor.cursor.execute(sql)
                else:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_many, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 5'")
                    lasensor.cursor.execute(sql)

    
    #Third arterial Section
    road_name = "GRAND"
    function_class_numeric = 3
    direction = 1   #0:N 1:S 2:E 3:W
    start_loc = (-118.260674, 34.043035) #W Olympic Blvd
    end_loc = (-118.266278, 34.035738) #Venice Blvd
    
    dict_sen = lasensor.map_sensor_arterial(lamap, road_name, function_class_numeric, direction, start_loc, end_loc) 
    
    for link in dict_sen:
            for sen in dict_sen[link]:
                if len(dict_sen[link]) == 1:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_one, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 6'")
                    lasensor.cursor.execute(sql)
                else:
                    sql = "insert into SS_SENSOR_MAPPING(link_id, sensor_id, one_to_many, section_id) values(%d,%d,%s,%s)"%(link, sen[0], "'Y'", "'Section 6'")
                    lasensor.cursor.execute(sql)
   
    '''
    fileout.close()
    lamap.close_db()


    
        