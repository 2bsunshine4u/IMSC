import psycopg2
import datetime
import Utils

class Pattern(object):
    def __init__(self):
        self.init_db()
        
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
    #find (lon, lat) of (from_node, to_node) of all links on the certain highway or arterial
    
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

        
        return link_loc
    
    def locate_sensors(self, onstreet, direction, function_class_numeric):
    #locate all sensors on the road
        print "Locating Sensors"
        if function_class_numeric == 1:
            sql = "select distinct sensor_id, ST_AsText(start_lat_long) from highway_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and onstreet like '%" + onstreet + "%' and direction = '" + str(direction) +"'"
        else:
             sql = "select distinct sensor_id, ST_AsText(start_lat_long) from arterial_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and upper(onstreet) like '%" + onstreet + "%' and direction = '" + str(direction) +"'"
        self.cursor.execute(sql)
        sensors = self.cursor.fetchall()
        sensor_loc = {}
        for s in sensors:
            if s[0] not in sensor_loc:
                sensor_loc[s[0]] = Utils.extract_loc_from_geometry(s[1])
        
        return sensor_loc
    
    def map_link_sensor(self, sectionid):
    #fetch mapping information from database
        print "fetching mapping information from database"
        sql = "select link_id, sensor_id from ss_sensor_mapping where section_id = 'Section " + str(sectionid) + "'"
        self.cursor.execute(sql)
        mapping = self.cursor.fetchall()
        dict_link_sensor = {}
        for m in mapping:
            if m[0] not in dict_link_sensor:
                dict_link_sensor[m[0]] = []
            dict_link_sensor[m[0]].append(m[1])
        
        return dict_link_sensor
        
    def realtime_pattern(self, road_name, function_class_numeric, direction, sectionid, start_date):
    #get realtime_pattern of section
        link_loc = lapattern.locate_links(road_name, function_class_numeric)
        sensor_loc = lapattern.locate_sensors(road_name, direction, function_class_numeric)
        dict_link_sensor = lapattern.map_link_sensor(sectionid)
        
        start_time = start_date + " 6:00:00"
        start_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        day_spd = {}
        for d in range(0, 7):
            dt = start_dt + datetime.timedelta(days=d)
            day_spd[d] = []
            for t in range(0, 60):
                dt_end = dt+datetime.timedelta(minutes=15)
                link_spd = {}
                for link in dict_link_sensor:
                    avg_spd = []
                    for sensor in dict_link_sensor[link]:
                        sql = "Select avg(speed) from  sensor_data_highway where sensor_id= "+str(sensor)+" and speed > 1 and speed < 150 and STATUS_Ok=TRUE and timestamp >= '"+dt.strftime("%Y-%m-%d %H:%M:%S")+"' and timestamp <= '"+dt_end.strftime("%Y-%m-%d %H:%M:%S")+"'"
                        self.cursor.execute(sql)
                        s = self.cursor.fetchall()[0][0]
                        loc = sensor_loc[sensor]
                        avg_spd.append([s, loc])
                    if len(avg_spd) < len(dict_link_sensor[link]):
                        print "No available data"
                    elif len(avg_spd) == 1:
                        link_spd[link] = avg_spd[0][0]
                        print "With sensor on it, average speed on link", link, "from", dt, "to", dt_end, "is", link_spd[link]
                    else:
                        mid_lon = (link_loc[link][0][0] + link_loc[link][1][0]) / 2.0
                        mid_lat = (link_loc[link][0][1] + link_loc[link][1][1]) / 2.0
                        dist1 = Utils.map_dist(mid_lon, mid_lat, avg_spd[0][1][0], avg_spd[0][1][1])
                        dist2 = Utils.map_dist(mid_lon, mid_lat, avg_spd[1][1][0], avg_spd[1][1][1])
                        link_spd[link] = (float(avg_spd[0][0])*dist2+float(avg_spd[1][0])*dist1)/(dist1+dist2)
                        print "Without sensor on it, average speed on link", link, "from", dt, "to", dt_end, "is average of", avg_spd[0][0], "and", avg_spd[1][0], "to be", link_spd[link]
                
                total_len = 0
                weighted_spd = 0
                for link in link_spd:
                    link_len = Utils.map_dist(link_loc[link][0][0], link_loc[link][0][1], link_loc[link][1][0], link_loc[link][1][1])
                    total_len += link_len
                    weighted_spd += link_spd[link] * link_len
                
                day_spd[d].append(weighted_spd/total_len)   
                print "Average speed from", dt, "to", dt_end,"is", day_spd[d][len(day_spd[d])-1]
                
                dt = dt_end
                        
                '''
                sql = "Select * from  sensor_data_highway where sensor_id= 716427 and speed >1 and  STATUS_Ok=TRUE and timestamp >= '2015-09-25 06:00:00' and timestamp <= '2015-09-26 06:00:15';"
                self.cursor.execute(sql)
                avg_spd = self.cursor.fetchall()
                '''
                
        
    
if __name__ == '__main__':
    lapattern = Pattern()
    
    sectionid = 1
    road_name = "I-105"
    function_class_numeric = 1 #1: Highway 3:Arterial
    direction = 2    #0:N 1:S 2:E 3:W
    start_date = "2015-09-21"
    
    lapattern.realtime_pattern(road_name, function_class_numeric, direction, sectionid, start_date)
    
    
    