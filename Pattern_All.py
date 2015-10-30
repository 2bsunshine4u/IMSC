import psycopg2
import datetime
import Utils

class Pattern(object):
    def __init__(self):
        self.init_db()
        self.nodes = {}
        self.links = {}
        self.sensors = {}
        
        self.rp = {}
        
    def init_db(self):
        print "Connecting to database ......"
        self.conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
        if self.conn_to:
            print "Connected."
            #self.conn_to.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn_to.cursor()
        
        
    def close_db(self):
        self.conn_to.commit()
        self.conn_to.close()
    
    def map_link_sensor(self):
    #fetch mapping information from database
        sql = "select distinct road_name from \"SS_SECTION_PATTERN_ALL\""
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        roads = []
        for road_name in results:
            roads.append(road_name[0])
        print "These roads have been processed previously:", roads
        
        print "fetching mapping information from database"
        sql = "select road_name, direction, from_postmile, link_id, sensor_id from \"SS_SENSOR_MAPPING_ALL\" "
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        mapping = {}
        for road_name, direction, from_postmile,link,sensor in results:
            if road_name not in roads: 
                if road_name not in mapping:
                    mapping[road_name] = {}
                if direction not in mapping[road_name]:
                    mapping[road_name][direction] = {}
                section = int(from_postmile / 3)
                if section not in mapping[road_name][direction]:
                    mapping[road_name][direction][section] = {}
                if link not in mapping[road_name][direction][section]:
                    mapping[road_name][direction][section][link] = []
                if sensor:
                    mapping[road_name][direction][section][link].append(sensor)
                    if road_name not in self.sensors:
                        self.sensors[road_name] = []
                    if sensor not in self.sensors[road_name]:
                        self.sensors[road_name].append(sensor)
        
        return mapping
    
    def pre_nodes(self, mapping):
        print "preprocessing nodes"
        sql = "select node_id, ST_AsText(geom) from nodes"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for node_id, pos in results:
            if node_id not in self.nodes:
                self.nodes[node_id] = Utils.extract_loc_from_geometry(pos)
    
    def pre_links(self, road_name, mapping):
        print "preprocessing links on road:", road_name
        self.links[road_name] = {}
        for direction in mapping[road_name]:
            for section in mapping[road_name][direction]:
                for link in mapping[road_name][direction][section]:
                    sql = "select from_node_id, to_node_id, length from links where link_id = " + str(link)
                    self.cursor.execute(sql)
                    start_node, end_node, length = self.cursor.fetchall()[0]
                    if length <= 0:
                        print "Wrong Length Data!"
                    self.links[road_name][link] = [start_node, end_node, length]
        
        return self.links          
       
    def road_sensor_data(self, road_name, mapping):
        ss = "("
        for sensor_id in self.sensors[road_name]:
            ss += str(sensor_id) + ','
        ss = ss[:-1] + ')'
            
        print "preprocessing sensor_loc on road:", road_name
        sensor_loc = {}
        sql = "select distinct sensor_id, ST_AsText(start_lat_long) from highway_congestion_config where last_seen_at >= '2015-01-01' and last_seen_at < '2016-01-01' and sensor_id in " + ss
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for sensor_id, loc in results:
            sensor_loc[sensor_id] = Utils.extract_loc_from_geometry(loc)
        
        
        print "sensor_data fetching on road", road_name
        sensor_data = {}
        for sensor_id in self.sensors[road_name]:
            sensor_data[sensor_id] = {}
            for d in range(0, 7):
                sensor_data[sensor_id][d] = {}
        
        start_dt = "2015-09-17 00:00:00"
        end_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        sql = "Select sensor_id, timestamp at time zone 'PST', speed from sensor_data_highway where sensor_id in "+ss+" and speed > 1 and speed < 150 and STATUS_Ok=TRUE and timestamp at time zone 'PST' >= '"+start_dt+"' and timestamp at time zone 'PST' <= '"+end_dt+"'"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        print "sensor_data fetching finished, begin preprocessing"
        for sensor_id, dt, speed in results:
            dt = dt.replace(second = 0)
            if dt.minute >=0 and dt.minute < 15:
                dt = dt.replace(minute = 0)
            elif dt.minute >=15 and dt.minute < 30:
                dt = dt.replace(minute = 15)
            elif dt.minute >=30 and dt.minute < 45:
                dt = dt.replace(minute = 30)
            elif dt.minute >=45 and dt.minute < 60:
                dt = dt.replace(minute = 45)

            str_dt = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.datetime.strptime(str_dt, "%Y-%m-%d %H:%M:%S")
            if dt.date() >= datetime.date(dt.year,3,18) and dt.date() <= datetime.date(dt.year,11,10):
                dt += datetime.timedelta(hours=1)
            day = dt.weekday()
            time = dt.time()

            
            if time not in sensor_data[sensor_id][day]:
                sensor_data[sensor_id][day][time] = []
            sensor_data[sensor_id][day][time].append(speed)
            
        return sensor_loc, sensor_data
                
    def realtime_pattern(self, road_name, direction, section, mapping, sensor_loc, sensor_data):
    #get realtime_pattern of section
    
        print "real time pattern processing on:",road_name, direction, section
        
        if (road_name, direction, section) in self.rp:
            return self.rp[(road_name, direction, section)]
        
        day_spd = {}
        for day in range(0, 7):
            time = datetime.time(6,0,0)
            day_spd[day] = []
            for t in range(0, 60):
                if time.minute == 45:
                    time_end = datetime.time(time.hour+1,0,time.second)
                else:
                    time_end = datetime.time(time.hour,time.minute+15,time.second)
                link_spd = {}
                for link in mapping[road_name][direction][section]:
                    lon1,lat1 = self.nodes[self.links[road_name][link][0]]
                    lon2, lat2 = self.nodes[self.links[road_name][link][1]]
                    avg_spd = []
                    for sensor in mapping[road_name][direction][section][link]: 
                        loc = sensor_loc[sensor]
                        if time in sensor_data[sensor][day]:
                            sd = sensor_data[sensor][day][time]
                            spd = sum(sd)/len(sd)
                            
                            avg_spd.append([spd, loc])
                    if len(avg_spd) == 0:
                        pass#print "No available data for link:", link
                    elif len(avg_spd) != 2 or Utils.is_in_bbox(avg_spd[0][1][0],avg_spd[0][1][1],lon1,lat1,lon2,lat2):
                        t = map(lambda x:float(x[0]), avg_spd)
                        link_spd[link] = sum(t)/len(t)
                    elif len(avg_spd) == 2:
                        mid_lon = (lon1 + lon1) / 2.0
                        mid_lat = (lat1 + lat2) / 2.0
                        if direction == 0 or direction == 1:
                            dist1 = abs(mid_lat-avg_spd[0][1][1])
                            dist2 = abs(mid_lat-avg_spd[1][1][1])
                        else:
                            dist1 = abs(mid_lon-avg_spd[0][1][0])
                            dist2 = abs(mid_lon-avg_spd[1][1][0])
                        link_spd[link] = (float(avg_spd[0][0])*dist2+float(avg_spd[1][0])*dist1)/(dist1+dist2)
                            
                
                total = 0
                for link in link_spd:
                    total += link_spd[link]
                    
                if total > 0:
                    day_spd[day].append(float(total)/len(link_spd))   
                else:
                    day_spd[day].append(0)
                
                time = time_end
                
        self.rp[(road_name, direction, section)] = day_spd
        
        return day_spd
        
    def historical_pattern(self, road_name, direction, section, mapping):
        '''
        days = ["'Monday'", "'Tuesday'", "'Wednesday'", "'Thursday'", "'Friday'", "'Saturday'", "'Sunday'"]
        
        his_day_spd = {} 
        
        
        sql = "select day, historical_pattern from \"SS_SECTION_PATTERN\" where road_name = '"+road_name+"' and direction ="+str(direction)+" and from_postmile = "+str(section*3)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for day,historical_pattern in results:
            d = days.index("'"+day+"'")
            his_day_spd[d] = historical_pattern
            for i in range(0,60):
                if his_day_spd[d][i]:
                    his_day_spd[d][i] = float(his_day_spd[d][i])
                else:
                    his_day_spd[d][i] = 0
            
        return his_day_spd
        '''
        
        days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        
        print "historical_data preprocessing"
        
        his_day_link_weight = {}

        for d in range(0, 7):
            day = days[d]
            his_day_link_weight[d] = {}
            for link in mapping[road_name][direction][section]:
                start_node, end_node = self.links[road_name][link][:2]
                sql = "select weights_" + day + " from edge_weight_metric WHERE start_originalid = " + str(start_node) + " AND end_originalid = "+str(end_node)
                self.his_cursor.execute(sql)
                results = self.his_cursor.fetchall()
                if len(results) > 0:
                    weights = results[0][0]
                    if len(weights) == 60:
                        his_day_link_weight[d][link] = weights
            
        his_day_spd = {}
        for day in range(0, 7):
            his_day_spd[day] = []
            for t in range(0, 60):
                link_weight = {}
                for link in mapping[road_name][direction][section]:
                    if link in his_day_link_weight[day]:
                        weight = his_day_link_weight[day][link][t]
                        if weight > 0:
                            link_weight[link] = weight
                    else:
                        pass#print "No available historical weight on link", link    
                
                total_len = 0
                total_weight = 0

                for link in link_weight:
                    total_len += self.links[road_name][link][2]
                    total_weight += link_weight[link]
                
                if total_weight >0:
                    spd = (float(total_len)*1609.344/float(total_weight)) * 3600.0
                    his_day_spd[day].append(spd)
                else:
                    his_day_spd[day].append(0)
                    print "No available historical weight in timeslot",t
        
        return his_day_spd
        
    
    def cal_similarity(self, x, y):
        p = {}
        point = {}
        p[0] = []
        for i in range(0, 60):
            if x[i] == 0 or y[i] == 0:
                p[0].append(0)
            else:
                avg = (x[i] + y[i]) / 2.0
                dist = abs(x[i] - y[i]) / avg
                p[0].append(1.0 - dist)
        
        p[1] = p[0][:12]
        p[2] = p[0][12:36]
        p[3] = p[0][36:48]
        p[4] = p[0][48:]
        for i in p:
            while p[i].count(0) > 0:
                p[i].remove(0)
            if len(p[i]) > 0:
                point[i] = float(sum(p[i]))/len(p[i]) * 10.0
            else:
                point[i] = 0
            
        return point
        
    
    def generate_all(self):
        days = ["'Monday'", "'Tuesday'", "'Wednesday'", "'Thursday'", "'Friday'", "'Saturday'", "'Sunday'"]
        
        mapping = self.map_link_sensor()
        self.pre_nodes(mapping)
        
        '''
        print "Table has been emptied!!!"
        sql = "truncate \"SS_SECTION_PATTERN_ALL\""
        self.cursor.execute(sql)
        self.conn_to.commit()
        '''
        
        for road in mapping:
            self.pre_links(road, mapping)
            sensor_loc, sensor_data = self.road_sensor_data(road, mapping)
            
            print "Connecting to historical database ......"
            self.his_conn_to = psycopg2.connect(host='graph-3.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='tallygo', user='ds', password='ds2015')
            if self.his_conn_to:
                print "Connected."
            self.his_cursor = self.his_conn_to.cursor()
            
            for direction in mapping[road]:
                print "Begin road", road,"direction:",direction
                for section in mapping[road][direction]:
                    rp = self.realtime_pattern(road, direction, section, mapping, sensor_loc, sensor_data)
                    hp = self.historical_pattern(road, direction, section, mapping)
                    
                    for d in range(0,7):
                        if rp[d].count(0) == 60:
                            print "Empty section pattern:",section
                            if section-1 in mapping[road][direction] and section+1 in mapping[road][direction]:
                                prev = self.realtime_pattern(road, direction, section-1, mapping, sensor_loc, sensor_data)
                                nex = self.realtime_pattern(road, direction, section+1, mapping, sensor_loc, sensor_data)
                                if prev[d].count(0) == 60:
                                    rp[d] = nex[d]
                                elif nex[d].count(0) == 60:
                                    rp[d] = prev[d]
                                else:
                                    for i in range(0,60):
                                        rp[d][i] = (prev[d][i]+nex[d][i])/2.0
                            elif section-1 in mapping[road][direction]:
                                prev = self.realtime_pattern(road, direction, section-1, mapping, sensor_loc, sensor_data)
                                rp[d] = prev[d]
                            elif section+1 in mapping[road][direction]:
                                nex = self.realtime_pattern(road, direction, section+1, mapping, sensor_loc, sensor_data)
                                rp[d] = nex[d]
                    
                    print "finish procesising, begin insert:",road,direction,section
                    for d in range(0,7):  
                        rp_d = Utils.list_to_str(rp[d])
                        hp_d = Utils.list_to_str(hp[d])
                        point = Utils.list_to_str(self.cal_similarity(rp[d], hp[d]))
                        
                        sql = "insert into \"SS_SECTION_PATTERN_ALL\"(road_name, direction, from_postmile, to_postmile, day, realtime_pattern, historical_pattern, similarity) values(%s,%d,%d,%d,%s,%s,%s,%s)"%(road, direction, section*3, section*3+3, days[d], rp_d, hp_d, point)
                        
                        self.cursor.execute(sql)
                        
            self.conn_to.commit();
            
            self.his_conn_to.close()
                        
                
                
if __name__ == '__main__':
    lapattern = Pattern()
    
    lapattern.generate_all()
    
    lapattern.close_db()
                
        
    