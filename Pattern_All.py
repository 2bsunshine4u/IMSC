import psycopg2
import datetime
import Utils

class Pattern(object):
    def __init__(self):
        self.init_db()
        
        self.sensor_data = {}
        
        self.rp = {}
        
    def init_db(self):
        print "Connecting to database ......"
        self.conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
        if self.conn_to:
            print "Connected."
        self.cursor = self.conn_to.cursor()
        
        
    def close_db(self):
        self.conn_to.commit()
        self.conn_to.close()
        
        self.his_conn_to.close()
    
    def map_link_sensor(self):
    #fetch mapping information from database
        print "fetching mapping information from database"
        sql = "select road_name, direction, from_postmile, link_id, sensor_id from \"SS_SENSOR_MAPPING\" "
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        mapping = {}
        for road_name, direction, from_postmile,link,sensor in results:
            if int(road_name) >= 100: 
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
                    if sensor not in self.sensor_data:
                        self.sensor_data[sensor] = {}
                        for d in range(0,7):
                            self.sensor_data[sensor][d] = {}

        return mapping
        
        
    def pre_sensor_data(self):
        start_dt = "2015-09-17 06:00:00"
        end_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for sensor in self.sensor_data:
            print "realtime_data preprocessing of sensor", sensor
            sql = "Select timestamp at time zone 'PST', speed from  sensor_data_highway where sensor_id= "+str(sensor)+" and speed > 1 and speed < 150 and STATUS_Ok=TRUE and timestamp at time zone 'PST' >= '"+start_dt+"' and timestamp at time zone 'PST' <= '"+end_dt+"'"
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for dt, speed in results:
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

                if time not in self.sensor_data[sensor][day]:
                    self.sensor_data[sensor][day][time] = []
                self.sensor_data[sensor][day][time].append(speed)
                
    def realtime_pattern(self, road_name, direction, section, mapping):
    #get realtime_pattern of section
    
        print "real time pattern processing on section", section
        
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
                    avg_spd = []
                    for sensor in mapping[road_name][direction][section][link]: 
                        if time in self.sensor_data[sensor][day]:
                            sd = self.sensor_data[sensor][day][time]
                            spd = sum(sd)/len(sd)
                            
                            avg_spd.append(spd)
                    if len(avg_spd) == 0:
                        pass#print "No available data for link:", link
                    else:
                        link_spd[link] = sum(avg_spd)/len(avg_spd)
                
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
    
    def node_inf(self, mapping):
        print "preprocessing nodes of links"
        link_nodes = {}
        
        for road in mapping:
            link_nodes[road] = {}
            for direction in mapping[road]:
                for section in mapping[road][direction]:
                    for link in mapping[road][direction][section]:
                        sql = "select from_node_id, to_node_id, length from links where link_id = " + str(link)
                        self.cursor.execute(sql)
                        start_node, end_node, length = self.cursor.fetchall()[0]
                        if length <= 0:
                            print "Wrong Length Data!"
                        link_nodes[road][link] = [start_node, end_node, length]
        
        return link_nodes
        
    def historical_pattern(self, road_name, direction, section, mapping, link_nodes):
        days = ["'Monday'", "'Tuesday'", "'Wednesday'", "'Thursday'", "'Friday'", "'Saturday'", "'Sunday'"]
        
        his_day_spd = {} 
        v1_day_spd = {}
        
        sql = "select day, historical_pattern,v1_pattern from \"SS_SECTION_PATTERN_ALL\" where road_name = '"+road_name+"' and direction ="+str(direction)+" and from_postmile = "+str(section*3)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for day,historical_pattern,v1_pattern in results:
            d = days.index("'"+day+"'")
            his_day_spd[d] = historical_pattern
            v1_day_spd[d] = v1_pattern
            for i in range(0,60):
                if his_day_spd[d][i]:
                    his_day_spd[d][i] = float(his_day_spd[d][i])
                else:
                    his_day_spd[d][i] = 0
                if v1_day_spd[d][i]:
                    v1_day_spd[d][i] = float(v1_day_spd[d][i])
                else:
                    v1_day_spd[d][i] = 0
            
        return his_day_spd, v1_day_spd
        '''
        print "historical_data preprocessing"
        
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        
        his_day_link_weight = {}
        v1_day_link_weight = {}
        for d in range(0, 7):
            day = days[d]
            his_day_link_weight[d] = {}
            v1_day_link_weight[d] = {}
            for link in mapping[road_name][direction][section]:
                start_node, end_node = link_nodes[road_name][link][:2]
                sql = "select weights from edge_weight_metric_"+day+" WHERE start_originalid = " + str(start_node) + " AND end_originalid = "+str(end_node)
                self.his_cursor.execute(sql)
                results = self.his_cursor.fetchall()
                if len(results) > 0:
                    weights = results[0][0]
                    if len(weights) == 60:
                        his_day_link_weight[d][link] = weights
                
                sql = "select weights from current_data_metric_"+day+" WHERE start_originalid = " + str(start_node) + " AND end_originalid = "+str(end_node)
                self.his_cursor.execute(sql)
                results = self.his_cursor.fetchall()
                if len(results) > 0:
                    weights = results[0][0]
                    if len(weights) == 60:
                        v1_day_link_weight[d][link] = weights
            
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
                    total_len += link_nodes[road_name][link][2]
                    total_weight += link_weight[link]
                
                if total_weight >0:
                    spd = (float(total_len)*1609.344/float(total_weight)) * 3600.0
                    his_day_spd[day].append(spd)
                else:
                    his_day_spd[day].append(0)
                    print "No available historical weight in timeslot",t
                    
        v1_day_spd = {}
        for day in range(0, 7):
            v1_day_spd[day] = []
            for t in range(0, 60):
                link_weight = {}
                for link in mapping[road_name][direction][section]:
                    if link in v1_day_link_weight[day]:
                        weight = v1_day_link_weight[day][link][t]
                        if weight > 0:
                            link_weight[link] = weight
                    else:
                        pass#print "No available v1 weight on link", link
                
                total_len = 0
                total_weight = 0

                for link in link_weight:
                    total_len += link_nodes[road_name][link][2]
                    total_weight += link_weight[link]
                
                if total_weight >0:
                    spd = (float(total_len)*1609.344/float(total_weight)) * 3600.0
                    v1_day_spd[day].append(spd)
                else:
                    v1_day_spd[day].append(0)
                    print "No available v1 weight in timeslot",t
        
        return his_day_spd, v1_day_spd
        '''
    
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
        self.pre_sensor_data()
        link_nodes = self.node_inf(mapping)
        
        print "Connecting to database ......"
        self.his_conn_to = psycopg2.connect(host='graph-3.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='tallygo', user='ds', password='ds2015')
        if self.his_conn_to:
            print "Connected."
        self.his_cursor = self.his_conn_to.cursor()
        
        for road in mapping:
            for direction in mapping[road]:
                print "Begin road", road,"direction:",direction
                for section in mapping[road][direction]:
                    print "Begin section", section
                    rp = self.realtime_pattern(road, direction, section, mapping)                          
                    hp, v1p = self.historical_pattern(road, direction, section, mapping, link_nodes)
                    
                    for d in range(0,7):
                        if rp[d].count(0) == 60:
                            print "Empty section pattern:",section
                            if section-1 in mapping[road][direction] and section+1 in mapping[road][direction]:
                                prev = self.realtime_pattern(road, direction, section-1, mapping)
                                nex = self.realtime_pattern(road, direction, section+1, mapping)
                                if prev[d].count(0) == 60:
                                    rp[d] = nex[d]
                                elif nex[d].count(0) == 60:
                                    rp[d] = prev[d]
                                else:
                                    for i in range(0,60):
                                        rp[d][i] = (prev[d][i]+nex[d][i])/2.0
                            elif section-1 in mapping[road][direction]:
                                prev = self.realtime_pattern(road, direction, section-1, mapping)
                                rp[d] = prev[d]
                            elif section+1 in mapping[road][direction]:
                                nex = self.realtime_pattern(road, direction, section+1, mapping)
                                rp[d] = nex[d]
                    
                    point_rt_his = self.cal_similarity(rp[0], hp[0])
                    point_rt_v1 = self.cal_similarity(rp[0], v1p[0])
                    
                    print "finish procesising, begin insert:",road,direction,section
                    for d in range(0,7):  
                        rp_d = Utils.list_to_str(rp[d])
                        hp_d = Utils.list_to_str(hp[d])
                        v1p_d = Utils.list_to_str(v1p[d])
                        point_rt_his = Utils.list_to_str(self.cal_similarity(rp[d], hp[d]))
                        point_rt_v1 = Utils.list_to_str(self.cal_similarity(rp[d], v1p[d]))
                        
                        sql = "insert into \"SS_SECTION_PATTERN\"(road_name, direction, from_postmile, to_postmile, day, realtime_pattern, historical_pattern, v1_pattern, similarity_rt_his, similarity_rt_v1) values(%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"%(road, direction, section*3, section*3+3, days[d], rp_d, hp_d, v1p_d, point_rt_his, point_rt_v1)
                        
                        self.cursor.execute(sql)
                        
                
                
if __name__ == '__main__':
    lapattern = Pattern()
    
    lapattern.generate_all()
    
    lapattern.close_db()
                
        
    