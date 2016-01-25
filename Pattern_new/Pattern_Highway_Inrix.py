import psycopg2
import cx_Oracle
import datetime
import Utils

class Pattern(object):
    def __init__(self, mapping_table, pattern_table, nodes_table, links_table, historical_table):
        self.mapping_table = mapping_table
        self.pattern_table = pattern_table
        self.nodes_table = nodes_table
        self.links_table = links_table
        self.historical_table = historical_table
        
        self.init_db()
        self.nodes = {}
        self.links = {}
        self.segments = {}
        
        self.ip = {}
        
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
        print sql
        results = self.cursor.fetchall()

        return results

    def operation_oracle(self, sql):
        self.cursor.execute(sql)
    
    def map_link_segment(self):
    #fetch mapping information from database
        print "fetching mapping information from database"
        sql = "select road_name, direction, from_postmile, link_id, segment_id, on_edge_flag from " + self.mapping_table
        results = self.query_oracle(sql)
        mapping = {}
        for road_name, direction, from_postmile,link,segment,on_edge_flag in results:
            if road_name not in mapping:
                mapping[road_name] = {}
            if direction not in mapping[road_name]:
                mapping[road_name][direction] = {}
            section = int(from_postmile / 3)
            if section not in mapping[road_name][direction]:
                mapping[road_name][direction][section] = {}
            if link not in mapping[road_name][direction][section]:
                mapping[road_name][direction][section][link] = []
            if segment:
                mapping[road_name][direction][section][link].append((segment, on_edge_flag))
                if road_name not in self.segments:
                    self.segments[road_name] = []
                if segment not in self.segments[road_name]:
                    self.segments[road_name].append(segment)
        
        return mapping
    
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
    
    def pre_links(self, road_name, mapping):
        print "preprocessing links on road:", road_name
        self.links[road_name] = {}
        for direction in mapping[road_name]:
            for section in mapping[road_name][direction]:
                for link in mapping[road_name][direction][section]:
                    sql = "select from_node_id, to_node_id from "+self.links_table+" where link_id = " + str(link)
                    results = self.query_oracle(sql)
                    start_node, end_node = results[0]
                    start_loc = self.locate_node(start_node)
                    end_loc = self.locate_node(end_node)
                    length = Utils.map_dist(start_loc[0], start_loc[1], end_loc[0],end_loc[1])/1609.344
                    if length <= 0:
                        print "Wrong Length Data!"
                    self.links[road_name][link] = [start_node, end_node, length]
        
        return self.links  

    def road_segment_data(self, road_name):
        ss = "("
        for segment_id in self.segments[road_name]:
            ss += str(segment_id) + ','
        ss = ss[:-1] + ')'
            
        print "preprocessing segment_loc on road:", road_name
        segment_loc = {}
        sql = "select distinct segment_id, start_lon, start_lat, end_lon, end_lat from inrix_section_config t where segment_id in " + ss
        results = self.query_oracle(sql)
        for segment_id, start_lon, start_lat, end_lon, end_lat in results:
            segment_loc[segment_id] = ((start_lon+end_lon)/2.0, (start_lat+end_lat)/2.0)

        print "segment_data fetching on road", road_name
        segment_data = {}
        for segment_id in self.segments[road_name]:
            segment_data[segment_id] = {}
            for d in range(0, 7):
                segment_data[segment_id][d] = {}

        start_dt = "2015-06-25"
        end_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        sql = "Select segment_id, date_time, speed from inrix_traffic_history where date_time >= to_date('"+start_dt+"', 'YYYY-MM-DD') and segment_id in "+ss+" and speed > 1 and speed < 150 "
        results = self.query_oracle(sql)
        
        print "segment_data fetching finished, begin preprocessing"
        for segment_id, dt, speed in results:
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
            '''
            if dt.date() >= datetime.date(dt.year,3,18) and dt.date() <= datetime.date(dt.year,11,1):
                dt += datetime.timedelta(hours=1)
            '''
            day = dt.weekday()
            time = dt.time()
            
            if time not in segment_data[segment_id][day]:
                segment_data[segment_id][day][time] = []
            segment_data[segment_id][day][time].append(speed)
            
        return segment_loc, segment_data
                
    def inrix_pattern(self, road_name, direction, section, mapping, segment_loc, segment_data):
    #get inrix_pattern of section
    
        print "inrix pattern processing on:",road_name, direction, section
        
        if (road_name, direction, section) in self.ip:
            return self.ip[(road_name, direction, section)]
        
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
                    lon1, lat1 = self.locate_node(self.links[road_name][link][0])
                    lon2, lat2 = self.locate_node(self.links[road_name][link][1])
                    avg_spd = []
                    for segment, on_edge_flag in mapping[road_name][direction][section][link]: 
                        loc = segment_loc[segment]
                        if time in segment_data[segment][day]:
                            sd = segment_data[segent][day][time]
                            spd = sum(sd)/len(sd)
                            
                            avg_spd.append([spd, loc])
                    if len(avg_spd) == 0:
                        pass#print "No available data for link:", link
                    elif on_edge_flag == 'f' and len(avg_spd) == 2:
                        mid_lon = (lon1 + lon1) / 2.0
                        mid_lat = (lat1 + lat2) / 2.0
                        if direction == 0 or direction == 1:
                            dist1 = abs(mid_lat-avg_spd[0][1][1])
                            dist2 = abs(mid_lat-avg_spd[1][1][1])
                        else:
                            dist1 = abs(mid_lon-avg_spd[0][1][0])
                            dist2 = abs(mid_lon-avg_spd[1][1][0])
                        link_spd[link] = (float(avg_spd[0][0])*dist2+float(avg_spd[1][0])*dist1)/(dist1+dist2)
                    else:
                        t = map(lambda x:float(x[0]), avg_spd)
                        link_spd[link] = sum(t)/len(t)
                
                total = 0
                for link in link_spd:
                    total += link_spd[link]
                    
                if total > 0:
                    day_spd[day].append(float(total)/len(link_spd))   
                else:
                    day_spd[day].append(0)
                
                time = time_end
                
        self.ip[(road_name, direction, section)] = day_spd
        
        return day_spd        
    
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
        
        mapping = self.map_link_segment()
        
        '''
        print "Table has been emptied!!!"
        sql = "truncate " + self.pattern_table
        self.cursor.execute(sql)
        self.conn_to.commit()
        '''
        
        for road in mapping:
            self.pre_links(road, mapping)
            segment_loc, segment_data = self.road_segment_data(road)
            
            for direction in mapping[road]:
                print "Begin road", road,"direction:",direction
                for section in mapping[road][direction]:
                    ip = self.inrix_pattern(road, direction, section, mapping, segment_loc, segment_data)
                    
                    for d in range(0,7):
                        if ip[d].count(0) == 60:
                            print "Empty section pattern:",section
                            if section-1 in mapping[road][direction] and section+1 in mapping[road][direction]:
                                prev = self.inrix_pattern(road, direction, section-1, mapping, segment_loc, segment_data)
                                nex = self.inrix_pattern(road, direction, section+1, mapping, segment_loc, segment_data)
                                if prev[d].count(0) == 60:
                                    ip[d] = nex[d]
                                elif nex[d].count(0) == 60:
                                    ip[d] = prev[d]
                                else:
                                    for i in range(0,60):
                                        ip[d][i] = (prev[d][i]+nex[d][i])/2.0
                            elif section-1 in mapping[road][direction]:
                                prev = self.inrix_pattern(road, direction, section-1, mapping, segment_loc, segment_data)
                                ip[d] = prev[d]
                            elif section+1 in mapping[road][direction]:
                                nex = self.inrix_pattern(road, direction, section+1, mapping, segment_loc, segment_data)
                                ip[d] = nex[d]
                    
                    print "finish procesising, begin insert:",road,direction,section
                    for d in range(0,7):  
                        ip_d = Utils.list_to_oracle_array(ip[d])
                        #point = Utils.list_to_str(self.cal_similarity(ip[d], rp[d]))
                        
                        sql = "update "+ self.pattern_table +" set inrix_pattern = " + ip_d + ", similarity = 0 where road_name = '"+str(road)+"' and direction = "+str(direction)+" and from_postmile = "+str(section*3)+" and to_postmile = "+str(section*3+3)+" and weekday = "+days[d]
                        print sql
                        self.operation_oracle(sql)
                        
            self.conn.commit();
                        
                
                
if __name__ == '__main__':
    lapattern = Pattern( "segment_mapping_highway", "pattern_highway", "nodes", "links", "")
    
    lapattern.generate_all()
    
    lapattern.close_db()
                
        
    