import psycopg2
import datetime
import Utils

class Pattern(object):
    def __init__(self):
        self.init_db()
        self.nodes = {}
        self.links = {}
        self.tmc = {}
        
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
        roads = []
        
        sql = "select distinct road_name from ss_arterial_pattern"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for road_name in results:
            roads.append(road_name)
        print "These roads have been processed previously:", roads
        
        print "fetching tmc information from database"
        sql = "select tmc, road, direction, start_longitude, start_latitude, end_longitude, end_latitude, miles from tmc_identification"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        links = {}
        for tmc, road_name, direction, start_longitude, start_latitude, end_longitude, end_latitude, miles in results:
            if road_name not in roads: 
                if road_name not in links:
                    links[road_name] = {}
                if direction not in links[road_name]:
                    links[road_name][direction] = []    
                links[road_name][direction].append({'tmc': tmc, 'start': (float(start_longitude), float(start_latitude)), 'end': (float(end_longitude), float(end_latitude)), 'miles': float(miles)})
                
                if road_name not in self.tmc:
                    self.tmc[road_name] = []
                if tmc not in self.tmc[road_name]:
                    self.tmc[road_name].append(tmc)
        
        print "Sorting the links"
        for road_name in links:
            for direction in links[road_name]:
                if len(links[road_name][direction]) > 1:
                    if direction == "NORTHBOUND":
                        links[road_name][direction].sort(key=lambda x:x['start'][1], reverse=False)
                    elif direction == "SOUTHBOUND":
                        links[road_name][direction].sort(key=lambda x:x['start'][1], reverse=True)
                    elif direction == "EASTBOUND":
                        links[road_name][direction].sort(key=lambda x:x['start'][0], reverse=False)
                    elif direction == "WESTBOUND":
                        links[road_name][direction].sort(key=lambda x:x['start'][0], reverse=True)
                    else:
                        print "INVALID DIRECTION!!\n\n\n"
                        
        sorted_links = {}
        for road_name in links:
            sorted_links[road_name] = {}
            for direction in links[road_name]:
                sorted_links[road_name][direction] = []
                
                road_links = links[road_name][direction][:]
                temp = 0
                while len(road_links) > 0:
                    if len(sorted_links[road_name][direction]) == 0:
                        flag = True
                        while flag:
                            for tmc_inf in range(0, len(road_links)):
                                if road_links[tmc_inf]['end'] == road_links[temp]['start']:
                                    temp = tmc_inf
                                    break
                                flag = False
                    else:
                        flag = False
                        for tmc_inf in range(0, len(road_links)):
                            if sorted_links[road_name][direction][-1]['end'] == road_links[tmc_inf]['start']:
                                temp = tmc_inf
                                flag = True
                                break
                        if flag == False:
                            if sorted_links[road_name][direction][-1] == links[road_name][direction][-1]:
                                break
                            else:
                                dist = Utils.map_dist(sorted_links[road_name][direction][-1]['end'][0], sorted_links[road_name][direction][-1]['end'][1], road_links[temp]['start'][0], road_links[temp]['start'][1])
                                for tmc_inf in range(0, len(road_links)):
                                    dist2 = Utils.map_dist(sorted_links[road_name][direction][-1]['end'][0], sorted_links[road_name][direction][-1]['end'][1], road_links[tmc_inf]['start'][0], road_links[tmc_inf]['start'][1])
                                    if dist2 < dist:
                                        temp = tmc_inf
                                        dist = Utils.map_dist(sorted_links[road_name][direction][-1]['end'][0], sorted_links[road_name][direction][-1]['end'][1], road_links[temp]['start'][0], road_links[temp]['start'][1])
                                        
                    sorted_links[road_name][direction].append(road_links[temp])
                    del(road_links[temp]) 
                    if temp >= len(road_links):
                        temp = len(road_links) - 1
        
        return sorted_links
    
    def sectionize (self, links, section_len):
        print "Sectionizing!"
        mapping = {}
        for road_name in links:
            mapping[road_name] = {}
            for direction in links[road_name]:
                print "Road:", road_name, "direction:", direction
                mapping[road_name][direction] = {}
                
                mapping[road_name][direction][0] = []
                cur_sec = 0
                prev_loc = links[road_name][direction][0]['start']
                cur_miles = 0
                for idx in range(0, len(links[road_name][direction])):
                    cur_lon = links[road_name][direction][idx]['start'][0]
                    cur_lat = links[road_name][direction][idx]['start'][1]
                    if cur_miles > section_len:
                        cur_sec += 1
                        mapping[road_name][direction][cur_sec] = []
                        cur_miles = 0
                    if not prev_loc == (cur_lon, cur_lat):
                        while cur_miles + (Utils.map_dist(prev_loc[0], prev_loc[1], cur_lon, cur_lat) / 1609.344) > section_len:
                            cur_sec += 1
                            mapping[road_name][direction][cur_sec] = []
                            mid_lon = prev_loc[0] + (cur_lon-prev_loc[0]) * (section_len - cur_miles) /  (Utils.map_dist(cur_lon, cur_lat, prev_loc[0], prev_loc[1]) / 1609.344)
                            mid_lat = prev_loc[1] + (cur_lat-prev_loc[1]) * (section_len - cur_miles) /  (Utils.map_dist(cur_lon, cur_lat, prev_loc[0], prev_loc[1]) / 1609.344)
                            prev_loc = (mid_lon, mid_lat)
                            cur_miles = 0
                        
                        cur_miles += (Utils.map_dist(prev_loc[0], prev_loc[1], cur_lon, cur_lat) / 1609.344)
                        prev_loc = (cur_lon, cur_lat)
                    
                    
                    miles = links[road_name][direction][idx]['miles']
                    tmc = links[road_name][direction][idx]['tmc']
                    mapping[road_name][direction][cur_sec].append(tmc)
                    cur_miles += miles
                    prev_loc = links[road_name][direction][idx]['end']
                    
        print mapping
                    
        return mapping
        
       
    def road_tmc_data(self, road_name, mapping):
        print "tmc_data fetching on road", road_name
        print "With tmc", self.tmc[road_name]
        
        tmc_data = {}
        for tmc in self.tmc[road_name]:
            tmc_data[tmc] = {}
            for d in range(0, 7):
                tmc_data[tmc][d] = {}
        
        
        #start_dt = "2015-09-17 00:00:00"
        #end_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for tmc in self.tmc[road_name]:
            sql = "Select timestamp at time zone 'PST', speed from tmc_data where tmc_code = '"+tmc+"' and speed > 1 and speed < 150"
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
                if dt.date() >= datetime.date(dt.year,3,18) and dt.date() <= datetime.date(dt.year,11,1):
                    dt += datetime.timedelta(hours=1)
                day = dt.weekday()
                time = dt.time()

                if time not in tmc_data[tmc][day]:
                    tmc_data[tmc][day][time] = []
                tmc_data[tmc][day][time].append(speed)
            
        return tmc_data
                
    def realtime_pattern(self, road_name, direction, section, mapping, tmc_data):
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
                avg_spd = []
                for tmc in mapping[road_name][direction][section]:
                    if time in tmc_data[tmc][day]:
                        td = tmc_data[tmc][day][time]
                        spd = sum(td) / len(td)
                        avg_spd.append(spd)
                
                if len(avg_spd) == 0:
                    #print "No available data for section:", road_name, direction, section, "in time:", day, time
                    day_spd[day].append(0)
                else:
                    #print "Has available data for section:", road_name, direction, section, "in time:", day, time
                    day_spd[day].append(sum(avg_spd) / len(avg_spd))
                    
                time = time_end
                
        self.rp[(road_name, direction, section)] = day_spd
        
        return day_spd
    '''
    def historical_pattern(self, road_name, direction, section, mapping):
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
        '''
    
    def generate_all(self, section_len):
        days = ["'Monday'", "'Tuesday'", "'Wednesday'", "'Thursday'", "'Friday'", "'Saturday'", "'Sunday'"]
        
        links = self.map_link_sensor()
        mapping = self.sectionize(links, section_len)
        
        for road_name in mapping:
            tmc_data = self.road_tmc_data(road_name, mapping)

            for direction in mapping[road_name]:
                for section in mapping[road_name][direction]:
                    if len(mapping[road_name][direction][section]) > 0:
                        rp = self.realtime_pattern(road_name, direction, section, mapping, tmc_data)

                        print "begin insert:",road_name,direction,section
                        for d in range(0,7):  
                            rp_d = Utils.list_to_str(rp[d])

                            sql = "insert into ss_arterial_pattern (road_name, direction, from_postmile, to_postmile, day, realtime_pattern) values('%s','%s',%d,%d,%s,%s)"%(road_name, direction, section*section_len, (section+1)*section_len, days[d], rp_d)

                            self.cursor.execute(sql)
                        
            self.conn_to.commit();
                        
                
                
if __name__ == '__main__':
    section_len = 1
    
    lapattern = Pattern()
    
    lapattern.generate_all(section_len)
    
    lapattern.close_db()
                
        
    