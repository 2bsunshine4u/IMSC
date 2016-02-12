import cx_Oracle
import datetime
import Utils

class Pattern(object):
    def __init__(self, segment_config_table, segment_data_table, pattern_table):
        self.segment_config_table = segment_config_table
        self.segment_data_table = segment_data_table
        self.pattern_table = pattern_table
        
        self.init_db()
        self.segments = {}
        
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

    def operation_oracle(self, sql):
        self.cursor.execute(sql)

    def segment_config(self, road_name):
        config = {}
        sql = "select distinct segment_id, length_kms, road_list, direction, start_lon, start_lat, end_lon, end_lat from "+self.segment_config_table+" where road_list like '%"+road_name+"%'"
        results = self.query_oracle(sql)
        for segment_id, length, road_list, direction, start_lon, start_lat, end_lon, end_lat in results:
            if segment_id not in config:
                config[segment_id] = {}
                config[segment_id]["length"] = length
                config[segment_id]["road_list"] = road_list
                config[segment_id]["direction"] = direction
                config[segment_id]["start_lon"] = start_lon
                config[segment_id]["start_lat"] = start_lat
                config[segment_id]["end_lon"] = end_lon
                config[segment_id]["end_lat"] = end_lat

        return config

    def segment_data(self, road_name, config):
        month_dict = {
            "Jan 2014": "SYS_P4902",
            "Feb 2014": "SYS_P4904",
            "Mar 2014": "SYS_P4906",
            "Apr 2014": "SYS_P4908",
            "May 2014": "SYS_P4910",
            "June 2014": "SYS_P4912",
            "Jul 2014": "SYS_P4914",
            "Aug 2014": "SYS_P4916",
            "Sept 2014": "SYS_P4918",
            "Oct 2014": "SYS_P4920",
            "Nov 2014": "SYS_P4922",
            "Dec 2014": "SYS_P4924",
            "Jan 2015": "SYS_P4926",
            "Feb 2015": "SYS_P4928",
            "Mar 2015": "SYS_P4930",
            "Apr 2015": "SYS_P4932",
            "May 2015": "SYS_P4934",
            "June 2015": "SYS_P4936"
        }

        print "segment_data fetching on road", road_name
        segment_data = {}
        for segment_id in config:
            segment_data[segment_id] = {}
            for m in month_dict:
                segment_data[segment_id][m] = {}
                for d in range(0, 7):
                    segment_data[segment_id][m][d] = {}

        start_dt = "2015-06-25"
        end_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for segment_id in segment_data:
            print "segment_id: ",segment_id
            for month in segment_data[segment_id]:
                sql = "Select segment_id, date_time, speed from "+self.segment_data_table+" partition("+ month_dict[month] +") where segment_id = "+str(segment_id)+" and speed > 1"
                results = self.query_oracle(sql)
                
                for segment_id, dt, speed in results:
                    speed = float(speed)*1.609344

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
                    
                    dt -= datetime.timedelta(hours=8)

                    if dt.date() >= datetime.date(dt.year,3,18) and dt.date() <= datetime.date(dt.year,11,1):
                        dt += datetime.timedelta(hours=1)
                    
                    day = dt.weekday()
                    time = dt.time()
                    
                    if time not in segment_data[segment_id][month][day]:
                        segment_data[segment_id][month][day][time] = []
                    segment_data[segment_id][month][day][time].append(speed)
            
        return segment_data
                
    def inrix_pattern(self, road_name, config, segment_data):
        print "inrix pattern processing on:",road_name
        
        pattern = {}
        for segment_id in segment_data:
            pattern[segment_id] = {}
            for month in segment_data[segment_id]:
                pattern[segment_id][month] = {};
                for day in range(0, 7):
                    time = datetime.time(6,0,0)
                    pattern[segment_id][month][day] = []
                    for t in range(0, 60):
                        if time.minute == 45:
                            time_end = datetime.time(time.hour+1,0,time.second)
                        else:
                            time_end = datetime.time(time.hour,time.minute+15,time.second)

                        if time in segment_data[segment_id][month][day]:
                            sd = segment_data[segment_id][month][day][time]
                            spd = sum(sd)/len(sd)
                        else:
                            spd = 0

                        pattern[segment_id][month][day].append(spd)
                        
                        time = time_end
        
        return pattern
        
    
    def generate_all(self, roads):
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
        print "Table has been emptied!!!"
        sql = "truncate table " + self.pattern_table
        self.operation_oracle(sql)
        
        for road_name in roads:
            config = self.segment_config(road_name)
            segment_data = self.segment_data(road_name, config)
            inrix_pattern = self.inrix_pattern(road_name, config, segment_data)

            print "finish procesising, begin insert:",road_name
            for segment_id in inrix_pattern:
                for month in inrix_pattern[segment_id]:
                    for day in range(0,7):  
                        ip_d = Utils.list_to_str(inrix_pattern[segment_id][month][day])

                        print segment_id, days[day], ip_d
                        
                        sql = "insert into "+ self.pattern_table +" (road_name, direction, segment_id, length, road_list, start_lon, start_lat, end_lon, end_lat, month, weekday, inrix_pattern) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)"
                        data = (road_name, config[segment_id]["direction"], segment_id, config[segment_id]["length"], config[segment_id]["road_list"], config[segment_id]["start_lon"], config[segment_id]["start_lat"], config[segment_id]["end_lon"], config[segment_id]["end_lat"], month, days[day], ip_d)
                        self.insert_oracle(sql, data)
                        
            self.conn.commit();
                
                
if __name__ == '__main__':
    lapattern = Pattern("inrix_section_config", "inrix_traffic_history", "inrix_pattern_arterial")

    roads = ["Figueroa St", "Aviation Blvd", "La Cienega Blvd", "Martin Luther King Jr Blvd", "Grand Ave", "La Cienega Ave", "S Hoover St"]
    
    lapattern.generate_all(roads)
    
    lapattern.close_db()
                
        
    