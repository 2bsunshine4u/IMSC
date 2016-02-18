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

    def init_rnsets(self, interval):
        sql = "select count(*) from "+self.segment_config_table
        results = self.query_oracle(sql)
        max_rn = results[0][0]

        sql = "select max(rn) from "+self.pattern_table
        results = self.query_oracle(sql)
        cur_rn = results[0][0]
        if not cur_rn:
            cur_rn = 0

        print "max rownum: ", max_rn, "current rownum:", cur_rn, "interval: ", interval

        rn_sets = []

        for i in range(cur_rn + 1, max_rn+1, interval):
            if i + interval - 1 <= max_rn:
                rn_sets.append((i, i+interval))
            else:
                rn_sets.append((i, max_rn+1))

        return rn_sets

    def segment_config(self, min_rn, max_rn):
        config = {}
        sql = "select segment_id, length_kms, road_name, road_list, direction, start_lon, start_lat, end_lon, end_lat, rn from (select distinct s.*, rownum as rn from "+self.segment_config_table+" s) where rn >= "+str(min_rn)+" and rn < "+str(max_rn)
        results = self.query_oracle(sql)
        for segment_id, length, road_name, road_list, direction, start_lon, start_lat, end_lon, end_lat, rn in results:
            sql = "select count(*) from "+self.pattern_table+" where segment_id = "+str(segment_id)
            count = self.query_oracle(sql)[0][0]
            if count == 0 and segment_id not in config:
                config[segment_id] = {}
                config[segment_id]["length"] = length
                config[segment_id]["road_name"] = road_name
                config[segment_id]["road_list"] = road_list
                config[segment_id]["direction"] = direction
                config[segment_id]["start_lon"] = start_lon
                config[segment_id]["start_lat"] = start_lat
                config[segment_id]["end_lon"] = end_lon
                config[segment_id]["end_lat"] = end_lat
                config[segment_id]["rn"] = rn
            else:
                print "segment_id: ", segment_id, "is already in pattern_table!!"

        return config

    def segment_data(self, min_rn, max_rn, config):
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

        print "segment_data fetching on rownum from ", min_rn, "to", max_rn

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
            print "segment_id: ",segment_id, "rownum:", config[segment_id]["rn"]
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

                    //if dt.date() >= datetime.date(dt.year,3,18) and dt.date() <= datetime.date(dt.year,11,1):
                    //    dt += datetime.timedelta(hours=1)
                    
                    day = dt.weekday()
                    time = dt.time()
                    
                    if time not in segment_data[segment_id][month][day]:
                        segment_data[segment_id][month][day][time] = []
                    segment_data[segment_id][month][day][time].append(speed)
            
        return segment_data
                
    def inrix_pattern(self, min_rn, max_rn, config, segment_data):
        print "inrix pattern processing on rownum from", min_rn, "to", max_rn
        
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
        
    
    def generate_all(self):
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
        rn_sets = self.init_rnsets(1);
        
        for min_rn, max_rn in rn_sets:
            config = self.segment_config(min_rn, max_rn)
            segment_data = self.segment_data(min_rn, max_rn, config)
            inrix_pattern = self.inrix_pattern(min_rn, max_rn, config, segment_data)

            print "finish procesising, begin insert rownum:", min_rn, "to", max_rn
            for segment_id in inrix_pattern:
                for month in inrix_pattern[segment_id]:
                    for day in range(0,7):  
                        ip_d = Utils.list_to_str(inrix_pattern[segment_id][month][day])

                        print segment_id, days[day], ip_d
                        
                        sql = "insert into "+ self.pattern_table +" (road_name, direction, segment_id, length, road_list, start_lon, start_lat, end_lon, end_lat, month, weekday, inrix_pattern, rn) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)"
                        data = (config[segment_id]["road_name"], config[segment_id]["direction"], segment_id, config[segment_id]["length"], config[segment_id]["road_list"], config[segment_id]["start_lon"], config[segment_id]["start_lat"], config[segment_id]["end_lon"], config[segment_id]["end_lat"], month, days[day], ip_d, config[segment_id]["rn"])
                        self.insert_oracle(sql, data)
                        
            self.conn.commit();
                
                
if __name__ == '__main__':
    lapattern = Pattern("inrix_section_config", "inrix_traffic_history", "inrix_pattern_arterial")
    
    lapattern.generate_all()
    
    lapattern.close_db()
                
        
    