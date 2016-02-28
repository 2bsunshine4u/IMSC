import cx_Oracle
import datetime
import Utils

class Prefix(object):
    def __init__(self, geocode_table):
        self.geocode_table = geocode_table
        
        self.init_db()
        
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

    def update_prefix(self):
        sql = "select distinct segment_id, start_road, end_road, mid_road from "+self.geocode_table+" where check_flag = 'f'"
        results = self.query_oracle(sql)
        for segment_id, start_road, end_road, mid_road in results:
            if self.remove_prefix(start_road) == self.remove_prefix(end_road):
                road = start_road
                print "start_road: ", start_road, "\tend_road: ", end_road
            elif self.remove_prefix(start_road) == self.remove_prefix(mid_road):
                road = start_road
                print "start_road: ", start_road, "\tmid_road: ", mid_road
            elif self.remove_prefix(mid_road) == self.remove_prefix(end_road):
                road = mid_road
                print "mid_road: ", mid_road, "\tend_road: ", end_road
            else:
                continue

            update_sql = "update "+self.geocode_table+" set road = '"+road+"', check_flag = 't' where segment_id = "+str(segment_id)
            self.operation_oracle(update_sql)

    def remove_prefix(self, addr):
        addr = addr.strip();

        if addr[0] in ['E', 'W', 'N', 'S'] and addr[1] == ' ':
            return addr[2:]
        else:
            return addr
if __name__ == "__main__":
    prefix = Prefix("reverse_geocode")
    prefix.update_prefix()
    prefix.close_db();
