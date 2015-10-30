import psycopg2

if __name__ == '__main__':
    road_sections = {}
    fileout = open("sections.txt", 'w')
    
    print "Connecting to database ......"
    conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
    if conn_to:
        print "Connected."
    cursor = conn_to.cursor() 
    
    sql = "select road_name, direction, from_postmile, to_postmile from \"SS_SENSOR_MAPPING_ALL\""
    cursor.execute(sql)
    results = cursor.fetchall()
    for road_name, direction, from_postmile, to_postmile in results:
        if road_name not in road_sections:
            road_sections[road_name] = {}
        if direction not in road_sections[road_name]:
            road_sections[road_name][direction] = []
        if (from_postmile, to_postmile) not in road_sections[road_name][direction]:
            road_sections[road_name][direction].append((from_postmile, to_postmile))
            
            
    roads = []
        
    keys = road_sections.keys()
    keys.sort(key=lambda x:int(x))
    for road in keys:  
        p = str(road)+" => array("
        if road not in roads:
            roads.append(int(road))
        for direction in road_sections[road]:
            s = "road_name["+road+'][' + str(direction)+']=['
            for (from_pm, to_pm) in road_sections[road][direction]:
                s += str(from_pm)+','
            if s[-1] == ',':
                s = s[:-1] + '];'
            s += '\n'
            fileout.write(s)
            
            p += str(direction)+','
        p = p[:-1]+'),'
        print p
            
    print roads
    
    fileout.close()
    