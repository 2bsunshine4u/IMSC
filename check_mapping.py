import psycopg2
import Utils

if __name__ == '__main__':
    fileout = open("pattern.txt", 'w')
    
    print "Connecting to database ......"
    conn_to = psycopg2.connect(host='osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='osm', user='ds', password='ds2015')
    if conn_to:
        print "Connected."
    cursor = conn_to.cursor() 
    
    print "fetching mapping information from database"
    sql = "select road_name, direction, from_postmile, link_id, sensor_id from \"SS_SENSOR_MAPPING\""
    cursor.execute(sql)
    results = cursor.fetchall()
    mapping = {}
    for road_name, direction, from_postmile, link_id, sensor_id in results:
        if int(road_name):
            section = from_postmile/3
            if road_name not in mapping:
                mapping[road_name] = {}
            if direction not in mapping[road_name]:
                mapping[road_name][direction] = {}
            if section not in mapping[road_name][direction]:
                mapping[road_name][direction][section] = {}
            if link_id not in mapping[road_name][direction][section]:
                mapping[road_name][direction][section][link_id] =[]
            if sensor_id:
                if sensor_id not in mapping[road_name][direction][section][link_id]:
                    mapping[road_name][direction][section][link_id].append(sensor_id)

    print "Connecting to database ......"
    his_conn_to = psycopg2.connect(host='graph-3.cfmyklmn07yu.us-west-2.rds.amazonaws.com', port='5432', database='tallygo', user='ds', password='ds2015')
    if his_conn_to:
        print "Connected."
    his_cursor = his_conn_to.cursor()
    
    
    his_mapping = {}
    link_name = {}
    for road_name in mapping:
        for direction in mapping[road_name]:
            for section in mapping[road_name][direction]:
                for link_id in mapping[road_name][direction][section]:
                    sql = "select name_default, from_node_id, to_node_id from links where link_id = " + str(link_id)
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    name_default, from_node_id, to_node_id = results[0]
                    link_name[link_id] = name_default
                        
                    sql = "select sensor_id from on_edge_sensor_mapping where from_node_id ="+str(from_node_id)+"and to_node_id = "+str(to_node_id)
                    his_cursor.execute(sql)
                    results = his_cursor.fetchall()
                    for sensorl in results:
                        sensor_id = sensorl[0]
                        if link_id not in his_mapping:
                            his_mapping[link_id] = []
                        his_mapping[link_id].append(sensor_id)
    
    print "Comparing"
    for road_name in mapping:
        for direction in mapping[road_name]:
            for section in mapping[road_name][direction]:
                for link_id in mapping[road_name][direction][section]:
                    if (link_id not in his_mapping and len(mapping[road_name][direction][section][link_id]) == 1) or  (link_id in his_mapping and set(his_mapping[link_id]).issubset(set(mapping[road_name][direction][section][link_id])) == False):
                        flag = False
                        for sensor_id in mapping[road_name][direction][section][link_id]: 
                            sql = "select * from highway_sensor_config where sensor_id =" + str(sensor_id)
                            cursor.execute(sql)
                            result = cursor.fetchall()
                            if len(result) == 0:
                                flag = True
                        if flag:
                            continue
                        
                        
                        print '\n\n'
                        print "Road:", link_name[link_id]
                        print "Direction", direction
                        print "Section:", section
                        print "Link id:", link_id
                        
                        sql = "select from_node_id, to_node_id from links where link_id = " + str(link_id)
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        from_node_id, to_node_id = results[0]
                        
                        sql = "select ST_AsText(geom) from nodes where node_id = "+ str(from_node_id)
                        cursor.execute(sql)
                        from_node_loc = Utils.extract_loc_from_geometry(cursor.fetchall()[0][0])
                        
                        sql = "select ST_AsText(geom) from nodes where node_id = "+ str(to_node_id)
                        cursor.execute(sql)
                        to_node_loc = Utils.extract_loc_from_geometry(cursor.fetchall()[0][0])
                        
                        print "from_node:",from_node_loc[::-1],"to_node:", to_node_loc[::-1]
                        
                        print "\nMy mapped Sensors:", mapping[road_name][direction][section][link_id]
                        if link_id not in his_mapping:
                            print "Historical mapped Sensors:", []
                        else:
                            print "Historical mapped Sensors:", his_mapping[link_id]
                        
                        
                        print "My sensor Location:"
                        for sensor_id in mapping[road_name][direction][section][link_id]: 
                            sql = "select direction, onstreet, ST_AsText(start_lat_long) from highway_congestion_config where sensor_id = " + str(sensor_id)
                            cursor.execute(sql)
                            result = cursor.fetchall()[0]
                            sensor_dir = result[0]
                            onstreet = result[1]
                            sensor_loc = Utils.extract_loc_from_geometry(result[2])
                            print "sensor_id:",sensor_id,"direction:",sensor_dir,"onstreet:",onstreet,"loc:",sensor_loc[::-1]
                            
                        if link_id not in his_mapping:
                            print "Missing Mapping!!!!!!"
                            continue
                        
                        print "\nHistorical sensor Location:"
                        for sensor_id in his_mapping[link_id]: 
                            sql = "select direction, onstreet, ST_AsText(start_lat_long) from highway_congestion_config where sensor_id = " + str(sensor_id)
                            cursor.execute(sql)
                            result = cursor.fetchall()[0]
                            sensor_dir = result[0]
                            onstreet = result[1]
                            sensor_loc = Utils.extract_loc_from_geometry(result[2])
                            print "sensor_id:",sensor_id,"direction:",sensor_dir,"onstreet:",onstreet,"loc:",sensor_loc[::-1]
                            if road_name not in onstreet:
                                print "WRONG ROAD!!!!"
                            if sensor_dir != direction:
                                print "WRONG DIRECTION!!!"
                            
                    else:
                        #print "Same mapping on:",road_name,direction,section,link_id
                        continue
                        
                            
                        