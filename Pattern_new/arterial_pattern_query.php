<?php
    $host = "gd.usc.edu";
    $sid = "ADMS";
    $username = "shuai";
    $password = "shuai2015pass";

    $pattern_table = "inrix_pattern_arterial";

    $db  =  oci_connect($username,$password,"$host/$sid");
        if(!$db){
          echo "Error : Unable to open database\n";
        } 

    $road_name = $_GET["road_name"];

    date_default_timezone_set("America/Los_Angeles");
    $weekday = date("l");

    $sql = "SELECT segment_id, length, direction, road_list, start_lon, start_lat, end_lon, end_lat, weekday, inrix_pattern from $pattern_table where road_name = '$road_name'";
    $stid = oci_parse($db, $sql);
    $ret = oci_execute($stid, OCI_DEFAULT);
    if(!$ret){
        exit;
    }
    $segments = array();
    while(($row = oci_fetch_row($stid)) != false){ 
        $segment_id = $row[0]; 
        $length = $row[1]; 
        $direction = $row[2]; 
        $road_list = $row[3]; 
        $start_lon = $row[4]; 
        $start_lat = $row[5]; 
        $end_lon = $row[6]; 
        $end_lat = $row[7];
        $weekday = $row[8];
        $raw_pattern = substr($row[9], strpos($row[9], '{')+1, -2); 
        $day_pattern = explode(",", $raw_pattern);
        if (!array_key_exists($segment_id, $segments)){
            $pattern = array();
            $pattern[$weekday] = $day_pattern;
            $segments[$segment_id] = array($start_lon, $start_lat, $end_lon, $end_lat, $road_name, $length, $direction, $road_list, $pattern);
        }
        else{
            $segments[$segment_id][8][$weekday] = $day_pattern;
        }
    }
        
    echo json_encode($segments);
?>