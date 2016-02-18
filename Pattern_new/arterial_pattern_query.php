<?php
    $host = "gd.usc.edu";
    $sid = "ADMS";
    $username = "shuai";
    $password = "shuai2015pass";

    $config_table = "inrix_section_config";
    $pattern_table = "inrix_pattern_arterial";
    $geocode_table = "reverse_geocode";

    $db  =  oci_connect($username,$password,"$host/$sid");
        if(!$db){
          echo "Error : Unable to open database\n";
        } 

    $road_name = $_GET["road_name"];

    date_default_timezone_set("America/Los_Angeles");
    $weekday = date("l");

    if (isset($_GET['segment_id'])):
        $segment_id = $_GET['segment_id'];
        $sql = "SELECT segment_id, length, direction, road_list, start_lon, start_lat, end_lon, end_lat, month, weekday, inrix_pattern from $pattern_table where segment_id = $segment_id";
        $stid = oci_parse($db, $sql);
        $ret = oci_execute($stid, OCI_DEFAULT);
        if(!$ret){
            exit;
        }
        while(($row = oci_fetch_row($stid)) != false){ 
            $segment_id = $row[0]; 
            $length = $row[1]; 
            $direction = $row[2]; 
            $road_list = $row[3]; 
            $start_lon = $row[4]; 
            $start_lat = $row[5]; 
            $end_lon = $row[6]; 
            $end_lat = $row[7];
            $month = $row[8];
            $weekday = $row[9];
            $raw_pattern = substr($row[10], strpos($row[10], '{')+1, -2); 
            $day_pattern = explode(",", $raw_pattern);

            if (!isset($segment)){
                $pattern = array();
                $pattern[$month] = array();
                $pattern[$month][$weekday] = $day_pattern;
                $segment = array($segment_id, $start_lon, $start_lat, $end_lon, $end_lat, $road_name, $length, $direction, $road_list, $pattern);
            }
            if(!array_key_exists($month, $segment[9])){
                $segment[9][$month] = array();
                $segment[9][$month][$weekday] = $day_pattern;
            }
            else{
                $segment[9][$month][$weekday] = $day_pattern;
            }
        }   
        echo json_encode($segment);


    elseif (isset($_GET['direction'])):
        $direction = $_GET['direction'];
        $sql = "SELECT distinct segment_id, start_lon, start_lat, end_lon, end_lat from $config_table where segment_id in (select segment_id from $geocode_table where road = '$road_name') and direction = '$direction'";
        $stid = oci_parse($db, $sql);
        $ret = oci_execute($stid, OCI_DEFAULT);
        if(!$ret){
            exit;
        }
        $segments = array();
        while(($row = oci_fetch_row($stid)) != false){ 
            $segment_id = $row[0]; 
            $start_lon = $row[1]; 
            $start_lat = $row[2]; 
            $end_lon = $row[3]; 
            $end_lat = $row[4];
            if (!array_key_exists($segment_id, $segments)){
                $segments[$segment_id] = array($start_lon, $start_lat, $end_lon, $end_lat);
            }
        }   
        echo json_encode($segments);

    else:
        $sql = "select distinct direction from $config_table where segment_id in (select segment_id from $geocode_table where road = '$road_name')";
        $stid = oci_parse($db, $sql);
        $ret = oci_execute($stid, OCI_DEFAULT);
        if(!$ret){
            exit;
        }
        $directions = array();
        while(($row = oci_fetch_row($stid)) != false){ 
            array_push($directions, $row[0]);
        }

        echo json_encode($directions);

    endif;
        
    
?>