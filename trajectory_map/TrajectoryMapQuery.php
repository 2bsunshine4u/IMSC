<?php
$host = "host=osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com";
$port = "port=5432";
$dbname = "dbname=osm";
$credentials = "user=ds password=928Sbi2sl";
/*
$host = "host=localhost";
$port = "port=5432";
$dbname = "dbname=traj";
$credentials = "user=traj password=traj";
*/
$db  =  pg_connect( "$host $port $dbname $credentials" ); 
if(!$db){
  echo "Error : Unable to open database\n";
} 

$gpx_id = $_GET["gpx_id"];
$paths = array();

if ($gpx_id == "All"){
    $sql = "SELECT gpx_id, trk_id, trkseg_id, trkpt_id, timestamp, ST_X(geom), ST_Y(geom) FROM gpx_data WHERE timestamp <> ''";
}
else{
    $sql = "SELECT gpx_id, trk_id, trkseg_id, trkpt_id, timestamp, ST_X(geom), ST_Y(geom) FROM gpx_data WHERE gpx_id = $gpx_id and timestamp <> ''";

}
$ret = pg_query($db, $sql);
if(!$ret){
    echo pg_last_error($db);
    exit;
}

while($row = pg_fetch_row($ret)){
    $gpx_id = (int)$row[0];
    if (!array_key_exists($gpx_id, $paths)){
        $paths[$gpx_id] = array();
    }
    $trk_id = (int)$row[1];
    if (!array_key_exists($trk_id, $paths[$gpx_id])){
        $paths[$gpx_id][$trk_id] = array();
    }
    $trkseg_id = (int)$row[2];
    if (!array_key_exists($trkseg_id, $paths[$gpx_id][$trk_id])){
        $paths[$gpx_id][$trk_id][$trkseg_id] = array();
    }
    $trkpt_id = (int)$row[3];
    $time = $row[4];
    $lng = (float)$row[5];
    $lat = (float)$row[6];
    $paths[$gpx_id][$trk_id][$trkseg_id][$trkpt_id] = array($time, $lng, $lat);
}
pg_close($db);

foreach ($paths as $gpx_id => $value) {
    foreach ($paths[$gpx_id] as $trk_id => $value) {
        foreach ($paths[$gpx_id][$trk_id] as $trkseg_id => $value) {
            ksort($paths[$gpx_id][$trk_id][$trkseg_id]);
        }
    }
}
    
echo json_encode($paths);
?>