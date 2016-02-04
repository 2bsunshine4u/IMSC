<?php
    $host = "gd.usc.edu";
    $sid = "ADMS";
    $username = "shuai";
    $password = "shuai2015pass";

    $links_table = "links";
    $nodes_table = "nodes";
    $segments_table = "inrix_section_config";
    $mapping_table = "segment_mapping_highway";
    $pattern_table = "inrix_pattern_arterial";


    $db  =  oci_connect($username,$password,"$host/$sid");
        if(!$db){
          echo "Error : Unable to open database\n";
        } 

    date_default_timezone_set("America/Los_Angeles");
    $weekday = date("l");

    $sql = "SELECT road_name, segment_id, length, direction, road_list, start_lon, start_lat, end_lon, end_lat from $pattern_table";
    $stid = oci_parse($db, $sql);
    $ret = oci_execute($stid, OCI_DEFAULT);
    if(!$ret){
        exit;
    }
    $road_segments = array();
    while(($row = oci_fetch_row($stid)) != false){
        $road_name = $row[0]; 
        $segment_id = $row[1]; 
        $length = $row[2]; 
        $direction = $row[3]; 
        $road_list = $row[4]; 
        $start_lon = $row[5]; 
        $start_lat = $row[6]; 
        $end_lon = $row[7]; 
        $end_lat = $row[8];
        if (!array_key_exists($road_name, $road_segments)){
            $road_segments[$road_name] = array();
        }
        if (!array_key_exists($segment_id, $road_segments[$road_name])){
            $road_segments[$road_name][$segment_id] = array($start_lon, $start_lat, $end_lon, $end_lat, $road_name, $length, $direction, $road_list);
        }
    }
?>
<!DOCTYPE html>
<html>
<head>
<script src="https://code.jquery.com/jquery.min.js"></script>
<script src="http://maps.googleapis.com/maps/api/js?key=AIzaSyBX-020tzotCYjKZb4_p9r-ASMBjZtcgbE"></script>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js" integrity="sha384-0mSbJDEHialfmuBBQP6A4Qrprq5OVfW37PRR3j5ELqxss1yVqOtnepnHVP9aJ7xS" crossorigin="anonymous"></script>
<title>Arterial Segments</title>
</head>

<body>
    <div style="width:100%;text-align:center;">
        <h1>Arterial Pattern</h1>
        <p>Click on segments to see details.</p>
        <div id="googleMap" style="height:640px;"></div>
        <div style="padding-top:20px; padding-bottom:80px; margin-left:auto; margin-right:auto;width:70%;">
            <label class="control-label col-md-2" for="roads" style="margin-top:7px">GPX_ID: </label>
            <div class="col-md-8">
                <select class="form-control" id="roads"></select>
            </div>
            <div class="col-md-2">
                <button class="btn btn-default" id="show_segments">Show Segments</button>
            </div>
        </div>
    </div>
</body>
<script>
var map;
var marker_array = [];
var road_segments = <?php echo json_encode($road_segments); ?>;

function initialize(){
    var mapProp = {
        center:new google.maps.LatLng(34.0500, -118.2500),
        zoom:9,
        mapTypeId:google.maps.MapTypeId.ROADMAP
    };

    map=new google.maps.Map(document.getElementById("googleMap"), mapProp);

    var option;
    //$("#gpx_id").append("<option value='All'>All</option>");
    for (var road_name in road_segments){
        option = "<option value='"+road_name+"'>"+road_name+"</option>";
        $("#roads").append(option); 
    } 
};
    
google.maps.event.addDomListener(window, 'load', initialize);
    
$("#show_segments").click(function(){
    show_result(road_segments[$("#roads").val()]);
    /*
    $("#show_segments").html("Please Wait...");
    $("#show_segments").prop('disabled', 'true');
    $.ajax({
        url: "arterial_pattern_query.php",
        data:{
            "road_name": $("#roads").val()
        },
        type: "GET",
        success: function(output){
            //document.write(output);
            var data = $.parseJSON(output);
            show_result(data);
            $("#show_segments").html("Show Segments");
            document.getElementById("show_segments").disabled = false;
            map.setCenter(new google.maps.LatLng(34.0500, -118.2500));
            map.setZoom(9);
        },
        error: function(xhr){
            alert("An error occured: " + xhr.status + " " + xhr.statusText);
        }
    });*/
});
    
function show_result(segments){
    var marker_icon = 'blue_dot.png';
    var color = randomcolor();
    for (var m in marker_array){
        marker_array[m].setMap(null);
    }
    marker_array = [];

    for (var segment_id in segments){
        var start_lon = parseFloat(segments[segment_id][0]); 
        var start_lat = parseFloat(segments[segment_id][1]);
        var end_lon = parseFloat(segments[segment_id][2]); 
        var end_lat = parseFloat(segments[segment_id][3]); 
        var road_name = segments[segment_id][4];
        var length = segments[segment_id][5]; 
        var direction = segments[segment_id][6];
        var road_list = segments[segment_id][7];

        var start_pt = new google.maps.LatLng(start_lat, start_lon);
        var end_pt = new google.maps.LatLng(end_lat, end_lon);

        var path = [start_pt, end_pt];
        for (var i in path){
            var marker=new google.maps.Marker({
                position: path[i],
                icon: marker_icon
            });

            marker_array.push(marker);
            marker.setMap(map);
        }

        var polyline = new google.maps.Polyline({
            path:path,
            strokeColor:color,
            strokeOpacity:0.8,
            strokeWeight:4
        });

        marker_array.push(polyline);
        polyline.setMap(map);

        var content = "<b>Road: " + road_name + "</b><br/>";
        content += "<b>segment_id: " + segment_id + "</b><br/>";
        content += "direction: " + direction + "</br>";
        content += "road_list: " + road_list + "</br>";
        content += "length: " + length + " km</br>";
        content += "direction: " + direction + "</br>";
        content += "start_location: " + start_lat + ", " + start_lon + "</br>";
        content += "end_location: " + end_lat + ", " + end_lon + "</br>";
        var mid = new google.maps.LatLng((start_lat+end_lat)/2.0, (start_lon+end_lon)/2.0);
        attachMessage(polyline, mid, content);
    }
}

    
var infowindow = new google.maps.InfoWindow({maxWidth:300});
    
function attachMessage(marker, position, message) {
    marker.addListener('click', function() {
        infowindow.setContent(message);
        infowindow.setPosition(position);
        infowindow.open(marker.get('map'));
        
    });
}
    
function randomcolor(){
    var colorvalue=["0","2","3","4","5","6","7","8","9","a","b","c","d","e","f"], colorprefix="#", index;
    for(var i=0;i < 6; i++){
        index=Math.round(Math.random()*14);
        colorprefix+=colorvalue[index];
    }
    return colorprefix;
}
</script>
</html>