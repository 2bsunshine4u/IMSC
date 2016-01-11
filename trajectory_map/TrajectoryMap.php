<?php
    /*osmgpxfilter query:
    /usr/java/jdk1.7.0_79/bin/java -showversion -d64 -Xms1024m -Xmx6g -XX:MaxPermSize=128m -XX:-UseGCOverheadLimit -jar target/osmgpxfilter-0.1.jar -i ../gpx-planet-2013-04-09.tar.xz -ds dump -bbox top=34.5830 left=-119.4370 bottom=33.2980 right=-116.7240 -c -wpg db=traj user=traj password=traj host=localhost port=5432 geometry=point
    */
    $host = "host=osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com";
    $port = "port=5432";
    $dbname = "dbname=osm";
    $credentials = "user=ds password=928Sbi2sl";

    $db  =  pg_connect( "$host $port $dbname $credentials" ); 
    if(!$db){
      echo "Error : Unable to open database\n";
    } 

    $gpx_id_array = array();
    $sql = "SELECT distinct gpx_id FROM gpx_data WHERE timestamp <> ''";
    $ret = pg_query($db, $sql);
    if(!$ret){
        echo pg_last_error($db);
        exit;
    }
    $rows = pg_fetch_all($ret);
    foreach ($rows as $key => $row) {
        array_push($gpx_id_array, $row["gpx_id"]);
    }
    pg_close($db);
?>
<!DOCTYPE html>
<html>
<head>
<script  src="https://code.jquery.com/jquery.min.js"></script>
<script src="http://maps.googleapis.com/maps/api/js?key=AIzaSyBX-020tzotCYjKZb4_p9r-ASMBjZtcgbE"></script>
<title>Trajectory Map</title>
</head>

<body>
    <div style="width:1000px;text-align:center;">
        <h1>Trajectory Map</h1>
        <p style="text-align:left">Please select the gpx_id and click on "Show Trajectory" button.</p>
        <p style="text-align:left">After the trajectory is shown, you may click on pins to see detailed information. And you can use the zoom in&out button at the right-bottom corner</p>
        <div id="googleMap" style="height:640px;"></div>
        <div style="margin-top:40px; margin-left:auto; margin-right:auto;">
            <label for="gpx_id">GPX_ID: </label>
            <select id="gpx_id"></select>
            <button id="show_traj">Show Trajectory</button>
        </div>
    </div>
</body>
<script>
var map;
var marker_array = [];

function initialize(){
    var mapProp = {
        center:new google.maps.LatLng(34.0500, -118.2500),
        zoom:9,
        mapTypeId:google.maps.MapTypeId.ROADMAP
    };

    map=new google.maps.Map(document.getElementById("googleMap"), mapProp);


    var gpx_id_array = <?php echo json_encode($gpx_id_array); ?>;
    var option;
    $("#gpx_id").append("<option value='All'>All</option>");
    for (var gpxid in gpx_id_array){
        option = "<option value='"+gpx_id_array[gpxid]+"'>"+gpx_id_array[gpxid]+"</option>";
        $("#gpx_id").append(option); 
    } 
};
    
google.maps.event.addDomListener(window, 'load', initialize);
    
$("#show_traj").click(function(){
    $("#show_traj").html("Please Wait...");
    $("#show_traj").prop('disabled', 'true');
    $.ajax({
        url: "TrajectoryMapQuery.php",
        data:{
            "gpx_id": $("#gpx_id").val()
        },
        type: "GET",
        success: function(output){
            //document.write(output);
            var data = $.parseJSON(output);
            show_result(data);
            $("#show_traj").html("Show Trajectory");
            document.getElementById("show_traj").disabled = false;
            map.setCenter(new google.maps.LatLng(34.0500, -118.2500));
            map.setZoom(9);
        },
        error: function(xhr){
            alert("An error occured: " + xhr.status + " " + xhr.statusText);
        }
    });
});
    
function show_result(paths){ 
    var marker_icon = 'blue_dot.png';
    for (var m in marker_array){
        marker_array[m].setMap(null);
    }
    marker_array = [];

    for (var gpx_id in paths){
        for (var trk_id in paths[gpx_id]){
            var color = randomcolor();
            for (var trk_seg in paths[gpx_id][trk_id]){
                var path_latlng = paths[gpx_id][trk_id][trk_seg];
                var path = [];

                for (var i in path_latlng){
                    var position = new google.maps.LatLng(path_latlng[i][3], path_latlng[i][2]);
                    path.push(position);

                    var marker=new google.maps.Marker({
                        position: position,
                        icon: marker_icon
                    });

                    marker_array.push(marker);
                    marker.setMap(map);

                    var content = "<b>Position: " + marker.getPosition().lat() + ", " + marker.getPosition().lng() + "</b><br/>";
                    content += "<p>gpx_id: " + gpx_id + "</p>";
                    content += "<p>trk_id: " + trk_id + "</p>";
                    content += "<p>trkseg_id: " + trk_seg + "</p>";
                    content += "<p>trkpt_id: " + path_latlng[i][0] + "</p>";
                    content += "<p>time: " + path_latlng[i][1] + "</p>";
                    attachMessage(marker, content);
                }

                var polyline = new google.maps.Polyline({
                    path:path,
                    strokeColor:color,
                    strokeOpacity:0.8,
                    strokeWeight:4
                });

                marker_array.push(polyline);
                polyline.setMap(map);
            }
        }
    }
}

    
var infowindow = new google.maps.InfoWindow({maxWidth:300});
    
function attachMessage(marker, message) {
    marker.addListener('click', function() {
        infowindow.setContent(message);
        infowindow.open(marker.get('map'), marker);
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