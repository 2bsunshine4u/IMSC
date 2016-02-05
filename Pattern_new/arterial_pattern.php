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

    date_default_timezone_set("America/Los_Angeles");
    $weekday = date("l");

    $sql = "SELECT distinct road_name from $pattern_table";
    $stid = oci_parse($db, $sql);
    $ret = oci_execute($stid, OCI_DEFAULT);
    if(!$ret){
        exit;
    }
    $roads = array();
    while(($row = oci_fetch_row($stid)) != false){
        $road_name = $row[0]; 
        array_push($roads, $road_name);
    }
?>
<!DOCTYPE html>
<html>
<head>
<script src="https://code.jquery.com/jquery.min.js"></script>
<script src="http://maps.googleapis.com/maps/api/js?key=AIzaSyBX-020tzotCYjKZb4_p9r-ASMBjZtcgbE"></script>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js" integrity="sha384-0mSbJDEHialfmuBBQP6A4Qrprq5OVfW37PRR3j5ELqxss1yVqOtnepnHVP9aJ7xS" crossorigin="anonymous"></script>
<script src="Chart.js"></script>
<title>Arterial Segments</title>
</head>

<body>
    <div style="width:100%;text-align:center;">
        <h1>Arterial Pattern</h1>
        <p>Click on segments to see details.</p>
        <div id="googleMap" style="height:640px;"></div>
        <div style="padding-top:20px; padding-bottom:80px; margin-left:auto; margin-right:auto;width:70%;">
            <label class="control-label col-md-2" for="roads" style="margin-top:7px">Road: </label>
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
var road_list = <?php echo json_encode($roads); ?>;

function initialize(){
    var mapProp = {
        center:new google.maps.LatLng(34.0500, -118.2500),
        zoom:9,
        mapTypeId:google.maps.MapTypeId.ROADMAP
    };

    map=new google.maps.Map(document.getElementById("googleMap"), mapProp);

    var option;
    //$("#gpx_id").append("<option value='All'>All</option>");
    for (var road_name in road_list){
        option = "<option value='"+road_list[road_name]+"'>"+road_list[road_name]+"</option>";
        $("#roads").append(option); 
    } 
};
    
google.maps.event.addDomListener(window, 'load', initialize);
    
$("#show_segments").click(function(){
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
    });
});
    
function show_result(segments){
    var marker_icon = 'blue_dot.png';
    var color = randomcolor();
    for (var m in marker_array){
        marker_array[m].setMap(null);
    }
    marker_array = [];

    var weekday = '<?php echo $weekday; ?>';

    for (var segment_id in segments){
        var start_lon = parseFloat(segments[segment_id][0]); 
        var start_lat = parseFloat(segments[segment_id][1]);
        var end_lon = parseFloat(segments[segment_id][2]); 
        var end_lat = parseFloat(segments[segment_id][3]); 
        var road_name = segments[segment_id][4];
        var length = segments[segment_id][5]; 
        var direction = segments[segment_id][6];
        var road_list = segments[segment_id][7];
        var patterns = segments[segment_id][8];

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

        var content = "<table class='table table-condensed'><tr><td><b>Road: </b></td><td><b>" + road_name + "</b></td></tr>";
        content += "<tr><td><b>segment_id: </b></td><td><b>" + segment_id + "</b></td></tr>";
        content += "<tr><td>direction: </td><td>" + direction + "</td></tr>";
        content += "<tr><td>road_list: </td><td>" + road_list + "</td></tr>";
        content += "<tr><td>length: </td><td>" + length + " km</td></tr>";
        content += "<tr><td>direction: </td><td>" + direction + "</td></tr>";
        content += "<tr><td>start_position: </td><td>" + start_lat + ", " + start_lon + "</td></tr>";
        content += "<tr><td>end_position: </td><td>" + end_lat + ", " + end_lon + "</td></tr>";
        content += "<tr><td><label class='control-label' for='#weekday'>inrix_pattern: </label></td>";
        content += "<td><select class='form-control' id='weekday'>";
        content += "<option value='Monday'>Monday</option>";
        content += "<option value='Tuesday'>Tuesday</option>";
        content += "<option value='Wednesday'>Wednesday</option>";
        content += "<option value='Thursday'>Thursday</option>";
        content += "<option value='Friday'>Friday</option>";
        content += "<option value='Saturday'>Saturday</option>";
        content += "<option value='Sunday'>Sunday</option></select></td></tr>";
        content += "<tr><td colspan='2'><canvas id='segment_chart' width='600' height='250' /></td></tr></table>";
        var mid = new google.maps.LatLng((start_lat+end_lat)/2.0, (start_lon+end_lon)/2.0);
        attachMessage(polyline, mid, content, patterns, weekday);
    }
}

    
var infowindow = new google.maps.InfoWindow({maxWidth:800});
    
function attachMessage(marker, position, message, patterns, weekday) {
    marker.addListener('click', function() {
        infowindow.setContent(message);
        infowindow.setPosition(position);
        infowindow.open(marker.get('map'));
        show_chart(patterns[weekday]);
        $("#weekday option[value='"+weekday+"']").prop('selected', 'selected');
        $("#weekday").change(function(event) {
            //document.write($("#weekday").val());
            show_chart(patterns[$("#weekday").val()]);
        });
    });
}

function show_chart(pattern){
    var ctx = document.getElementById("segment_chart").getContext("2d");
    var chart_data = {
        labels : ["6:00","6:15","6:30","6:45","7:00","7:15","7:30","7:45","8:00","8:15","8:30","8:45","9:00","9:15","9:30","9:45",
    "10:00","10:15","10:30","10:45","11:00","11:15","11:30","11:45","12:00","12:15","12:30","12:45","13:00","13:15","13:30","13:45",
    "14:00","14:15","14:30","14:45","15:00","15:15","15:30","15:45","16:00","16:15","16:30","16:45","17:00","17:15","17:30","17:45",
    "18:00","18:15","18:30","18:45","19:00","19:15","19:30","19:45","20:00","20:15","20:30","20:45"
        ],
        datasets : [
            {
            fillColor : "rgba(255,255,255,0)",
            strokeColor : "rgba(0,0,255,1)",
            pointColor : "rgba(0,180,205,1)",
            pointStrokeColor : "#fff",
            data : pattern
            }
        ]
    }
    var myNewChart = new Chart(ctx).Line(chart_data, {
        scaleOverride :false ,   
        scaleSteps : 11,        
        scaleStepWidth : 10,   
        scaleStartValue : 20,    
        pointDot : true,        
        pointDotRadius : 5,     
        pointDotStrokeWidth : 1,
        datasetStrokeWidth : 3, 
        animation : true,       
        animationSteps : 60    
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