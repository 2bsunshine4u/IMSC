<?php 
$host = "host=osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com";
$port = "port=5432";
$dbname = "dbname=osm";
$credentials = "user=ds password=ds2015";

$db  =  pg_connect( "$host $port $dbname $credentials" ); 
    if(!$db){
      echo "Error : Unable to open database\n";
    } 

date_default_timezone_set("America/Los_Angeles");
$weekday = date("l");


if ($_SERVER["REQUEST_METHOD"] == "GET"):

$road_similarity = array();

$sql = 'SELECT road_name, direction, from_postmile, similarity from ' .'"'.'SS_SECTION_PATTERN_ALL'.'"'.' WHERE day='."'".$weekday."'";
$ret = pg_query($db, $sql);
if(!$ret){
    echo pg_last_error($db);
    exit;
}

while($row = pg_fetch_row($ret)){
    $road_name = (int)$row[0];
    if (!array_key_exists($road_name, $road_similarity)){
        $road_similarity[$road_name] = array();
    }
    $direction = (int)$row[1];
    if (!array_key_exists($direction, $road_similarity[$road_name])){
        $road_similarity[$road_name][$direction] = array(array(),array(),array(),array(),array());
    }
    $section = ((int)$row[2])/3;
    $raw_similarity = substr($row[3], 1, strlen($row[3])-2);
    $similarity = explode(",", $raw_similarity);
    for ($i = 0; $i < 5; $i ++){
        if ($similarity[$i] != "NULL"){
            array_push($road_similarity[$road_name][$direction][$i], $similarity[$i]);
        }
    }
}

foreach ($road_similarity as $road_name => $s){
    ksort($road_similarity[$road_name]);
    foreach ($road_similarity[$road_name] as $direction => $si){
        for ($i = 0; $i < 5; $i ++){
            if (count($road_similarity[$road_name][$direction][$i]) == 0){
                $road_similarity[$road_name][$direction][$i] = "null";
            }
            else{
                $road_similarity[$road_name][$direction][$i] = round(array_sum($road_similarity[$road_name][$direction][$i]) / count($road_similarity[$road_name][$direction][$i]), 2);
            }
        }
    }
}
ksort($road_similarity);


?>

<head>
<title>Patterns Comparison</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link href="pattern_all.css" rel="stylesheet" type="text/css" media="screen"/>
<script src="Chart.js"></script>
<script src="Chart.min.js"></script>
<script src="jquery-2.1.4.js"></script>
</head>
<body>
<h1 style = 'text-align:center'>Pattern Comparison</h1>
<div class="main_div">
<table class="main" border="0">
    <tr>
        <th rowspan="2">Highway_Name</th>
        <th rowspan="2">Direction</th>
        <th rowspan="2">Day</th>
        <th colspan="5">Similarity</th>
    </tr>
    <tr>
        <th>Moring(6-9)</th>
        <th>Noon(9-15)</th>
        <th>Afternoon(15-18)</th>
        <th>Evening(18-21)</th>
        <th>Average</th>
    </tr>           
    <?php 
        foreach ($road_similarity as $road_name=>$directions){
            foreach ($directions as $direction=>$similarity){
                if ($direction == 0){
                    $dir = "North";
                }
                elseif ($direction == 1){
                    $dir = "South";
                }
                elseif ($direction == 2){
                    $dir = "East";
                }
                elseif ($direction == 3){
                    $dir = "West";
                }
                foreach ($similarity as $key=>$value){
                    if ($value < 8){
                        $similarity[$key] = "<td bgcolor='#FFFF00'>$value";
                    }
                    else{
                        $similarity[$key] = "<td>$value";
                    }
                }
                echo "<tr id='R$road_name"."_"."$direction' onclick=\"click_road('$road_name','$direction')\"><td>".$road_name."</td><td>$dir</td><td>".$weekday."</td>$similarity[1]</td>$similarity[2]</td>$similarity[3]</td>$similarity[4]</td>$similarity[0]</td></tr>";
            }
        }
    ?>
</table>    
</div>
</body>
<script type="text/javascript">
    function click_road(road_name, direction){
        var dir;
        if (direction == 0){
            dir = "North";
        }
        else if(direction == 1){
            dir = "South";
        }
        else if (direction == 2){
            dir = "East";
        }
        else if (direction == 3){
            dir = "West";
        }
        var selected_road = null;
        if ($(".section_row").length > 0){
            selected_road = $(".section_row").prev();
            $(".section_row").remove();
        }
        if (!selected_road || selected_road.attr('id') != "R"+road_name+"_"+direction){
            var php_self = "<?php echo $_SERVER['PHP_SELF'] ?>";
            $.post(php_self, {"road_name": road_name, "direction": direction}, function(data){
                var table2html = "<tr class='section_row'><td colspan='8'><table border='0' class='section' width='80%'><tr><th rowspan='2'>Highway_Name</th><th rowspan='2'>Direction</th><th rowspan='2'>From_Postmile</th><th rowspan='2'>To_Postmile</th><th rowspan='2'>Day</th><th colspan='5'>Similarity</th></tr><tr><th>Moring(6-9)</th><th>Noon(9-15)</th><th>Afternoon(15-18)</th><th>Evening(18-21)</th><th>Average</th></tr>";
                var section_similarity = data;
                for (var section=0; section < section_similarity.length; section ++){
                    if (section_similarity[section]){
                        for (var i=0; i < 5; i++){
                            if (section_similarity[section][i] < 8){
                                section_similarity[section][i] = "<td bgcolor='#FFFF00'>" + section_similarity[section][i];
                            }
                            else{
                                section_similarity[section][i] = "<td>" + section_similarity[section][i];
                            }
                        }
                        table2html += "<tr id='S"+road_name+"_"+direction+"_"+section+"' onclick=\"click_section('"+road_name+"','"+direction+"','"+section+"');\"><td>"+road_name+"</td><td>"+dir+"</td><td>"+(section*3)+"</td><td>"+(section*3+3)+"</td><td><?php echo $weekday; ?></td>"+section_similarity[section][1]+"</td>"+section_similarity[section][2]+"</td>"+section_similarity[section][3]+"</td>"+section_similarity[section][4]+"</td>"+section_similarity[section][0]+"</td></tr>"; 
                    }
                }
                table2html += "</table></td></tr>";
                $("#R"+road_name+"_"+direction).after(table2html);
            },
            "json");
        }
    }
    
    function click_section(road_name, direction, section){
        var selected_section = null;
        if ($(".color_row").length > 0){
            selected_section = $(".color_row").prev();
            $(".color_row").remove();
            $(".chart_row").remove();
        }
        if (!selected_section || selected_section.attr('id') != "S"+road_name+"_"+direction+"_"+section){
            var php_self = "<?php echo $_SERVER['PHP_SELF']; ?>";
            $.post(php_self, {"road_name": road_name, "direction":direction, "section":section}, function(data){
                var chart_html = "<tr class='color_row'><td colspan='10'><table align='center'><th>Realtime_pattern: </th><th><hr width=50 noshade color='#0000FF' /></th><th>&nbsp;Historical_pattern:</th><th><hr width=50 noshade color='#00FF00'></th></table></td></tr><tr class='chart_row'><td colspan='10'><canvas id='section_chart' width='800%' height='400' /></td></tr>";
                $("#S"+road_name+"_"+direction+"_"+section).after(chart_html);

                var ctx = document.getElementById("section_chart").getContext("2d");
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
                        data : data['realtime_pattern']
                        },	
                        {
                        fillColor : "rgba(255,255,255,0)",
                        strokeColor : "rgba(0,255,0,1)",
                        pointColor : "rgba(34,140,34,1)",
                        pointStrokeColor : "#fff",
                        data : data['historical_pattern']
                        }
                    ]
                }
                var myNewChart = new Chart(ctx).Line(chart_data, {
                    scaleOverride :true ,   //是否用硬编码重写y轴网格线
                    scaleSteps : 11,        //y轴刻度的个数
                    scaleStepWidth : 10,   //y轴每个刻度的宽度
                    scaleStartValue : 20,    //y轴的起始值
                    pointDot : true,        //是否显示点
                    pointDotRadius : 5,     //点的半径  
                    pointDotStrokeWidth : 1,//点的线宽
                    datasetStrokeWidth : 3, //数据线的线宽
                    animation : true,       //是否有动画效果
                    animationSteps : 60    //动画的步数
                } );
                
            },
            "json");
        }
    }
</script>
<?php
elseif ($_SERVER["REQUEST_METHOD"] == "POST"):
    ini_set("display_errors", "off");
    $road_name = $_POST["road_name"];
    $direction = $_POST["direction"];

    if (!isset($_POST["section"])):
        $section_similarity = array();

        $sql = 'SELECT from_postmile, to_postmile, similarity from ' .'"'.'SS_SECTION_PATTERN_ALL'.'"'.' WHERE road_name='."'".$road_name."'".' AND direction='.$direction.' AND day='."'".$weekday."'";
        $ret = pg_query($db, $sql);
        if(!$ret){
            echo pg_last_error($db);
            exit;
        }
        while($row = pg_fetch_row($ret)){
            $from_postmile = (int)$row[0];
            $to_postmile = (int)$row[1];
            $section = $from_postmile / 3;
            $raw_similarity = substr($row[2], 1, strlen($row[2])-2);
            $similarity = explode(",", $raw_similarity);
            foreach ($similarity as $key => $value) {
                if($value=="NULL"){
                    $similarity[$key] = null;
                }
                else{
                    $similarity[$key] = round($similarity[$key], 2);
                }
            }

            if ($section != count($section_similarity)){
                for ($i = count($section_similarity); $i<$section; $i++){
                    $section_similarity[$i] = null;
                }
            }
            $section_similarity[$section] = $similarity;
        }

        echo json_encode($section_similarity);
    else:
        $section = $_POST["section"];
        $from_postmile = (int)$section * 3;

        $patterns = array();
        
        $sql = "SELECT realtime_pattern, historical_pattern from \"SS_SECTION_PATTERN_ALL\" WHERE road_name = '$road_name' AND direction = $direction AND from_postmile = $from_postmile AND day = '$weekday'";
        $ret = pg_query($db, $sql);
        if(!$ret){
            echo pg_last_error($db);
            exit;
        }
        $row = pg_fetch_row($ret);
        $raw_realtime_pattern = substr($row[0], 1, strlen($row[0])-2);
        $realtime_pattern = explode(",", $raw_realtime_pattern);
        $raw_historical_pattern = substr($row[1], 1, strlen($row[1])-2);
        $historical_pattern = explode(",", $raw_historical_pattern);

        foreach ($realtime_pattern as $key => $value) {
            if($value=="NULL")
                $realtime_pattern[$key] = null;
        }
        foreach ($historical_pattern as $key => $value) {
            if($value=="NULL")
                $historical_pattern[$key] = null;
        }
        
        $patterns['realtime_pattern'] = $realtime_pattern;
        $patterns['historical_pattern'] = $historical_pattern;

        echo json_encode($patterns);
    endif;
endif;
?>
