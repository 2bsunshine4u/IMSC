<?php
        echo "<h1 style = 'text-align:center'>Pattern Comparison</h1>";	
	$host        = "host=osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com";
  $port        = "port=5432";
  $dbname      = "dbname=osm";
  $credentials = "user=ds password=ds2015";
  $roads       = array('AVIATION BLVD', 'FIGUEROA ST', 'GRAND AVE', 'LA CIENEGA AVE', 'LA CIENEGA BLVD', 'MARTIN LUTHER KING BLVD', 'S HOOVER ST');
  sort($roads);
  $road_name = null;
  $realtime_pattern = null;
  $historical_pattern = null;
  $si = null;
  $day = null;
  $direction = -1;
  $postmile = -1;
  if($_POST){
    $road_name = $_POST['road_name'];
    $direction = $_POST['direction'];
    $postmile  = $_POST['postmile'];
    $day       = $_POST['day'];
    $ppost     = explode("-", $postmile);
    $postmile  = $ppost[0];
      
    $db  =  pg_connect( "$host $port $dbname $credentials" ); 
    if(!$db){
      echo "Error : Unable to open database\n";
    } 
    $sql = "SELECT realtime_pattern from ss_arterial_pattern WHERE road_name='$road_name' AND direction='$direction' AND from_postmile=$postmile AND day= '$day'";
      
    $ret = pg_query($db, $sql);
    if(!$ret){
      echo pg_last_error($db);
      exit;
    } 
    while($row = pg_fetch_row($ret)){
      $ex_row1 = substr($row[0], 1, strlen($row[0])-2);
      $realtime_pattern = explode(",", $ex_row1);
      foreach ($realtime_pattern as $key => $value) {
        if($value=="NULL")
          $realtime_pattern[$key] = null;
      }
    /*
      $ex_row2 = substr($row[6], 1, strlen($row[6])-2);
      $historical_pattern = explode(",", $ex_row2);//pattern2
      $ex_row3 = substr($row[7], 1, strlen($row[7])-2);
      $si = explode(",", $ex_row3);//similarity
      
      foreach ($historical_pattern as $key => $value) {
        if($value=="NULL")
          $historical_pattern[$key] = null;
      }*/
    }
  pg_close($db);  
  }
?>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<link href="phpwork.css" rel="stylesheet" type="text/css" media="screen" />
</head>
<body >
<div class="div_cen">
<form name='getinfo' method='post'> 
  <div class="styled-select">
    ROAD_NAME: 
    <select name='road_name'>
      <?php
        foreach ($roads as $key => $value) {
          if($road_name==$value){
            echo "<option value='$value' selected='selected'>$value</option>";
          }
          else{
            echo "<option value='$value'>$value</option>";
          }
        }
      ?>
    </select>
    &nbsp;
    DIRECTION: 
    <select name='direction'>
    </select>
    &nbsp;
    POSTMILE: 
    <select name='postmile'>
    </select>
    &nbsp;
    DAY: 
    <select name='day'>
      <option value ="Monday" <?php if($day=='Monday'){echo "selected=true";}?>>Monday</option>
      <option value="Tuesday" <?php if($day=='Tuesday'){echo "selected=true";}?>>Tuesday</option>
      <option value="Wednesday" <?php if($day=='Wednesday'){echo "selected=true";}?>>Wednesday</option>
      <option value ="Thursday" <?php if($day=='Thursday'){echo "selected=true";}?>>Thursday</option>
      <option value ="Friday" <?php if($day=='Friday'){echo "selected=true";}?>>Friday</option>
      <option value="Saturday" <?php if($day=='Saturday'){echo "selected=true";}?>>Saturday</option>
      <option value ="Sunday" <?php if($day=='Sunday'){echo "selected=true";}?>>Sunday</option>
    </select>
    &nbsp;
    <input type='submit' value='Submit'/>
  </div>
</form>
</div>
</br>
<table border='1' align='center' >
  <tr>
    <th>Similarity</th>
    <th>Morning(6-9)</th>
    <th>Noon(9-15)</th>
    <th>Afternoon(15-18)</th>
    <th>Evening(18-21)</th>
    <th>Average</th>
  </tr>
  <tr>
    <?php
      echo "<td>Realtime&Production</td><td>".$si[1]."</td><td>".$si[2]."</td><td>".$si[3]."</td><td>".$si[4]."</td><td>".$si[0]."</td>";
    ?>
  </tr>
</table>
</br>
<table align='center'>
  <tr>
    <th>Realtime_pattern: </th>
    <th><hr width=50 noshade color="#0000FF"></th>
    <th>&nbsp;Historical_pattern: </th>
    <th><hr width=50 noshade color="#00FF00"></th>
  </tr>
</table>
<canvas class="can_cen" id="myChart" width="1050" height="530"></canvas>
</br>
</body>
<script src="Chart.js"></script>
<script src="Chart.min.js"></script>
<script src="jquery-2.1.4.js"></script>
<script type="text/javascript">
$(function(){
    var road_name = new Array();

road_name['AVIATION BLVD'] = [] 
road_name['AVIATION BLVD']['NORTHBOUND']=[0,1,2,3,4,5,6];
road_name['AVIATION BLVD']['SOUTHBOUND']=[0,1,2,3,4,5,6];
road_name['FIGUEROA ST'] = [] 
road_name['FIGUEROA ST']['NORTHBOUND']=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18];
road_name['FIGUEROA ST']['SOUTHBOUND']=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18];
road_name['GRAND AVE'] = [] 
road_name['GRAND AVE']['NORTHBOUND']=[0,1,2,3,4,5,6,7,8,9,10];
road_name['GRAND AVE']['SOUTHBOUND']=[0,1,2,3,4,5,6,7,8,9,10,11,12];
road_name['LA CIENEGA AVE'] = [] 
road_name['LA CIENEGA AVE']['NORTHBOUND']=[0];
road_name['LA CIENEGA AVE']['SOUTHBOUND']=[0];
road_name['LA CIENEGA BLVD'] = [] 
road_name['LA CIENEGA BLVD']['NORTHBOUND']=[0,1,2,3];
road_name['LA CIENEGA BLVD']['SOUTHBOUND']=[0,1,2,3];
road_name['MARTIN LUTHER KING BLVD'] = [] 
road_name['MARTIN LUTHER KING BLVD']['EASTBOUND']=[0,1];
road_name['MARTIN LUTHER KING BLVD']['WESTBOUND']=[0,1];
road_name['S HOOVER ST'] = [] 
road_name['S HOOVER ST']['NORTHBOUND']=[0,1,2];
road_name['S HOOVER ST']['SOUTHBOUND']=[0,1,2];



  sero =  $("select[name='road_name']").val();
  $("select[name='direction']").empty();
  for(var i in road_name[sero]){
    if(road_name[sero][i]){
      var tempdir = "<?php echo $direction?>";
      if(i==tempdir){
        var option = "<option value='"+i+"'+ selected='selected'>"+i+"</option>";
      }
      else{
        var option = "<option value='"+i+"'>"+i+"</option>";
      }
      $("select[name='direction']").append(option);  
    }
  }

  var temppo = <?php echo $postmile?>;
  var sedir = $("select[name='direction']").val();
  $("select[name='postmile']").empty();

  for(var i in road_name[sero][sedir]){
    var temp = road_name[sero][sedir][i] + '-' + (road_name[sero][sedir][i]+1);
    if(temppo==road_name[sero][sedir][i]){
      var option = "<option value='"+temp+"'+ selected='selected'>"+temp+"</option>"; 
    }
    else{ 
      var option = "<option value='"+temp+"'>"+temp+"</option>";  
    } 
    $("select[name='postmile']").append(option);  
  } 


  $("select[name='road_name']").change(function() {  
  var selected_value = $(this).val();  
  var dir =  $("select[name='direction']").val();    
  $("select[name='direction']").empty();
  for(var i in road_name[selected_value]){
    var flag = i;
    if(road_name[selected_value][i]){
      var option = "<option value='"+i+"'>"+i+"</option>";
      $("select[name='direction']").append(option);  
      $("select[name='postmile']").empty();
      for(var k in road_name[selected_value][flag]){
        var temp = road_name[selected_value][flag][k] + '-' + (road_name[selected_value][flag][k]+1);
        var option = $("<option>").val(temp).text(temp); 
        $("select[name='postmile']").append(option);  
    }  
    }
  }                  
  });  
  //
  
  $("select[name='direction']").change(function() {  
    var dir = $(this).val();   
    var ro  = $("select[name='road_name']").val();  
    $("select[name='postmile']").empty();
    for(var i in road_name[ro][dir]){
      var temp = road_name[ro][dir][i] + '-' + (road_name[ro][dir][i]+1);
      var option = $("<option>").val(temp).text(temp); 
      $("select[name='postmile']").append(option);  
    }                
  });  

  var  realtime_pattern = <?php echo json_encode($realtime_pattern) ?>;
  var  historical_pattern = <?php echo json_encode($historical_pattern) ?>;;
  var ctx = document.getElementById("myChart").getContext("2d");  
    var data = {
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
        data : realtime_pattern
      }	
     /*,{
        fillColor : "rgba(255,255,255,0)",
        strokeColor : "rgba(0,255,0,1)",
        pointColor : "rgba(34,140,34,1)",
	pointStrokeColor : "#fff",
        data : historical_pattern
      }*/
      ]
    }
  if(realtime_pattern || historical_pattern){
     var myNewChart = new Chart(ctx).Line(data,{
                scaleOverride :true ,   
                scaleSteps : 10,        
                scaleStepWidth : 5,   
                scaleStartValue : 0,   
                pointDot : true,        
                pointDotRadius : 5,     
                pointDotStrokeWidth : 1,
                datasetStrokeWidth : 3,
                animation : true,      
                animationSteps : 60    
                } );
  }
      }); 

</script>
