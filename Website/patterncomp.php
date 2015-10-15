<?php
        echo "<h1 style = 'text-align:center'>Pattern Comparison</h1>";	
	$host        = "host=osm-workspace-2.cfmyklmn07yu.us-west-2.rds.amazonaws.com";
  $port        = "port=5432";
  $dbname      = "dbname=osm";
  $credentials = "user=ds password=ds2015";
  $roads       = array(605,215,126,210,133,110,134,118,170,22,23,47,405,2,5,60,710,73,71,91,90,101,105,10,15,14,33,57,55,241);
  sort($roads);
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
    if($direction=='North'){
        $direction = 0;
      }
      else if($direction=='South'){
        $direction = 1;
      }
      else if($direction=='East'){
        $direction = 2;
      }
      else if($direction=='West'){
        $direction = 3;
    }
    $db  =  pg_connect( "$host $port $dbname $credentials" ); 
    if(!$db){
      echo "Error : Unable to open database\n";
    } 
    $sql = 'SELECT * from ' .'"'.'SS_SECTION_PATTERN_ALL'.'"'.' WHERE road_name='."'".$road_name."'".' AND direction='.
            $direction.' AND from_postmile='.$postmile.' AND day='."'".$day."'";//注意空格和单双引号
    /*$sql =<<<EOF
  SELECT * from "SS_SECTION_PATTERN" WHERE road_name = '2' ;
EOF;*/
    $ret = pg_query($db, $sql);
    if(!$ret){
      echo pg_last_error($db);
      exit;
    } 
    while($row = pg_fetch_row($ret)){
      $ex_row1 = substr($row[5], 1, strlen($row[5])-2);
      $realtime_pattern = explode(",", $ex_row1);//pattern1
      $ex_row2 = substr($row[6], 1, strlen($row[6])-2);
      $historical_pattern = explode(",", $ex_row2);//pattern2
      $ex_row3 = substr($row[7], 1, strlen($row[7])-2);
      $si = explode(",", $ex_row3);//similarity
      foreach ($realtime_pattern as $key => $value) {//数据库中读出"NULL",但是js识别需要"null"
        if($value=="NULL")
          $realtime_pattern[$key] = null;
      }
      foreach ($historical_pattern as $key => $value) {
        if($value=="NULL")
          $historical_pattern[$key] = null;
      }
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
            echo "<option value=$value selected='selected'>$value</option>";
          }
          else{
            echo "<option value=$value>$value</option>";
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
    <th><hr width=50 noshade color="#FF0000"></th>
  </tr>
</table>
<canvas class="can_cen" id="myChart" width="1050" height="530"></canvas>
</br>
</body>
<script src="Chart.js"></script>
<script src="Chart.min.js"></script>
<script src="jquery-2.1.4.js"></script>
<script type="text/javascript">
$(function(){  //js中方法
    var road_name = new Array();//声明三维数组
    for(var k=0;k<1000;k++){
      road_name[k] = new Array();
        for(var m=0;m<4;m++){
          road_name[k][m] = new Array();
          road_name[k][m] = null;
        }
    }
  //存放在数组中的下拉菜单内容，可放在单独文件中
  road_name[605][0]=[0,3,6,9,12,15,18,21,24];   
  road_name[605][1]=[0,3,6,9,12,15,18,21,24];         
  road_name[215][0]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45]
  road_name[215][1]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45]
  road_name[126][2]=[0,3,6,9,12,39]
  road_name[126][3]=[0,27,30,33,36,39]
  road_name[210][2]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,78,84,90]
  road_name[210][3]=[0,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81]
  road_name[133][0]=[0,3]
  road_name[133][1]=[0,3]
  road_name[110][0]=[0,3,6,9,12,15,18,21,24,27]
  road_name[110][1]=[0,3,6,9,12,15,18,21,24,27]
  road_name[134][2]=[0,3,6,9,12]
  road_name[134][3]=[0,3,6,9,12]
  road_name[118][2]=[0,3,6,9,12,15,18,21,24]
  road_name[118][3]=[0,3,6,9,12,15,18,21,24]
  road_name[170][0]=[0,3,6]
  road_name[170][1]=[0,3,6]
  road_name[22][2]=[0,3,6,9,12,15,18]
  road_name[22][3]=[0,3,6,9,12,15]
  road_name[23][0]=[0,3,6]
  road_name[23][1]=[0,3,6]
  road_name[47][0]=[0,3]
  road_name[47][1]=[0,3]
  road_name[405][0]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78]
  road_name[405][1]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75]
  road_name[2][2]=[0,3,6,9]
  road_name[2][3]=[0,6,9,12]
  road_name[5][0]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81,84,87,90,93,96,99,102,105]
  road_name[5][1]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81,84,87,90,93,96,99,102,105,108]
  road_name[60][2]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,54,57,60,63,66]
  road_name[60][3]=[0,3,6,9,12,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66]
  road_name[710][0]=[0,3,6,9,12,15,18,21,27]
  road_name[710][1]=[0,6,9,12,15,18,21,24,27]
  road_name[73][0]=[0,3,6,9,12,15]
  road_name[73][1]=[0,3,6,9,12,15]
  road_name[71][0]=[0,3,6,9,12,15]
  road_name[71][1]=[0,3,6,9,12]
  road_name[91][2]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54]
  road_name[91][3]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51]
  road_name[90][2]=[0,36]
  road_name[90][3]=[0,36,39]
  road_name[101][0]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69]
  road_name[101][2]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69]
  road_name[105][2]=[0,3,6,9,12,15]
  road_name[105][3]=[0,3,6,9,12,15]
  road_name[10][2]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81,84,87,90,93,96]
  road_name[10][3]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81,84,87,90,93]
  road_name[15][0]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81,84,87,90,93]
  road_name[15][1]=[0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63,66,69,72,75,78,81,84,87,90]
  road_name[14][1]=[0,3,6,9,12,15,18,21,24,27]
  road_name[14][2]=[0,3,6,9,12,15,18,21,24,27]
  road_name[33][0]=[0,3]
  road_name[33][1]=[0,3]
  road_name[57][0]=[0,3,6,9,12,15,18,21]
  road_name[57][1]=[0,3,6,9,12,15,18,21]
  road_name[55][0]=[0,3,6,9,12]
  road_name[55][1]=[0,3,6,9,12]
  road_name[241][0]=[0,3,6,9,12,15,18]
  road_name[241][1]=[0,3,6,9,12,15,18,21]
  //每次加载
  sero =  $("select[name='road_name']").val();
  $("select[name='direction']").empty();
  for(var i in road_name[sero]){
    if(road_name[sero][i]){
      var tempdir = <?php echo $direction?>;
      if(i==tempdir){//提交时选中的菜单项要显示出来
        if(i==0){
          i = 'North';
        }
        else if(i==1){
          i = 'South';
        }
        else if(i==2){
          i = 'East';
        }
        else if(i==3){
          i = 'West';
        }
        var option = "<option value='"+i+"'+ selected='selected'>"+i+"</option>";
      }
      else{
        if(i==0){
          i = 'North';
        }
        else if(i==1){
          i = 'South';  
        }
        else if(i==2){
          i = 'East';
        }
        else if(i==3){
          i = 'West';
        }          
        var option = "<option value='"+i+"'>"+i+"</option>";
      }
      $("select[name='direction']").append(option);  
    }
  }

  var temppo = <?php echo $postmile?>;
  var sedir = $("select[name='direction']").val();
  $("select[name='postmile']").empty();
  if(sedir=='North'){
    sedir = 0;
  }
  else if(sedir=='South'){
    sedir = 1;
  } 
  else if(sedir=='East'){
    sedir = 2;
  }
  else if(sedir=='West'){
    sedir = 3;
  }
  for(var i in road_name[sero][sedir]){
    var temp = road_name[sero][sedir][i] + '-' + (road_name[sero][sedir][i]+3);
    if(temppo==road_name[sero][sedir][i]){
      var option = "<option value='"+temp+"'+ selected='selected'>"+temp+"</option>"; 
    }
    else{ 
      var option = "<option value='"+temp+"'>"+temp+"</option>";  
    } 
    $("select[name='postmile']").append(option);  
  } 

  //每次更改road_name菜单时加载direction菜单
  $("select[name='road_name']").change(function() {  
  var selected_value = $(this).val();  
  var dir =  $("select[name='direction']").val();    
  $("select[name='direction']").empty();
  for(var i in road_name[selected_value]){
    var flag = i;
    if(road_name[selected_value][i]){
      if(i==0){
        i = 'North';
      }
      else if(i==1){
        i = 'South';
      }
      else if(i==2){
        i = 'East';
      }
      else if(i==3){
        i = 'West';
      }
      var option = "<option value='"+i+"'>"+i+"</option>";
      $("select[name='direction']").append(option);  
      $("select[name='postmile']").empty();
      for(var k in road_name[selected_value][flag]){
        var temp = road_name[selected_value][flag][k] + '-' + (road_name[selected_value][flag][k]+3);
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
    //被选中的option   
    $("select[name='postmile']").empty();
    if(dir=='North'){
      dir = 0;
    }
    else if(dir=='South'){
     dir = 1;
    }
    else if(dir=='East'){
      dir = 2;
    }
    else if(dir=='West'){
      dir = 3;
    }
    for(var i in road_name[ro][dir]){
      var temp = road_name[ro][dir][i] + '-' + (road_name[ro][dir][i]+3);
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
     ,{
        fillColor : "rgba(255,255,255,0)",
        strokeColor : "rgba(255,0,0,1)",
        pointColor : "rgba(255,20,147,1)",
        pointStrokeColor : "#fff",
        data : historical_pattern
      },
      ]
    }
  if(realtime_pattern || historical_pattern){
     var myNewChart = new Chart(ctx).Line(data,{
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
  }
      }); 

</script>
