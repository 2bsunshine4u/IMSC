<?php 
	$host = "gd.usc.edu";
    $sid = "ADMS";
    $username = "shuai";
    $password = "shuai2015pass";

    $config_table = "inrix_section_config";
    $geocode_table = "reverse_geocode";

    $db  =  oci_connect($username,$password,"$host/$sid");
    if(!$db){
      echo json_encode(false);
      return false;
    } 

	if(!empty($_POST['segment_id'])):
		$segment_id = $_POST['segment_id'];
		$road = $_POST['road'];
		$address = $_POST['address'];
		$start_road = $_POST['start_road'];
		$end_road = $_POST['end_road'];
		$mid_road = $_POST['mid_road'];
		if (strpos($road, '\'')) {
			$road = str_replace('\'', '\'\'', $road);
		}
		if (strpos($address, '\'')) {
			$address = str_replace('\'', '\'\'', $address);
		}
		if (strpos($start_road, '\'')) {
			$start_road = str_replace('\'', '\'\'', $start_road);
		}
		if (strpos($end_road, '\'')) {
			$end_road = str_replace('\'', '\'\'', $end_road);
		}
		if (strpos($mid_road, '\'')) {
			$mid_road = str_replace('\'', '\'\'', $mid_road);
		}
		$rn = $_POST['rn'];

		$sql = "";

		if ($segment_id == "truncate"){
				//$sql = "truncate table $geocode_table";
		}
		else {				
			$sql = "
				declare t_cols number;
				begin
					select segment_id into t_cols from $geocode_table where rn = $rn;
				exception 
				when no_data_found then begin 
					insert into $geocode_table (segment_id, road, address, rn, start_road, end_road, mid_road) values ($segment_id, '$road', '$address', $rn, '$start_road', '$end_road', '$mid_road');	
				end;
				when others then  begin
					null;
				end;
				end;
			";
		}
		if ($sql != ""){
			$stid = oci_parse($db, $sql);
			if(($ret = oci_execute($stid)) == false){
				echo json_encode(false);
      			return false;
			}
		}

		$rn += 1;
		$sql = "select * from (select segment_id, start_lon, end_lon, start_lat, end_lat, rownum as rn from $config_table) where rn = $rn";
		$stid = oci_parse($db, $sql);
		$ret = oci_execute($stid);
		$row = oci_fetch_row($stid);
		$segment_id = $row[0];
		$start_lon = $row[1];
		$end_lon = $row[2];
		$start_lat = $row[3];
		$end_lat = $row[4];

        echo json_encode(array($row[0], $start_lon, $start_lat, $end_lon, $end_lat, $row[5]));

	else:
		$sql = "select max(rn) from $geocode_table";
		$stid = oci_parse($db, $sql);
		$ret = oci_execute($stid);
		$row = oci_fetch_row($stid);
		if ($row[0] == ''){
			$max_rn = 0;
		}
		else{
			$max_rn = $row[0];
		}
?>
<html>
<body>
	<p />
</body>
</html>

<script src="https://code.jquery.com/jquery.min.js"></script>
<script type="text/javascript">
	$(function(){
		var geocoder = new google.maps.Geocoder;
		$("p").after("start with rn: <?php echo $max_rn; ?>");
		writeDB(geocoder, "truncate", "", "", <?php echo $max_rn; ?>, "", "", "");
	});

	function writeDB(geocoder, segmentid, road, address, rownum, start_road, end_road, mid_road){
		$.post("<?php echo $_SERVER['PHP_SELF']; ?>", {segment_id: segmentid, road: road, address: address, rn: rownum, start_road: start_road, end_road: end_road, mid_road: mid_road}, function(data) {
			//document.write(data);
			if (data == false){
				window.location.reload();
			}
			var segment_id = data[0];
			var start_latlng = {lat: parseFloat(data[2]), lng: parseFloat(data[1])};
			var end_latlng = {lat: parseFloat(data[4]), lng: parseFloat(data[3])};
			var mid_latlng = {lat: (parseFloat(data[2])+parseFloat(data[4]))/2.0, lng: (parseFloat(data[1])+parseFloat(data[3]))/2.0};
			var rn = data[5];
			if (data){
				setTimeout(function(){
					geocode0(geocoder, start_latlng, end_latlng, mid_latlng, segment_id, rn);
				}, 1500);
			}
			else {
				alert("no data returned from backend!");
			}
		}, "json");
	}

	function geocode0(geocoder, start_latlng, end_latlng, mid_latlng, segment_id, rn){
		geocoder.geocode({'location': start_latlng}, function(results, status) {
			if (status === google.maps.GeocoderStatus.OK) {
      			if (results[0]) {
      				var start_address = results[0].formatted_address.split(",")[0];
      				console.log(results[0].formatted_address, rn);
      				var start_road = start_address.replace(/^\d+([A-Z]?)(-\d+)?(\s+\d\/\d)?\s+/g, '');

      				setTimeout(function(){geocode1(geocoder, start_road, results[0].formatted_address, end_latlng, mid_latlng, segment_id, rn);}, 1000);
				}
				else {
        			alert('No results found');
      			}
    		} else {
      			document.write('Geocoder failed due to: ' + status + '<br />');
      			window.location.reload();
    		}
		});
	}

	function geocode1(geocoder, start_road, start_address, end_latlng, mid_latlng, segment_id, rn){
		geocoder.geocode({'location': end_latlng}, function(results, status) {
			if (status === google.maps.GeocoderStatus.OK) {
      			if (results[0]) {
      				var end_address = results[0].formatted_address.split(",")[0];
      				console.log(results[0].formatted_address, rn);
      				var end_road = end_address.replace(/^\d+([A-Z]?)(-\d+)?(\s+\d\/\d)?\s+/g, '');

      				if (start_road == end_road){
      					$("p").after("segment_id: "+segment_id+"        start_road: "+start_road+"          rn: "+rn+" <br />");
						writeDB(geocoder, segment_id, start_road, results[0].formatted_address, rn, start_road, end_road, "");
      				}
      				else{
      					$("p").after("segment_id: "+segment_id+"        start_road: "+start_road+"  end_road:"+end_road+" rn: "+rn+" <br />");
      					setTimeout(function(){geocode2(geocoder, mid_latlng, segment_id, rn, start_road, end_road);}, 1000);
      				}
				}else {
        			alert('No results found');
      			}
    		} else {
      			document.write('Geocoder failed due to: ' + status + '<br />');
      			window.location.reload();
    		}
		});
	}

	function geocode2(geocoder, mid_latlng, segment_id, rn, start_road, end_road){
		geocoder.geocode({'location': mid_latlng}, function(results, status) {
			if (status === google.maps.GeocoderStatus.OK) {
      			if (results[0]) {
      				var mid_address = results[0].formatted_address.split(",")[0];
      				console.log(results[0].formatted_address, rn);
      				var mid_road = mid_address.replace(/^\d+([A-Z]?)(-\d+)?(\s+\d\/\d)?\s+/g, '');

      				$("p").after("segment_id: "+segment_id+"        mid_road: "+mid_road+"          rn: "+rn+"<br />");
					writeDB(geocoder, segment_id, mid_road, results[0].formatted_address, rn, start_road, end_road, mid_road);
				}else {
        			alert('No results found');
      			}
    		} else {
      			document.write('Geocoder failed due to: ' + status + '<br />');
      			window.location.reload();
    		}
		});
	}
</script>
<script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyB86aHuCKkWMlKvTK5hmFNqAez9utzRGlA"></script>


<?php endif; ?>