<?php 
	$host = "gd.usc.edu";
    $sid = "ADMS";
    $username = "shuai";
    $password = "shuai2015pass";

    $config_table = "inrix_section_config";
    $geocode_table = "reverse_geocode";

    $db  =  oci_connect($username,$password,"$host/$sid");
    if(!$db){
      echo "Error : Unable to open database\n";
    } 

	if(!empty($_POST['segment_id'])):
		$segment_id = $_POST['segment_id'];
		$road = $_POST['road'];
		$address = $_POST['address'];
		$rn = $_POST['rn'];

		$sql = "";

		if ($segment_id == "truncate"){
			if ($rn == 0){
				$sql = "truncate table $geocode_table";
			}
		}
		else {
			$sql = "select count(*) from $geocode_table where segment_id = $segment_id";
			$stid = oci_parse($db, $sql);
			$ret = oci_execute($stid);
			$row = oci_fetch_row($stid);
			if ($row[0] == 0){
				$sql = "insert into $geocode_table (segment_id, road, address, rn) values ($segment_id, '$road', '$address', $rn)";
			}
			else {
				$sql = "";
			}
		}
		if ($sql != ""){
			$stid = oci_parse($db, $sql);
			$ret = oci_execute($stid);
		}

		$rn += 1;
		$sql = "select * from (select segment_id, start_lon, end_lon, start_lat, end_lat, rownum as rn from $config_table) where rn = $rn";
		$stid = oci_parse($db, $sql);
		$ret = oci_execute($stid);
		$row = oci_fetch_row($stid);
        $lon = ($row[1] + $row[2]) / 2.0;
        $lat = ($row[3] + $row[4]) / 2.0;
        echo json_encode(array($row[0], $lon, $lat, $row[5]));
	    


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
		writeDB(geocoder, "truncate", "", "", <?php echo $max_rn; ?>);
	});

	function writeDB(geocoder, segment_id, road, address, rownum){
		$.post("<?php echo $_SERVER['PHP_SELF']; ?>", {segment_id: segment_id, road: road, address: address, rn: rownum}, function(data) {
			console.log(data);
			var latlng = {lat: data[2], lng: data[1]};
			var rn = data[3];
			$("p").after("segment_id: "+segment_id+"        road: "+road+"          rn: "+rownum+"<br />");
			if (data){
				setTimeout(function(){geocode(geocoder, latlng, data[0], rn);}, 2000);
			}
			else {
				alert("no data returned from backend!");
			}
		}, "json");
	}

	function geocode(geocoder, latlng, segment_id, rn){
		geocoder.geocode({'location': latlng}, function(results, status) {
			if (status === google.maps.GeocoderStatus.OK) {
      			if (results[0]) {
      				var address = results[0].formatted_address.split(",")[0];
      				console.log(results[0].formatted_address);
      				road = address.replace(/^\d+(-\d+)?\s/g, '');
      				writeDB(geocoder, segment_id, road, results[0].formatted_address, rn);
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