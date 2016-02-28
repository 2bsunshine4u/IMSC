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
		if (strpos($road, '\'')) {
			$road = str_replace('\'', '\'\'', $road);
		}
		if (strpos($address, '\'')) {
			$address = str_replace('\'', '\'\'', $address);
		}

		$sql = "";

		if ($segment_id == "start"){
			//$sql = "update $geocode_table set road = 'none', check_flag = 'f' where road is null";
		}
		else {				
			if ($road != ""){
				$sql = "update $geocode_table set road = '$road', address = '$address', step_road = '$road', check_flag = 't' where segment_id = $segment_id";
			}
			else{
				$sql = "update $geocode_table set road = '$road', address = '$address', check_flag = 'f' where segment_id = $segment_id";
			}
		}
		if ($sql != ""){
			$stid = oci_parse($db, $sql);
			$ret = oci_execute($stid);
		}

		$sql = "select distinct segment_id, start_lon, start_lat, end_lon, end_lat from $config_table where segment_id in (select distinct segment_id from $geocode_table where check_flag = 'f' and road is not null)";
		$stid = oci_parse($db, $sql);
		$ret = oci_execute($stid);
		$row = oci_fetch_row($stid);

        echo json_encode($row);

    else:
?>
<html>
<body>
	<p> </p>
</body>
</html>

<script src="https://code.jquery.com/jquery.min.js"></script>
<script type="text/javascript">
	$(function(){
		var geocoder = new google.maps.Geocoder;
		var directionsService = new google.maps.DirectionsService;
		writeDB(geocoder, directionsService, "start", "", "");
	});

	function writeDB(geocoder, directionsService, segmentid, road, address){
		$.post("<?php echo $_SERVER['PHP_SELF']; ?>", {segment_id: segmentid, road: road, address: address}, function(data) {
			//document.write(data);
			if (data == false){
				window.location.reload();
			}
			var segment_id = data[0];
			var origin_latlng = {lat: parseFloat(data[2]), lng: parseFloat(data[1])};
			var dest_latlng = {lat: parseFloat(data[4]), lng: parseFloat(data[3])};
			if (data){
				setTimeout(function(){
					directionsService.route({
					    origin: origin_latlng,
					    destination: dest_latlng,
					    travelMode: google.maps.TravelMode.DRIVING
					}, function(response, status) {
						    if (status === google.maps.DirectionsStatus.OK) {
						    	var steps = response.routes[0].overview_path;
						    	setTimeout(function(){geocode(geocoder, directionsService, steps, 0, [], segment_id)}, 1000);
						    }
						    else {
						      	window.alert('Directions request failed due to ' + status +' '+ data[2] +', '+ data[1] +'   '+ data[4] +', ' +data[3]);
						      	window.location.reload();
						    }
						}
					);
				}, 1500);
			}
			else {
				alert("no data returned from backend!");
			}
		}, "json");
	}

	function geocode(geocoder, directionsService, steps, idx, addr_list, segment_id){
		if (idx >= steps.length){
			writeDB(geocoder, directionsService, segment_id, "", "");
			$('p').after("no same road on segment: "+segment_id+"<br />");
		}
		else{
			geocoder.geocode({'location': steps[idx]}, function(results, status) {
				if (status === google.maps.GeocoderStatus.OK) {
	      			if (results[0]) {
	      				var address = results[0].formatted_address.split(",")[0];
	      				console.log(results[0].formatted_address);
	      				var road = address.replace(/^\d+([A-Z]?)(-\d+)?(\s+\d\/\d)?\s+/g, '');

	      				for (var i = 0; i < addr_list.length; i++){
	      					if (remove_prefix(road) == remove_prefix(addr_list[i])){
	      						$('p').after(segment_id+':\t'+road + ',\t' +addr_list[i]+'<br />');
	      						writeDB(geocoder, directionsService, segment_id, addr_list[i], results[0].formatted_address);
	      						return 0;
	      					}
	      				}

	      				addr_list.push(road);
	      				setTimeout(function(){geocode(geocoder, directionsService, steps, idx+2, addr_list, segment_id)}, 1000);
					}
					else {
	        			alert('No results found');
	        			setTimeout(function(){geocode(geocoder, directionsService, steps, idx+1, addr_list, segment_id)}, 1000);
	      			}
	    		} else {
	      			document.write('Geocoder failed due to: ' + status + '<br />');
	      			window.location.reload();
	    		}
			});
		}
	}

	function remove_prefix(addr){
		addr = $.trim(addr);

        if ((addr[0] == 'E' || addr[0] == 'W' || addr[0] == 'S' || addr[0] == 'N') && addr[1] == ' ')
            return addr.slice(2);
        else
            return addr;
	}

	
</script>
<script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyB86aHuCKkWMlKvTK5hmFNqAez9utzRGlA"></script>


<?php endif; ?>