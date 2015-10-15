import math 
import random 

def extract_loc_from_geometry(geometry):
    idx1 = geometry.index('(')
    idx2 = geometry.index(')')
    loc = geometry[idx1+1:idx2]
    loc = loc.split(' ')
    if loc[0].find('.') < 0:
        loc[0] = loc[0][:4]+'.'+loc[0][4:]
    if loc[1].find('.') < 0:
        loc[1] = loc[1][:2]+'.'+loc[1][2:]
    loc[0] = (float)(loc[0])
    loc[1] = (float)(loc[1])
    return loc

def map_dist(lon1, lat1, lon2, lat2):
    #distance between 2 points on sphere surface, in meter
    if lon1 == lon2 and lat1 == lat2:
        return 0
    else:
        try:
            return 6378137*math.acos(math.sin(math.radians(lat1))*math.sin(math.radians(lat2))+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.cos(math.radians(lon2-lon1)))
        except Exception,ex:
            print Exception,":",ex
            return 1000000
        
def bearing(Longitude1, Latitude1, Longitude2, Latitude2):
    Longitude1, Latitude1, Longitude2, Latitude2 = map(math.radians, [Longitude1, Latitude1, Longitude2, Latitude2])
    x = math.sin(Longitude2 - Longitude1) * math.cos(Latitude2)
    y = math.cos(Latitude1) * math.sin(Latitude2) - math.sin(Latitude1) * math.cos(Latitude2) * math.cos(Longitude2 - Longitude1)
    heading = math.atan2(x, y) * 180 / 3.14159265
    if heading <0:
        heading += 360
    
    return heading

def point2line(lon, lat, lon1, lat1, lon2, lat2):
    bearing0 = bearing(lon1, lat1, lon2, lat2)
    bearing1 = bearing(lon1, lat1, lon, lat)
    bearing2 = bearing(lon, lat, lon2, lat2)
    angle1 = abs(bearing0 - bearing1)
    angle2 = abs(bearing0 - bearing2)
    if angle1 > 90 and angle1 < 270:
        dist = map_dist(lon1, lat1, lon, lat)
    elif angle2 > 90 and angle2 < 270:
        dist = map_dist(lon2, lat2, lon, lat)
    else:
        a1 = math.radians(angle1)
        dist = map_dist(lon1, lat1, lon, lat) * abs( math.sin(a1))
    
    return dist

def is_in_bbox(lon1, lat1, lon2, lat2, lon, lat):
    mlon = min(lon1, lon2)
    mlat = min(lat1, lat2)
    xlon = max(lon1, lon2)
    xlat = max(lat1, lat2)
    b = bearing(lon1, lat1, lon2, lat2)
    if b < 45 or (b >= 135 and b < 225) or b > 315:
        return lat >= mlat and lat <= xlat
    else:
        return lon >= mlon and lon <= xlon
    
def list_to_str(l):
    s = "'{"
    for i in range(0, len(l)):
        if i == 0:
            if not l[i] == 0:
                s += str(l[i])
            else:
                s += "null"
        else:
            if not l[i] == 0:
                s += "," + str(l[i])
            else:
                s += ", null"
    s += "}'"
    
    return s
            

