import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    
    Returns:
        Distance in meters
    """
    # Convert decimal degrees to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Earth radius in meters
    r = 6371000
    
    return c * r


def calculate_bearing(lat1, lon1, lat2, lon2):
    """
    Calculate the bearing from point 1 to point 2
    
    Returns:
        Bearing in radians
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    
    x = math.sin(dlon) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)
    
    return math.atan2(x, y)


def destination_point(lat, lon, bearing, distance):
    """
    Calculate destination point given start point, bearing, and distance
    
    Args:
        lat: Starting latitude in degrees
        lon: Starting longitude in degrees
        bearing: Bearing in radians
        distance: Distance in meters
        
    Returns:
        (lat, lon) tuple of destination point in degrees
    """
    # Earth radius in meters
    r = 6371000
    
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    
    angular_distance = distance / r
    
    lat2 = math.asin(
        math.sin(lat_rad) * math.cos(angular_distance) +
        math.cos(lat_rad) * math.sin(angular_distance) * math.cos(bearing)
    )
    
    lon2 = lon_rad + math.atan2(
        math.sin(bearing) * math.sin(angular_distance) * math.cos(lat_rad),
        math.cos(angular_distance) - math.sin(lat_rad) * math.sin(lat2)
    )
    
    return math.degrees(lat2), math.degrees(lon2)