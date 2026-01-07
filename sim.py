"""
APF (Artificial Potential Field) Simulation for Drone Swarms
Uses real geolocation (latitude, longitude) and Haversine distance calculation.
Drones use DroneNode class and fly_to_target method to reach calculated positions.
"""

import math
import time
import cmd
import os
import socket
import threading
from control import DroneNode, FlightMode
from loguru import logger
from dotenv import load_dotenv
from util import haversine_distance, calculate_bearing


load_dotenv()

# Listening the target position
HOST = "0.0.0.0"
PORT = 65432

ITERATION = 50
UPDATE_INTERVAL = 1.0
PRINT_INTERVAL = 5


class APFDroneSwarm:
    def __init__(self, connection_strings, target_pos, altitude=10.0):
        """
        Initialize APF drone swarm simulation
        
        Args:
            connection_strings: List of MAVLink connection strings for each drone
            target_pos: (latitude, longitude) tuple for target position
            altitude: Flight altitude in meters (default: 10.0m)
        """
        self.target_pos = target_pos  # (lat, lon)
        self.altitude = altitude
        
        # APF parameters (distances in meters)
        self.target_radius = 50.0  # Desired distance from target in meters
        self.repulsion_strength = 100.0  # Repulsive force from target
        self.inter_node_repulsion = 50.0  # Repulsion between drones
        self.attraction_strength = 0.3  # Attraction to circular formation
        self.damping = 0.8  # Velocity damping
        self.max_speed = 10.0  # Max speed in m/s
        self.min_inter_drone_distance = 10.0  # Minimum distance between drones in meters
        
        # Initialize drone nodes
        self.drones = []
        self.velocities = []  # Store velocity for each drone (vx, vy in m/s)
        
        for i, conn_str in enumerate(connection_strings):
            drone = DroneNode(conn_str)
            self.drones.append(drone)
            self.velocities.append({'vx': 0.0, 'vy': 0.0})
            logger.info(f"Created DroneNode {i} with connection: {conn_str}")

        self._stop_server = False
        self.target_server_t = threading.Thread(target=self.target_position_server_thread)
        # FIX 1: Make server thread a daemon so it doesn't block program exit
        self.target_server_t.daemon = True
        self.target_server_t.start()

    def verify_target_data(self, data_str: str) -> bool:
        try:
            if ',' in data_str:
                lat_str, lon_str = data_str.split(',')
            else:
                parts = data_str.split()
                if len(parts) != 2:
                    print(f"Invalid data format: {data_str}. Expected: 'latitude,longitude' or 'latitude longitude'")
                    return False
                lat_str, lon_str = parts
                # Parse and validate coordinates
                lat = float(lat_str.strip())
                lon = float(lon_str.strip())
                
                # Validate latitude range: -90 to 90
                if not (-90 <= lat <= 90):
                    logger.error(f"Invalid latitude: {lat}. Must be between -90 and 90")
                    return False
                
                # Validate longitude range: -180 to 180
                if not (-180 <= lon <= 180):
                    logger.error(f"Invalid longitude: {lon}. Must be between -180 and 180")
                    return False
                
                # Update target position
                self.target_pos = (lat, lon)
                logger.success(f"Target position updated to: ({lat:.6f}, {lon:.6f})")
                return True
        except ValueError as e:
            logger.error(f"Failed to parse coordinates: {data_str}. Error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while parsing target data: {str(e)}")
            return False


    def target_position_server_thread(self):
        """
        Handles the server-side logic for receiving target position updates.
        
        FIX 2: Added socket timeouts and stop flag checking to prevent indefinite blocking.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Allow quick restart of the server
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((HOST, PORT))
                s.listen(1)
                # Set timeout on accept() to avoid indefinite blocking
                s.settimeout(1.0)  # 1 second timeout
                logger.info(f"Server listening on {HOST}:{PORT}")
                
                conn = None
                try:
                    while not self._stop_server:
                        try:
                            # accept() will raise socket.timeout after 1 second
                            conn, addr = s.accept()
                            logger.info(f"Connected by {addr}")
                            break
                        except socket.timeout:
                            # Timeout occurred, loop will check _stop_server and try again
                            continue
                    
                    # If we exited the loop due to _stop_server flag, exit function
                    if self._stop_server or conn is None:
                        logger.info("Server shutdown requested")
                        return
                    
                    # Connection established, now receive data
                    with conn:
                        # FIX 2b: Set timeout on recv() as well
                        conn.settimeout(1.0)
                        while not self._stop_server:
                            try:
                                data = conn.recv(1024)
                                if data:
                                    logger.info(f"Server received: {data.decode()}")
                                    self.verify_target_data(data)
                                else:
                                    # Empty data means client closed connection
                                    logger.info("Client closed connection")
                                    break
                            except socket.timeout:
                                # Timeout on recv, check stop flag and continue
                                continue
                except Exception as e:
                    logger.error(f"Error in server accept/receive: {str(e)}")
                finally:
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass
                            
        except Exception as e:
            logger.error(f"Server thread error: {str(e)}")

    
    def connect_all(self):
        """Connect to all drones"""
        success = True
        for i, drone in enumerate(self.drones):
            if drone.connect():
                logger.success(f"Drone {i} connected successfully")
            else:
                logger.error(f"Failed to connect to drone {i}")
                success = False
        return success
    
    def arm_and_takeoff_all(self):
        """Arm and takeoff all drones"""
        for i, drone in enumerate(self.drones):
            # Set to GUIDED mode
            drone.set_flight_mode(FlightMode.GUIDED)
            time.sleep(0.5)
            
            # Takeoff
            if drone.takeoff(self.altitude):
                logger.success(f"Drone {i} takeoff command sent")
            else:
                logger.error(f"Drone {i} takeoff failed")
        
        # Wait for drones to reach altitude
        logger.info(f"Waiting for drones to reach altitude {self.altitude}m...")
        time.sleep(10)
    
    def get_drone_position(self, drone_index):
        """
        Get current position of a drone
        
        Returns:
            (lat, lon) tuple or None if position unavailable
        """
        drone = self.drones[drone_index]
        status = drone.get_drone_status()
        return status.get('position')
    
    def calculate_repulsion_from_target(self, drone_index):
        """
        Calculate repulsive force from target position
        
        Returns:
            (force_lat, force_lon) force components in degrees/iteration
        """
        if self.target_pos is None:
            return 0, 0
        
        pos = self.get_drone_position(drone_index)
        if pos is None:
            return 0, 0
        
        drone_lat, drone_lon = pos
        target_lat, target_lon = self.target_pos
        
        distance = haversine_distance(drone_lat, drone_lon, target_lat, target_lon)
        
        if distance < 0.1:  # Avoid division by zero
            distance = 0.1
        
        # Strong repulsion when close to target
        force_magnitude = self.repulsion_strength / (distance**2)
        
        # Calculate bearing from target to drone (repulsion direction)
        bearing = calculate_bearing(target_lat, target_lon, drone_lat, drone_lon)
        
        # Convert force to lat/lon components
        force_lat = force_magnitude * math.cos(bearing)
        force_lon = force_magnitude * math.sin(bearing)
        
        return force_lat, force_lon
    
    def calculate_attraction_to_circle(self, drone_index):
        """
        Calculate attraction to maintain circular formation around target
        
        Returns:
            (force_lat, force_lon) force components
        """
        if self.target_pos is None:
            return 0, 0
        
        pos = self.get_drone_position(drone_index)
        if pos is None:
            return 0, 0
        
        drone_lat, drone_lon = pos
        target_lat, target_lon = self.target_pos
        
        distance = haversine_distance(drone_lat, drone_lon, target_lat, target_lon)
        
        if distance < 0.1:
            return 0, 0
        
        # Calculate distance error (positive = too far, negative = too close)
        distance_error = distance - self.target_radius
        
        # Attractive force toward the circle
        force_magnitude = self.attraction_strength * distance_error
        
        # Calculate bearing from drone to target (attraction direction when too far)
        bearing = calculate_bearing(drone_lat, drone_lon, target_lat, target_lon)
        
        # Force direction: toward center if too far, away if too close
        force_lat = force_magnitude * math.cos(bearing)
        force_lon = force_magnitude * math.sin(bearing)
        
        return force_lat, force_lon
    
    def calculate_inter_drone_repulsion(self, drone_index):
        """
        Calculate repulsion between drones to maintain spacing
        
        Returns:
            (force_lat, force_lon) force components
        """
        force_lat_total = 0
        force_lon_total = 0
        
        pos = self.get_drone_position(drone_index)
        if pos is None:
            return 0, 0
        
        drone_lat, drone_lon = pos
        
        for i, other_drone in enumerate(self.drones):
            if i == drone_index:
                continue
            
            other_pos = self.get_drone_position(i)
            if other_pos is None:
                continue
            
            other_lat, other_lon = other_pos
            
            distance = haversine_distance(drone_lat, drone_lon, other_lat, other_lon)
            
            # Only calculate repulsion if drones are within repulsion range
            if distance > self.min_inter_drone_distance and distance < 100:  # 100m max range
                # Repulsion magnitude increases as drones get closer
                force_magnitude = self.inter_node_repulsion / (distance**2)
                
                # Calculate bearing from other drone to this drone (repulsion direction)
                bearing = calculate_bearing(other_lat, other_lon, drone_lat, drone_lon)
                
                # Convert to lat/lon components
                force_lat_total += force_magnitude * math.cos(bearing)
                force_lon_total += force_magnitude * math.sin(bearing)
        
        return force_lat_total, force_lon_total
    
    def update_drone_velocity(self, drone_index, fx_total, fy_total):
        """
        Update drone velocity based on forces using physics simulation
        
        Args:
            drone_index: Index of the drone
            fx_total: Total force in latitude direction
            fy_total: Total force in longitude direction
        """
        # Get current velocity
        vx = self.velocities[drone_index]['vx']
        vy = self.velocities[drone_index]['vy']
        
        # Apply damping (friction)
        vx *= self.damping
        vy *= self.damping
        
        # Apply forces (treat as acceleration for this iteration)
        vx += fx_total
        vy += fy_total
        
        # Limit speed
        speed = math.sqrt(vx**2 + vy**2)
        if speed > self.max_speed:
            scaling_factor = self.max_speed / speed
            vx *= scaling_factor
            vy *= scaling_factor
        
        # Store updated velocity
        self.velocities[drone_index]['vx'] = vx
        self.velocities[drone_index]['vy'] = vy
    
    def update_drone_position(self, drone_index):
        """
        Update drone position based on velocity
        
        Args:
            drone_index: Index of the drone to update
        """
        drone = self.drones[drone_index]
        pos = self.get_drone_position(drone_index)
        
        if pos is None:
            return
        
        lat, lon = pos
        vx = self.velocities[drone_index]['vx']
        vy = self.velocities[drone_index]['vy']
        
        # Calculate new position
        # Simplified: treat velocity components as degree/s changes
        new_lat = lat + vx * 0.00001  # Scale down for realistic movement
        new_lon = lon + vy * 0.00001
        
        # Send fly_to command to drone
        drone.fly_to_target(new_lat, new_lon, self.altitude)
    
    def run(self, iterations=ITERATION, update_interval=UPDATE_INTERVAL, print_interval=PRINT_INTERVAL, verbose=False):
        """
        Run APF simulation for specified iterations
        
        Args:
            iterations: Number of iterations to run (use float('inf') for continuous)
            update_interval: Time between updates in seconds
            print_interval: Iterations between status prints
            verbose: Whether to print status each iteration
        """
        iteration = 0
        last_print = 0
        
        try:
            while iteration < iterations:
                # Calculate forces for each drone
                for i in range(len(self.drones)):
                    # Calculate all forces
                    fx_repulsion, fy_repulsion = self.calculate_repulsion_from_target(i)
                    fx_attraction, fy_attraction = self.calculate_attraction_to_circle(i)
                    fx_inter, fy_inter = self.calculate_inter_drone_repulsion(i)
                    
                    # Sum all forces
                    fx_total = fx_repulsion + fx_attraction + fx_inter
                    fy_total = fy_repulsion + fy_attraction + fy_inter
                    
                    # Update velocity and position
                    self.update_drone_velocity(i, fx_total, fy_total)
                    self.update_drone_position(i)
                
                # Print status periodically
                if verbose and iteration - last_print >= print_interval:
                    stats = self.get_swarm_statistics()
                    logger.info(f"Iteration {iteration}: Center=({stats['center_lat']:.6f}, {stats['center_lon']:.6f}), "
                               f"Avg Distance={stats.get('avg_distance_to_target', 0):.2f}m")
                    last_print = iteration
                
                # Sleep for specified interval
                time.sleep(update_interval)
                iteration += 1
                
        except KeyboardInterrupt:
            logger.info("APF simulation interrupted by user")
            raise
    
    def get_swarm_statistics(self):
        """
        Calculate and return swarm statistics
        
        Returns:
            dict: Dictionary containing swarm statistics
        """
        positions = []
        distances_to_target = []
        velocities = []
        
        for i in range(len(self.drones)):
            pos = self.get_drone_position(i)
            if pos:
                positions.append(pos)
                
                # Calculate distance to target
                if self.target_pos:
                    dist = haversine_distance(pos[0], pos[1], self.target_pos[0], self.target_pos[1])
                    distances_to_target.append(dist)
                
                # Calculate velocity magnitude
                vx = self.velocities[i]['vx']
                vy = self.velocities[i]['vy']
                velocity = math.sqrt(vx**2 + vy**2)
                velocities.append(velocity)
        
        # Calculate center position
        if positions:
            center_lat = sum(p[0] for p in positions) / len(positions)
            center_lon = sum(p[1] for p in positions) / len(positions)
        else:
            center_lat = 0
            center_lon = 0
        
        # Compile statistics
        stats = {
            'num_drones': len(self.drones),
            'positions_available': len(positions),
            'center_lat': center_lat,
            'center_lon': center_lon,
        }
        
        if distances_to_target:
            stats['avg_distance_to_target'] = sum(distances_to_target) / len(distances_to_target)
            stats['min_distance_to_target'] = min(distances_to_target)
            stats['max_distance_to_target'] = max(distances_to_target)
        
        if velocities:
            stats['avg_velocity'] = sum(velocities) / len(velocities)
        
        return stats
    
    def land_all(self):
        """Land all drones"""
        for i, drone in enumerate(self.drones):
            if drone.land():
                logger.success(f"Drone {i} landing")
            else:
                logger.error(f"Drone {i} land command failed")
    
    def cleanup(self):
        """
        Cleanup all resources before shutdown.
        
        FIX 3: Properly stop the server thread and wait for it to finish.
        """
        logger.info("Starting cleanup...")
        
        # Signal the server thread to stop
        self._stop_server = True
        
        # Wait for server thread to exit (with timeout to prevent hanging)
        if self.target_server_t and self.target_server_t.is_alive():
            self.target_server_t.join(timeout=2.0)
            if self.target_server_t.is_alive():
                logger.warning("Server thread did not stop cleanly (timeout)")
            else:
                logger.info("Server thread stopped cleanly")
        
        # Close all drone connections
        for i, drone in enumerate(self.drones):
            try:
                drone.cleanup()
                logger.info(f"Drone {i} connection closed")
            except Exception as e:
                logger.error(f"Error closing drone {i}: {str(e)}")
        
        logger.info("Cleanup complete")


class Cli(cmd.Cmd):
    intro = """
    ╔═══════════════════════════════════════════════════════════════╗
    ║         APF Drone Swarm Control System                        ║
    ║                                                               ║
    ║  Available Commands:                                          ║
    ║    connect  - Connect to all drones                           ║
    ║    takeoff  - Arm and takeoff all drones                      ║
    ║    flyto    - Fly to target position (lat lon)                ║
    ║    land     - Land all drones                                 ║
    ║    status   - Show current swarm status                       ║
    ║    set      - Set APF parameters                              ║
    ║    help     - Show detailed help for commands                 ║
    ║    exit     - Exit the program                                ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    prompt = "(APF-Swarm) "
    
    def __init__(self, completekey = "tab", stdin = None, stdout = None):
        super().__init__(completekey, stdin, stdout)

        self.swarm = APFDroneSwarm(
            connection_strings=os.getenv('DRONE_SWARM').split(', '),
            target_pos=(0.0, 0.0),
            altitude=10.0,
        )
        # Configure APF parameters (optional - adjust these for your scenario)
        self.swarm.target_radius = 10.0  # Distance drones should maintain from target (meters)
        self.swarm.repulsion_strength = 50.0  # Repulsion from target
        self.swarm.inter_node_repulsion = 25.0  # Repulsion between drones
        self.swarm.attraction_strength = 0.3  # Attraction to circular formation
        self.swarm.min_inter_drone_distance = 5.0  # Minimum distance between drones (meters)

    def do_connect(self, args):
        """
        Connect to all drones.
        Usage: connect
        """
        print("Connecting to all drones...")
        if not self.swarm.connect_all():
            logger.error("Failed to connect all drones")
        else:
            print("All drones connected successfully!")

    def do_takeoff(self, args):
        """
        Arm and takeoff all drones to configured altitude.
        Usage: takeoff
        """
        print(f"Initiating takeoff to {self.swarm.altitude}m...")
        self.swarm.arm_and_takeoff_all()
        print("Takeoff commands sent to all drones!")

    def do_flyto(self, args):
        """
        Fly to target position and maintain formation around it.
        Usage: flyto <latitude> <longitude>
        Example: flyto 35.123456 135.654321
        """
        try:
            lat, lon = args.split()
            lat = float(lat)
            lon = float(lon)
            
            # Update target position
            self.swarm.target_pos = (lat, lon)
            logger.info(f"Target position updated to: ({lat:.6f}, {lon:.6f})")
            
            # Start APF simulation to reach and maintain formation around target
            # Run for a limited number of iterations to allow user control
            print(f"Flying to target position: ({lat:.6f}, {lon:.6f})")
            print("Starting APF formation control...")
            self.swarm.run(iterations=float('inf'), update_interval=UPDATE_INTERVAL, print_interval=PRINT_INTERVAL, verbose=True)
            
        except ValueError:
            logger.error("Invalid coordinates. Usage: flyto <latitude> <longitude>")
        except KeyboardInterrupt:
            print("\nFlight control paused. You can now issue new commands.")
        except Exception as e:
            logger.error(f"Error during flyto: {str(e)}")

    def do_land(self, args):
        """
        Land all drones.
        Usage: land
        """
        print("Landing all drones...")
        self.swarm.land_all()
        logger.info("Land commands sent to all drones")
    
    def do_status(self, args):
        """
        Show current swarm status and statistics.
        Usage: status
        """
        print("\n" + "="*70)
        print("SWARM STATUS")
        print("="*70)
        
        stats = self.swarm.get_swarm_statistics()
        print(f"Target Position: ({self.swarm.target_pos[0]:.6f}, {self.swarm.target_pos[1]:.6f})")
        print(f"Target Radius: {self.swarm.target_radius}m")
        print(f"Flight Altitude: {self.swarm.altitude}m")
        print("\nSwarm Statistics:")
        print(f"  Center: ({stats.get('center_lat', 0):.6f}, {stats.get('center_lon', 0):.6f})")
        print(f"  Drones: {stats['num_drones']} (positions available: {stats['positions_available']})")
        print(f"  Avg Distance to Target: {stats.get('avg_distance_to_target', 0):.2f}m")
        print(f"  Min/Max Distance: {stats.get('min_distance_to_target', 0):.2f}m / {stats.get('max_distance_to_target', 0):.2f}m")
        print(f"  Avg Velocity: {stats.get('avg_velocity', 0):.2f} m/s")
        
        print("\nAPF Parameters:")
        print(f"  Repulsion Strength: {self.swarm.repulsion_strength}")
        print(f"  Attraction Strength: {self.swarm.attraction_strength}")
        print(f"  Inter-Node Repulsion: {self.swarm.inter_node_repulsion}")
        print(f"  Min Inter-Drone Distance: {self.swarm.min_inter_drone_distance}m")
        print(f"  Max Speed: {self.swarm.max_speed} m/s")
        print(f"  Damping: {self.swarm.damping}")
        
        print("\nDrone Positions:")
        print(f"{'ID':<4} {'Latitude':<14} {'Longitude':<14} {'Armed':<8} {'Mode':<12}")
        print("-"*70)
        for i, drone in enumerate(self.swarm.drones):
            pos = self.swarm.get_drone_position(i)
            status = drone.get_drone_status()
            if pos:
                armed = "Yes" if status.get('armed', False) else "No"
                mode = status.get('mode', 'N/A')
                print(f"{i:<4} {pos[0]:<14.6f} {pos[1]:<14.6f} {armed:<8} {mode:<12}")
            else:
                print(f"{i:<4} {'N/A':<14} {'N/A':<14} {'N/A':<8} {'N/A':<12}")
        print()
    
    def do_set(self, args):
        """
        Set APF parameters or system settings.
        Usage: 
            set altitude <meters>
            set target_radius <meters>
            set repulsion <value>
            set attraction <value>
            set inter_repulsion <value>
            set min_distance <meters>
            set max_speed <m/s>
            set damping <0.0-1.0>
        """
        try:
            parts = args.split()
            if len(parts) < 2:
                print("Usage: set <parameter> <value>")
                print("Type 'help set' for available parameters")
                return
            
            param = parts[0].lower()
            value = float(parts[1])
            
            if param == "altitude":
                self.swarm.altitude = value
                print(f"Altitude set to {value}m")
            elif param == "target_radius":
                self.swarm.target_radius = value
                print(f"Target radius set to {value}m")
            elif param == "repulsion":
                self.swarm.repulsion_strength = value
                print(f"Repulsion strength set to {value}")
            elif param == "attraction":
                self.swarm.attraction_strength = value
                print(f"Attraction strength set to {value}")
            elif param == "inter_repulsion":
                self.swarm.inter_node_repulsion = value
                print(f"Inter-node repulsion set to {value}")
            elif param == "min_distance":
                self.swarm.min_inter_drone_distance = value
                print(f"Minimum inter-drone distance set to {value}m")
            elif param == "max_speed":
                self.swarm.max_speed = value
                print(f"Maximum speed set to {value} m/s")
            elif param == "damping":
                if 0.0 <= value <= 1.0:
                    self.swarm.damping = value
                    print(f"Damping set to {value}")
                else:
                    print("Damping must be between 0.0 and 1.0")
            else:
                print(f"Unknown parameter: {param}")
                print("Type 'help set' for available parameters")
        except ValueError:
            print("Invalid value. Please provide a numeric value.")
        except Exception as e:
            logger.error(f"Error setting parameter: {str(e)}")
    
    def do_exit(self, args):
        """
        Exit the CLI and cleanup connections.
        Usage: exit
        """
        print("Cleaning up and exiting...")
        self.swarm.cleanup()
        print("Goodbye!")
        return True
    
    def do_quit(self, args):
        """
        Alias for exit.
        Usage: quit
        """
        return self.do_exit(args)
    
    def emptyline(self):
        pass   

if __name__ == '__main__':
    cli = Cli()
    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        cli.do_exit(None)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        cli.swarm.cleanup()