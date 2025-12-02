"""
APF (Artificial Potential Field) Simulation for Drone Swarms
Uses real geolocation (latitude, longitude) and Haversine distance calculation.
Drones use DroneNode class and fly_to_target method to reach calculated positions.
"""

import math
import time
import cmd
import os
from control import DroneNode, FlightMode
from loguru import logger



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
            
            if distance < 0.1:  # Avoid division by zero
                distance = 0.1
            
            # Only repel if too close
            if distance < self.min_inter_drone_distance * 2:
                force_magnitude = self.inter_node_repulsion / (distance**2)
                
                # Calculate bearing from other drone to this drone (repulsion direction)
                bearing = calculate_bearing(other_lat, other_lon, drone_lat, drone_lon)
                
                force_lat_total += force_magnitude * math.cos(bearing)
                force_lon_total += force_magnitude * math.sin(bearing)
        
        return force_lat_total, force_lon_total
    
    def update_drone_positions(self):
        """Update all drone positions based on APF forces"""
        for i, drone in enumerate(self.drones):
            pos = self.get_drone_position(i)
            if pos is None:
                logger.warning(f"Drone {i} position unavailable, skipping update")
                continue
            
            drone_lat, drone_lon = pos
            
            # Calculate all forces
            rep_lat, rep_lon = self.calculate_repulsion_from_target(i)
            att_lat, att_lon = self.calculate_attraction_to_circle(i)
            inter_lat, inter_lon = self.calculate_inter_drone_repulsion(i)
            
            # Sum all forces (in m/s equivalent)
            total_force_lat = rep_lat + att_lat + inter_lat
            total_force_lon = rep_lon + att_lon + inter_lon
            
            # Update velocity
            self.velocities[i]['vx'] += total_force_lat * 0.1
            self.velocities[i]['vy'] += total_force_lon * 0.1
            
            # Apply damping
            self.velocities[i]['vx'] *= self.damping
            self.velocities[i]['vy'] *= self.damping
            
            # Limit maximum speed
            speed = math.sqrt(self.velocities[i]['vx']**2 + self.velocities[i]['vy']**2)
            if speed > self.max_speed:
                self.velocities[i]['vx'] = (self.velocities[i]['vx'] / speed) * self.max_speed
                self.velocities[i]['vy'] = (self.velocities[i]['vy'] / speed) * self.max_speed
            
            # Calculate new position based on velocity
            # Convert velocity (m/s) to displacement (m) over update interval
            update_interval = 1.0  # seconds
            displacement = speed * update_interval
            
            if displacement > 0.1:  # Only move if displacement is significant
                # Calculate bearing from velocity components
                bearing = math.atan2(self.velocities[i]['vy'], self.velocities[i]['vx'])
                
                # Calculate new position
                new_lat, new_lon = destination_point(drone_lat, drone_lon, bearing, displacement)
                
                # Command drone to fly to new position
                logger.info(f"Drone {i}: Moving to ({new_lat:.6f}, {new_lon:.6f}), "
                           f"displacement: {displacement:.2f}m")
                drone.fly_to_target(new_lat, new_lon, self.altitude)
            else:
                logger.debug(f"Drone {i}: Minimal displacement ({displacement:.3f}m), holding position")
    
    def get_swarm_statistics(self):
        """Calculate and return swarm statistics"""
        if not self.drones:
            return {}
        
        positions = []
        for i in range(len(self.drones)):
            pos = self.get_drone_position(i)
            if pos:
                positions.append(pos)
        
        if not positions:
            return {'num_drones': len(self.drones), 'positions_available': 0}
        
        # Calculate center of mass
        center_lat = sum(p[0] for p in positions) / len(positions)
        center_lon = sum(p[1] for p in positions) / len(positions)
        
        # Calculate average distance from target
        if self.target_pos:
            distances = [haversine_distance(p[0], p[1], self.target_pos[0], self.target_pos[1]) 
                        for p in positions]
            avg_distance = sum(distances) / len(distances)
            min_distance = min(distances)
            max_distance = max(distances)
        else:
            avg_distance = min_distance = max_distance = 0
        
        # Calculate average velocity
        avg_velocity = sum(
            math.sqrt(v['vx']**2 + v['vy']**2) 
            for v in self.velocities
        ) / len(self.velocities)
        
        return {
            'center_lat': center_lat,
            'center_lon': center_lon,
            'avg_distance_to_target': avg_distance,
            'min_distance_to_target': min_distance,
            'max_distance_to_target': max_distance,
            'avg_velocity': avg_velocity,
            'num_drones': len(self.drones),
            'positions_available': len(positions)
        }
    
    def print_state(self, iteration):
        """Print current simulation state to console"""
        stats = self.get_swarm_statistics()
        print(f"\n{'='*70}")
        print(f"Iteration: {iteration}")
        print(f"{'='*70}")
        print(f"Target Position: ({self.target_pos[0]:.6f}, {self.target_pos[1]:.6f})")
        print(f"Target Radius: {self.target_radius}m")
        print(f"Swarm Center: ({stats.get('center_lat', 0):.6f}, {stats.get('center_lon', 0):.6f})")
        print(f"Number of Drones: {stats['num_drones']} (positions available: {stats['positions_available']})")
        print(f"Avg Distance to Target: {stats.get('avg_distance_to_target', 0):.2f}m")
        print(f"Min/Max Distance: {stats.get('min_distance_to_target', 0):.2f}m / {stats.get('max_distance_to_target', 0):.2f}m")
        print(f"Avg Velocity: {stats.get('avg_velocity', 0):.2f} m/s")
        print("\nDrone Positions:")
        print(f"{'ID':<4} {'Latitude':<14} {'Longitude':<14} {'Distance(m)':<12} {'Speed(m/s)':<10}")
        print(f"{'-'*70}")
        
        for i, drone in enumerate(self.drones):
            pos = self.get_drone_position(i)
            if pos:
                if self.target_pos:
                    dist = haversine_distance(pos[0], pos[1], self.target_pos[0], self.target_pos[1])
                else:
                    dist = 0
                speed = math.sqrt(self.velocities[i]['vx']**2 + self.velocities[i]['vy']**2)
                print(f"{i:<4} {pos[0]:<14.6f} {pos[1]:<14.6f} {dist:<12.2f} {speed:<10.2f}")
            else:
                print(f"{i:<4} {'N/A':<14} {'N/A':<14} {'N/A':<12} {'N/A':<10}")
    
    def run(self, iterations=ITERATION, update_interval=UPDATE_INTERVAL, print_interval=PRINT_INTERVAL, verbose=True):
        """
        Run the APF simulation for a specified number of iterations
        
        Args:
            iterations: Number of iterations to simulate
            update_interval: Time between position updates in seconds
            print_interval: Print state every N iterations (0 to disable)
            verbose: Whether to print simulation progress
        """
        print("Starting APF Drone Swarm Simulation")
        print(f"Target: ({self.target_pos[0]:.6f}, {self.target_pos[1]:.6f})")
        print(f"Drones: {len(self.drones)}")
        print(f"Target Radius: {self.target_radius}m")
        print(f"Altitude: {self.altitude}m")
        
        for i in range(iterations):
            self.update_drone_positions()
            
            if verbose and print_interval > 0 and (i + 1) % print_interval == 0:
                self.print_state(i + 1)
            
            time.sleep(update_interval)
        
        # Print final state
        if verbose:
            print(f"\n{'='*70}")
            print("SIMULATION COMPLETE")
            self.print_state(iterations)
    
    def land_all(self):
        """Command all drones to land"""
        for i, drone in enumerate(self.drones):
            if drone.land():
                logger.success(f"Drone {i} land command sent")
            else:
                logger.error(f"Drone {i} land command failed")
    
    def cleanup(self):
        """Cleanup all drone connections"""
        for i, drone in enumerate(self.drones):
            drone.cleanup()
            logger.info(f"Drone {i} connection closed")


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
            connection_strings=[
                "udp:172.21.128.1:14550", 
                "udp:172.21.128.1:14560",
                "udp:172.21.128.1:14570",
                "udp:172.21.128.1:14580",
                "udp:172.21.128.1:14590",
                ],
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
            self.swarm.run(iterations=ITERATION , update_interval=UPDATE_INTERVAL, print_interval=PRINT_INTERVAL, verbose=True)
            
        except ValueError:
            logger.error("Invalid coordinates. Usage: flyto <latitude> <longitude>")
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