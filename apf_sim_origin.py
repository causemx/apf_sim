import tkinter as tk
from tkinter import ttk, messagebox
import math
import random
from collections import deque
from enum import Enum


class TargetMovementPattern(Enum):
    """Target movement patterns"""
    STATIC = "Static (Click)"
    CIRCULAR = "Circular"
    LINEAR = "Linear"
    SINUSOIDAL = "Sinusoidal Wave"
    RANDOM_WALK = "Random Walk"
    SPIRAL = "Spiral"


class Node:
    """Represents a swarm agent/drone"""
    def __init__(self, x, y, node_id):
        self.x = x
        self.y = y
        self.vx = 0
        self.vy = 0
        self.id = node_id
        self.radius = 6
        self.trail = deque(maxlen=20)  # Store recent positions for trail


class MovingTarget:
    """Represents a moving target object"""
    def __init__(self, start_x, start_y, pattern, canvas_width, canvas_height, **kwargs):
        self.x = start_x
        self.y = start_y
        self.vx = 0
        self.vy = 0
        self.pattern = pattern
        self.time = 0
        self.canvas_width = canvas_width
        self.canvas_height = canvas_height
        self.trail = deque(maxlen=50)
        
        # Pattern parameters
        self.speed = kwargs.get('speed', 3)
        self.center_x = kwargs.get('center_x', canvas_width / 2)
        self.center_y = kwargs.get('center_y', canvas_height / 2)
        self.radius = kwargs.get('radius', 150)
        self.amplitude = kwargs.get('amplitude', 100)
        self.frequency = kwargs.get('frequency', 0.05)
        self.direction = kwargs.get('direction', 0)
        self.direction_change_interval = kwargs.get('direction_change_interval', 50)
    
    def update(self):
        """Update target position based on pattern"""
        old_x, old_y = self.x, self.y
        
        if self.pattern == TargetMovementPattern.STATIC:
            self.vx = 0
            self.vy = 0
        elif self.pattern == TargetMovementPattern.CIRCULAR:
            self._update_circular()
        elif self.pattern == TargetMovementPattern.LINEAR:
            self._update_linear()
        elif self.pattern == TargetMovementPattern.SINUSOIDAL:
            self._update_sinusoidal()
        elif self.pattern == TargetMovementPattern.RANDOM_WALK:
            self._update_random_walk()
        elif self.pattern == TargetMovementPattern.SPIRAL:
            self._update_spiral()
        
        self.trail.append((self.x, self.y))
        self.time += 1
    
    def _update_circular(self):
        angle = (self.time * self.speed / self.radius) % (2 * math.pi)
        new_x = self.center_x + self.radius * math.cos(angle)
        new_y = self.center_y + self.radius * math.sin(angle)
        self.vx = new_x - self.x
        self.vy = new_y - self.y
        self.x = new_x
        self.y = new_y
    
    def _update_linear(self):
        self.vx = self.speed * math.cos(self.direction)
        self.vy = self.speed * math.sin(self.direction)
        self.x += self.vx
        self.y += self.vy
        
        if self.x <= 0 or self.x >= self.canvas_width:
            self.direction = math.pi - self.direction
        if self.y <= 0 or self.y >= self.canvas_height:
            self.direction = -self.direction
    
    def _update_sinusoidal(self):
        x_offset = self.speed * self.time
        self.x = (self.center_x + x_offset) % self.canvas_width
        y_offset = self.amplitude * math.sin(self.frequency * self.time)
        self.y = self.center_y + y_offset
        self.y = max(self.amplitude, min(self.canvas_height - self.amplitude, self.y))
        
        self.vx = self.speed
        self.vy = self.amplitude * self.frequency * math.cos(self.frequency * self.time)
    
    def _update_random_walk(self):
        if self.time % self.direction_change_interval == 0:
            self.direction = random.uniform(0, 2 * math.pi)
        
        self.vx = self.speed * math.cos(self.direction)
        self.vy = self.speed * math.sin(self.direction)
        self.x += self.vx
        self.y += self.vy
        
        if self.x <= 0 or self.x >= self.canvas_width:
            self.direction = math.pi - self.direction
        if self.y <= 0 or self.y >= self.canvas_height:
            self.direction = -self.direction
    
    def _update_spiral(self):
        angle = (self.time * self.speed / self.radius) % (2 * math.pi)
        current_radius = self.radius + self.time * 0.2
        
        self.x = self.center_x + current_radius * math.cos(angle)
        self.y = self.center_y + current_radius * math.sin(angle)
        
        self.x = max(10, min(self.canvas_width - 10, self.x))
        self.y = max(10, min(self.canvas_height - 10, self.y))


class AdvancedAPFSimulationGUI:
    """Advanced APF Simulation GUI with multiple features"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Advanced APF Swarm Simulation - Tkinter")
        self.root.geometry("1400x800")
        
        # Canvas setup
        self.canvas_width = 900
        self.canvas_height = 650
        
        # Simulation state
        self.running = False
        self.paused = False
        self.iteration = 0
        self.simulation_time = 0
        
        # Parameters
        self.num_nodes = 20
        self.target_radius = 100
        self.repulsion_strength = 3000
        self.inter_node_repulsion = 500
        self.attraction_strength = 0.5
        self.damping = 0.9
        self.max_speed = 5
        
        # Nodes and target
        self.nodes = []
        self.target = None
        self.target_pattern = TargetMovementPattern.STATIC
        
        # Statistics
        self.stats_history = deque(maxlen=500)
        self.avg_distance = 0
        self.min_distance = 0
        self.max_distance = 0
        self.avg_velocity = 0
        
        # Visualization options
        self.show_trails = tk.BooleanVar(value=False)
        self.show_forces = tk.BooleanVar(value=False)
        self.show_grid = tk.BooleanVar(value=True)
        
        self._setup_ui()
        self._initialize_simulation()
    
    def _setup_ui(self):
        """Setup the UI layout"""
        # Main notebook with tabs
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Simulation tab
        sim_frame = ttk.Frame(notebook)
        notebook.add(sim_frame, text="Simulation")
        self._create_simulation_tab(sim_frame)
        
        # Statistics tab
        stats_frame = ttk.Frame(notebook)
        notebook.add(stats_frame, text="Statistics")
        self._create_statistics_tab(stats_frame)
        
        # Settings tab
        settings_frame = ttk.Frame(notebook)
        notebook.add(settings_frame, text="Settings")
        self._create_settings_tab(settings_frame)
    
    def _create_simulation_tab(self, parent):
        """Create the main simulation tab"""
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left - Canvas
        left_frame = ttk.Frame(main_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        title = ttk.Label(left_frame, text="Swarm Visualization", font=('Arial', 12, 'bold'))
        title.pack()
        
        self.canvas = tk.Canvas(left_frame, width=self.canvas_width, height=self.canvas_height,
                               bg='white', relief=tk.SUNKEN, borderwidth=2)
        self.canvas.pack(pady=5)
        self.canvas.bind('<Button-1>', self._on_canvas_click)
        
        status_frame = ttk.Frame(left_frame)
        status_frame.pack(fill=tk.X)
        self.status_label = ttk.Label(status_frame, text="Ready", font=('Arial', 9))
        self.status_label.pack()
        
        # Right - Quick controls
        right_frame = ttk.Frame(main_frame, width=300)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(10, 0))
        right_frame.pack_propagate(False)
        
        # Simulation controls
        ctrl_frame = ttk.LabelFrame(right_frame, text="Simulation Control")
        ctrl_frame.pack(fill=tk.X, pady=5)
        
        btn_frame = ttk.Frame(ctrl_frame)
        btn_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.start_btn = ttk.Button(btn_frame, text="▶ Start", command=self._start_simulation)
        self.start_btn.pack(side=tk.LEFT, padx=2)
        
        self.pause_btn = ttk.Button(btn_frame, text="⏸ Pause", command=self._pause_simulation, state=tk.DISABLED)
        self.pause_btn.pack(side=tk.LEFT, padx=2)
        
        self.reset_btn = ttk.Button(btn_frame, text="↻ Reset", command=self._reset_simulation)
        self.reset_btn.pack(side=tk.LEFT, padx=2)
        
        # Speed control
        speed_frame = ttk.Frame(ctrl_frame)
        speed_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(speed_frame, text="Speed:").pack(side=tk.LEFT)
        self.speed_var = tk.IntVar(value=30)
        ttk.Spinbox(speed_frame, from_=5, to=100, textvariable=self.speed_var, width=5).pack(side=tk.LEFT, padx=5)
        ttk.Label(speed_frame, text="ms").pack(side=tk.LEFT)
        
        # Iteration counter
        iter_frame = ttk.Frame(ctrl_frame)
        iter_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(iter_frame, text="Iteration:").pack(side=tk.LEFT)
        self.iter_label = ttk.Label(iter_frame, text="0", font=('Arial', 11, 'bold'), foreground='blue')
        self.iter_label.pack(side=tk.LEFT, padx=5)
        
        # Visualization options
        vis_frame = ttk.LabelFrame(right_frame, text="Visualization")
        vis_frame.pack(fill=tk.X, pady=5)
        
        ttk.Checkbutton(vis_frame, text="Show Trails", variable=self.show_trails).pack(anchor=tk.W, padx=5, pady=2)
        ttk.Checkbutton(vis_frame, text="Show Forces", variable=self.show_forces).pack(anchor=tk.W, padx=5, pady=2)
        ttk.Checkbutton(vis_frame, text="Show Grid", variable=self.show_grid).pack(anchor=tk.W, padx=5, pady=2)
        
        # Statistics display
        stat_frame = ttk.LabelFrame(right_frame, text="Statistics")
        stat_frame.pack(fill=tk.X, pady=5)
        
        self.stat_text = tk.Text(stat_frame, height=8, width=35, font=('Courier', 9))
        self.stat_text.pack(padx=5, pady=5)
        
        scrollbar = ttk.Scrollbar(stat_frame, command=self.stat_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.stat_text.config(yscrollcommand=scrollbar.set)
    
    def _create_statistics_tab(self, parent):
        """Create the statistics tab"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        title = ttk.Label(frame, text="Real-time Statistics", font=('Arial', 12, 'bold'))
        title.pack()
        
        # Canvas for graph
        self.stats_canvas = tk.Canvas(frame, height=400, bg='white', relief=tk.SUNKEN, borderwidth=2)
        self.stats_canvas.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Legend
        legend_frame = ttk.Frame(frame)
        legend_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(legend_frame, text="■ Avg Distance", foreground='blue').pack(side=tk.LEFT, padx=10)
        ttk.Label(legend_frame, text="■ Min Distance", foreground='green').pack(side=tk.LEFT, padx=10)
        ttk.Label(legend_frame, text="■ Max Distance", foreground='red').pack(side=tk.LEFT, padx=10)
    
    def _create_settings_tab(self, parent):
        """Create the settings tab"""
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left column
        left_col = ttk.Frame(main_frame)
        left_col.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        # Target settings
        target_frame = ttk.LabelFrame(left_col, text="Target Settings")
        target_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(target_frame, text="Movement Pattern:").pack(anchor=tk.W, padx=5, pady=(5, 0))
        self.pattern_var = tk.StringVar(value=TargetMovementPattern.STATIC.value)
        pattern_combo = ttk.Combobox(target_frame, textvariable=self.pattern_var,
                                    values=[p.value for p in TargetMovementPattern],
                                    state='readonly')
        pattern_combo.pack(fill=tk.X, padx=5, pady=5)
        pattern_combo.bind('<<ComboboxSelected>>', self._on_pattern_change)
        
        ttk.Label(target_frame, text="Target Speed:").pack(anchor=tk.W, padx=5)
        self.target_speed_var = tk.DoubleVar(value=3)
        ttk.Scale(target_frame, from_=0.5, to=10, variable=self.target_speed_var).pack(fill=tk.X, padx=5, pady=5)
        self.speed_label = ttk.Label(target_frame, text="0.0")
        self.speed_label.pack(anchor=tk.E, padx=5, pady=(0, 5))
        
        ttk.Label(target_frame, text="Formation Distance:").pack(anchor=tk.W, padx=5)
        self.form_dist_var = tk.DoubleVar(value=100)
        ttk.Scale(target_frame, from_=50, to=300, variable=self.form_dist_var).pack(fill=tk.X, padx=5, pady=5)
        self.dist_label = ttk.Label(target_frame, text="100")
        self.dist_label.pack(anchor=tk.E, padx=5, pady=(0, 5))
        
        # APF Forces
        force_frame = ttk.LabelFrame(left_col, text="APF Forces")
        force_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(force_frame, text="Repulsion Strength:").pack(anchor=tk.W, padx=5)
        self.repel_var = tk.IntVar(value=3000)
        ttk.Scale(force_frame, from_=500, to=5000, variable=self.repel_var).pack(fill=tk.X, padx=5, pady=5)
        self.repel_label = ttk.Label(force_frame, text="3000")
        self.repel_label.pack(anchor=tk.E, padx=5, pady=(0, 5))
        
        ttk.Label(force_frame, text="Attraction Strength:").pack(anchor=tk.W, padx=5)
        self.attr_var = tk.DoubleVar(value=0.5)
        ttk.Scale(force_frame, from_=0, to=2, variable=self.attr_var).pack(fill=tk.X, padx=5, pady=5)
        self.attr_label = ttk.Label(force_frame, text="0.50")
        self.attr_label.pack(anchor=tk.E, padx=5, pady=(0, 5))
        
        ttk.Label(force_frame, text="Damping Factor:").pack(anchor=tk.W, padx=5)
        self.damp_var = tk.DoubleVar(value=0.9)
        ttk.Scale(force_frame, from_=0.7, to=0.99, variable=self.damp_var).pack(fill=tk.X, padx=5, pady=5)
        self.damp_label = ttk.Label(force_frame, text="0.90")
        self.damp_label.pack(anchor=tk.E, padx=5, pady=(0, 5))
        
        ttk.Label(force_frame, text="Inter-Node Repulsion:").pack(anchor=tk.W, padx=5)
        self.inter_var = tk.IntVar(value=500)
        ttk.Scale(force_frame, from_=100, to=1000, variable=self.inter_var).pack(fill=tk.X, padx=5, pady=5)
        self.inter_label = ttk.Label(force_frame, text="500")
        self.inter_label.pack(anchor=tk.E, padx=5, pady=(0, 5))
        
        # Right column
        right_col = ttk.Frame(main_frame)
        right_col.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Swarm settings
        swarm_frame = ttk.LabelFrame(right_col, text="Swarm Settings")
        swarm_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(swarm_frame, text="Number of Nodes:").pack(anchor=tk.W, padx=5)
        self.nodes_var = tk.IntVar(value=20)
        self.nodes_spinbox = ttk.Spinbox(swarm_frame, from_=5, to=150, textvariable=self.nodes_var,
                                        width=10, command=self._on_nodes_change)
        self.nodes_spinbox.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(swarm_frame, text="Apply", command=self._on_nodes_change).pack(padx=5, pady=5)
        
        # Info frame
        info_frame = ttk.LabelFrame(right_col, text="Information")
        info_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.info_text = tk.Text(info_frame, height=15, font=('Courier', 9))
        self.info_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.info_text.insert('end', """
APF SWARM SIMULATION GUIDE

LEFT PANEL:
- Visual representation of swarm behavior
- Click on canvas to set static target
- Watch nodes form circular pattern

PARAMETERS:
- Repulsion: Higher = stronger push from target
- Attraction: Higher = tighter formation
- Damping: Higher = smoother movement
- Inter-Node: Higher = more spacing

PATTERNS:
- Static: Click canvas to move target
- Circular: Target moves in circle
- Linear: Target moves in straight line
- Sinusoidal: Target follows wave pattern
- Random: Target moves randomly
- Spiral: Target spirals outward

TIPS:
- Start with low numbers (5-10 nodes)
- Increase damping for stability
- Adjust attraction for formation tightness
        """)
        self.info_text.config(state=tk.DISABLED)
    
    def _on_canvas_click(self, event):
        """Handle canvas click"""
        if self.pattern_var.get() == TargetMovementPattern.STATIC.value:
            self.target.x = event.x
            self.target.y = event.y
            self.status_label.config(text=f"Target set at ({event.x}, {event.y})")
    
    def _on_pattern_change(self, event=None):
        """Handle pattern change"""
        pattern_str = self.pattern_var.get()
        for p in TargetMovementPattern:
            if p.value == pattern_str:
                self.target.pattern = p
                self.target.time = 0
                break
    
    def _on_nodes_change(self):
        """Handle node count change"""
        self.num_nodes = self.nodes_var.get()
        self._initialize_simulation()
        self.status_label.config(text=f"Swarm reset with {self.num_nodes} nodes")
    
    def _initialize_simulation(self):
        """Initialize the simulation"""
        self.nodes = []
        for i in range(self.num_nodes):
            x = random.uniform(100, self.canvas_width - 100)
            y = random.uniform(100, self.canvas_height - 100)
            self.nodes.append(Node(x, y, i))
        
        self.target = MovingTarget(
            self.canvas_width / 2, self.canvas_height / 2,
            TargetMovementPattern.STATIC,
            self.canvas_width, self.canvas_height
        )
    
    def _start_simulation(self):
        """Start simulation"""
        self.running = True
        self.paused = False
        self.start_btn.config(state=tk.DISABLED)
        self.pause_btn.config(state=tk.NORMAL)
        self._animate()
    
    def _pause_simulation(self):
        """Pause/resume simulation"""
        self.paused = not self.paused
        if self.paused:
            self.pause_btn.config(text="▶ Resume")
        else:
            self.pause_btn.config(text="⏸ Pause")
            self._animate()
    
    def _reset_simulation(self):
        """Reset simulation"""
        self.running = False
        self.paused = False
        self.iteration = 0
        self.simulation_time = 0
        self.stats_history.clear()
        self._initialize_simulation()
        self.start_btn.config(state=tk.NORMAL)
        self.pause_btn.config(state=tk.DISABLED)
        self.pause_btn.config(text="⏸ Pause")
        self.iter_label.config(text="0")
        self._draw()
        self.status_label.config(text="Simulation reset")
    
    def _calculate_repulsion_from_target(self, node):
        """Calculate repulsive force from target"""
        dx = node.x - self.target.x
        dy = node.y - self.target.y
        distance = math.sqrt(dx**2 + dy**2)
        
        if distance < 1:
            distance = 1
        
        force_magnitude = self.repulsion_strength / (distance ** 2)
        return (dx / distance) * force_magnitude, (dy / distance) * force_magnitude
    
    def _calculate_attraction_to_circle(self, node):
        """Calculate attraction force"""
        dx = node.x - self.target.x
        dy = node.y - self.target.y
        distance = math.sqrt(dx**2 + dy**2)
        
        if distance < 1:
            return 0, 0
        
        distance_error = distance - self.target_radius
        force_magnitude = self.attraction_strength * distance_error
        
        return -(dx / distance) * force_magnitude, -(dy / distance) * force_magnitude
    
    def _calculate_inter_node_repulsion(self, node):
        """Calculate inter-node repulsion"""
        fx_total = 0
        fy_total = 0
        
        for other_node in self.nodes:
            if other_node.id == node.id:
                continue
            
            dx = node.x - other_node.x
            dy = node.y - other_node.y
            distance = math.sqrt(dx**2 + dy**2)
            
            if distance < 1:
                distance = 1
            
            if distance < 80:
                force_magnitude = self.inter_node_repulsion / (distance ** 2)
                fx_total += (dx / distance) * force_magnitude
                fy_total += (dy / distance) * force_magnitude
        
        return fx_total, fy_total
    
    def _update_nodes(self):
        """Update node positions"""
        # Update parameters from UI
        self.repulsion_strength = self.repel_var.get()
        self.attraction_strength = self.attr_var.get()
        self.damping = self.damp_var.get()
        self.target_radius = self.form_dist_var.get()
        self.inter_node_repulsion = self.inter_var.get()
        
        # Update labels
        self.repel_label.config(text=str(self.repulsion_strength))
        self.attr_label.config(text=f"{self.attraction_strength:.2f}")
        self.damp_label.config(text=f"{self.damping:.2f}")
        self.inter_label.config(text=str(self.inter_node_repulsion))
        self.speed_label.config(text=f"{self.target_speed_var.get():.1f}")
        self.dist_label.config(text=f"{self.target_radius:.0f}")
        
        # Update target
        self.target.speed = self.target_speed_var.get()
        self.target.update()
        
        # Update nodes
        for node in self.nodes:
            rep_x, rep_y = self._calculate_repulsion_from_target(node)
            att_x, att_y = self._calculate_attraction_to_circle(node)
            inter_x, inter_y = self._calculate_inter_node_repulsion(node)
            
            total_fx = rep_x + att_x + inter_x
            total_fy = rep_y + att_y + inter_y
            
            node.vx += total_fx * 0.01
            node.vy += total_fy * 0.01
            
            node.vx *= self.damping
            node.vy *= self.damping
            
            speed = math.sqrt(node.vx**2 + node.vy**2)
            if speed > self.max_speed:
                node.vx = (node.vx / speed) * self.max_speed
                node.vy = (node.vy / speed) * self.max_speed
            
            node.x += node.vx
            node.y += node.vy
            
            node.x = max(node.radius, min(self.canvas_width - node.radius, node.x))
            node.y = max(node.radius, min(self.canvas_height - node.radius, node.y))
            
            node.trail.append((node.x, node.y))
        
        # Calculate statistics
        self._calculate_statistics()
        self.iteration += 1
        self.simulation_time += self.speed_var.get()
    
    def _calculate_statistics(self):
        """Calculate statistics"""
        if not self.nodes:
            return
        
        distances = [math.sqrt((node.x - self.target.x)**2 + (node.y - self.target.y)**2) 
                    for node in self.nodes]
        
        self.avg_distance = sum(distances) / len(distances)
        self.min_distance = min(distances)
        self.max_distance = max(distances)
        self.avg_velocity = sum(math.sqrt(node.vx**2 + node.vy**2) for node in self.nodes) / len(self.nodes)
        
        # Store in history
        self.stats_history.append((self.avg_distance, self.min_distance, self.max_distance))
        
        # Update statistics text
        stat_text = f"""Iteration: {self.iteration}
Simulation Time: {self.simulation_time}ms
Nodes: {len(self.nodes)}

Distances:
  Avg: {self.avg_distance:.1f}
  Min: {self.min_distance:.1f}
  Max: {self.max_distance:.1f}
  Range: {self.max_distance - self.min_distance:.1f}

Velocities:
  Avg: {self.avg_velocity:.2f}

Target:
  Position: ({self.target.x:.0f}, {self.target.y:.0f})
  Pattern: {self.target.pattern.value}"""
        
        self.stat_text.config(state=tk.NORMAL)
        self.stat_text.delete('1.0', tk.END)
        self.stat_text.insert('1.0', stat_text)
        self.stat_text.config(state=tk.DISABLED)
        
        self.iter_label.config(text=str(self.iteration))
    
    def _draw(self):
        """Draw visualization"""
        self.canvas.delete('all')
        
        # Draw grid
        if self.show_grid.get():
            for i in range(0, self.canvas_width, 50):
                self.canvas.create_line(i, 0, i, self.canvas_height, fill='#f5f5f5', width=1)
            for i in range(0, self.canvas_height, 50):
                self.canvas.create_line(0, i, self.canvas_width, i, fill='#f5f5f5', width=1)
        
        # Draw target trail
        if self.show_trails.get() and len(self.target.trail) > 1:
            trail_points = list(self.target.trail)
            for i in range(len(trail_points) - 1):
                self.canvas.create_line(
                    trail_points[i][0], trail_points[i][1],
                    trail_points[i+1][0], trail_points[i+1][1],
                    fill='#ffcccc', width=1
                )
        
        # Draw formation circle
        self.canvas.create_oval(
            self.target.x - self.target_radius,
            self.target.y - self.target_radius,
            self.target.x + self.target_radius,
            self.target.y + self.target_radius,
            outline='#ff6b6b', width=2, dash=(5, 5)
        )
        
        # Draw target
        self.canvas.create_oval(
            self.target.x - 8, self.target.y - 8,
            self.target.x + 8, self.target.y + 8,
            fill='red', outline='darkred', width=2
        )
        
        # Draw nodes
        for node in self.nodes:
            # Draw trail
            if self.show_trails.get() and len(node.trail) > 1:
                trail_points = list(node.trail)
                for i in range(len(trail_points) - 1):
                    alpha = int(100 * (i / len(trail_points)))
                    self.canvas.create_line(
                        trail_points[i][0], trail_points[i][1],
                        trail_points[i+1][0], trail_points[i+1][1],
                        fill='#ccddff', width=1
                    )
            
            # Draw node
            self.canvas.create_oval(
                node.x - node.radius, node.y - node.radius,
                node.x + node.radius, node.y + node.radius,
                fill='#4169e1', outline='darkblue', width=1
            )
            
            # Draw node ID
            self.canvas.create_text(
                node.x, node.y,
                text=str(node.id % 10),
                fill='white',
                font=('Arial', 7, 'bold')
            )
        
        # Draw swarm center
        if self.nodes:
            center_x = sum(n.x for n in self.nodes) / len(self.nodes)
            center_y = sum(n.y for n in self.nodes) / len(self.nodes)
            
            self.canvas.create_oval(
                center_x - 6, center_y - 6,
                center_x + 6, center_y + 6,
                fill='#00cc00', outline='darkgreen', width=2
            )
        
        # Draw statistics graph
        self._draw_statistics_graph()
    
    def _draw_statistics_graph(self):
        """Draw statistics graph"""
        if len(self.stats_history) < 2:
            return
        
        canvas = self.stats_canvas
        canvas.delete('all')
        
        width = canvas.winfo_width()
        height = canvas.winfo_height()
        
        if width == 1:
            return
        
        # Draw axes
        canvas.create_line(40, height - 30, width - 20, height - 30, width=2)
        canvas.create_line(40, 10, 40, height - 30, width=2)
        
        # Get data
        history = list(self.stats_history)
        max_val = max(max(h[0], h[1], h[2]) for h in history) or 1
        
        # Scale factor
        scale_x = (width - 60) / len(history)
        scale_y = (height - 40) / max_val
        
        # Draw lines
        colors = ['#4169e1', '#00cc00', '#ff0000']
        line_types = [0, 1, 2]  # avg, min, max
        
        for line_idx in line_types:
            points = []
            for i, (avg, min_d, max_d) in enumerate(history):
                values = [avg, min_d, max_d]
                y = height - 30 - (values[line_idx] * scale_y)
                x = 40 + (i * scale_x)
                points.append((x, y))
            
            for i in range(len(points) - 1):
                canvas.create_line(
                    points[i][0], points[i][1],
                    points[i+1][0], points[i+1][1],
                    fill=colors[line_idx], width=2
                )
    
    def _animate(self):
        """Animation loop"""
        if self.running and not self.paused:
            self._update_nodes()
            self._draw()
            speed = self.speed_var.get()
            self.root.after(speed, self._animate)


if __name__ == '__main__':
    root = tk.Tk()
    app = AdvancedAPFSimulationGUI(root)
    root.mainloop()