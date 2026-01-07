"""
node.py - Raft Consensus Algorithm Implementation

This module implements a Raft node that manages state machine replication
across a distributed cluster using the Raft consensus algorithm.
"""

import asyncio
import logging
import random
import time
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from dataclasses import dataclass, field

from message import (
    Message, MessageType, NodeState, LogEntry, 
    VoteRequest, VoteResponse, AppendEntriesRequest, 
    AppendEntriesResponse, MessageTranslator
)
from network_comm import NetworkComm
from node_metadata import NodeMetadata


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RaftNode:
    """
    A Raft consensus algorithm node implementation.
    
    Implements the core Raft protocol with leader election and log replication.
    """
    
    # Timing constants (in seconds)
    ELECTION_TIMEOUT_MIN = 1.5
    ELECTION_TIMEOUT_MAX = 3.0
    HEARTBEAT_INTERVAL = 0.5
    
    def __init__(self, node_metadata: NodeMetadata, all_nodes: List[NodeMetadata]):
        """
        Initialize a Raft node.
        
        Args:
            node_metadata: This node's metadata (host and port)
            all_nodes: List of all nodes in the cluster (including this one)
        """
        self.metadata = node_metadata
        self.all_nodes = all_nodes
        self.network = NetworkComm(all_nodes, node_metadata.get_port())
        
        # Persistent state (on all servers)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state (on all servers)
        self.commit_index = -1
        self.last_applied = -1
        self.state = NodeState.FOLLOWER
        
        # Volatile state (on leaders)
        self.next_index: Dict[str, int] = {}  # Indexed by node address
        self.match_index: Dict[str, int] = {}  # Indexed by node address
        
        # Election and heartbeat timing
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat_time = time.time()
        self.last_election_time = time.time()
        
        # State machine and callbacks
        self.state_machine: Dict[str, Any] = {}
        self.on_commit: Optional[Callable[[LogEntry], None]] = None
        
        # Running state
        self._running = False
        self._tasks: List[asyncio.Task] = []
    
    async def start(self):
        """Start the Raft node."""
        logger.info(f"Starting Raft node at {self.metadata}")
        self._running = True
        
        # Set up network connection handler
        self.network.set_connection_handler(self._handle_network_connection)
        
        # Start network communication
        await self.network.run()
        
        # Start main Raft loop
        self._tasks.append(asyncio.create_task(self._run_raft_loop()))
    
    async def stop(self):
        """Stop the Raft node."""
        logger.info("Stopping Raft node")
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
        
        # Wait for tasks
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Stop network
        await self.network.stop()
    
    async def _run_raft_loop(self):
        """Main Raft event loop."""
        logger.info(f"Raft loop started, initial state: {self.state.value}")
        
        while self._running:
            try:
                current_time = time.time()
                
                if self.state == NodeState.FOLLOWER:
                    await self._handle_follower_state(current_time)
                elif self.state == NodeState.CANDIDATE:
                    await self._handle_candidate_state(current_time)
                elif self.state == NodeState.LEADER:
                    await self._handle_leader_state(current_time)
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error in Raft loop: {e}")
                await asyncio.sleep(0.5)
    
    async def _handle_follower_state(self, current_time: float):
        """Handle follower state logic."""
        time_since_heartbeat = current_time - self.last_heartbeat_time
        
        # Check for election timeout
        if time_since_heartbeat > self.election_timeout:
            logger.info(f"Election timeout (waited {time_since_heartbeat:.2f}s), becoming candidate")
            await self._become_candidate()
    
    async def _handle_candidate_state(self, current_time: float):
        """Handle candidate state logic."""
        time_since_election = current_time - self.last_election_time
        
        # Retry election if timeout
        if time_since_election > self.election_timeout:
            logger.info("Election timeout, retrying election")
            await self._request_votes()
    
    async def _handle_leader_state(self, current_time: float):
        """Handle leader state logic."""
        time_since_heartbeat = current_time - self.last_heartbeat_time
        
        # Send heartbeat to all followers
        if time_since_heartbeat > self.HEARTBEAT_INTERVAL:
            await self._send_heartbeats()
            self.last_heartbeat_time = current_time
        
        # Update commit index if possible
        await self._update_commit_index()
        
        # Apply committed entries
        await self._apply_entries()
    
    async def _become_candidate(self):
        """Transition to candidate state and start election."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self._get_node_id()
        self.last_election_time = time.time()
        self.election_timeout = self._random_election_timeout()
        
        logger.info(f"Became candidate for term {self.current_term}")
        
        await self._request_votes()
    
    async def _request_votes(self):
        """Request votes from all other nodes."""
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
        
        vote_request = VoteRequest(
            term=self.current_term,
            candidate_id=self._get_node_id(),
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
        
        message = Message(
            msg_type=MessageType.VOTE_REQUEST.value,
            data=self._serialize_vote_request(vote_request),
            sender=self.metadata
        )
        
        # Send vote requests to all other nodes
        tasks = []
        for node in self.all_nodes:
            if not self.network.is_node_me(node):
                tasks.append(asyncio.create_task(
                    self._send_vote_request(node, message)
                ))
        
        # Gather responses
        if tasks:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            await self._process_vote_responses(responses)
    
    async def _send_vote_request(self, target_node: NodeMetadata, message: Message):
        """Send vote request and get response."""
        response = await self.network.send_message_with_response(target_node, message)
        return response
    
    async def _process_vote_responses(self, responses: List[Optional[Message]]):
        """Process vote responses and determine if elected."""
        votes_granted = 0
        max_term = self.current_term
        
        for response in responses:
            if response and response.msg_type == MessageType.VOTE_RESPONSE.value:
                try:
                    vote_resp = self._deserialize_vote_response(response.data)
                    
                    if vote_resp.term > max_term:
                        max_term = vote_resp.term
                    
                    if vote_resp.vote_granted and vote_resp.term == self.current_term:
                        votes_granted += 1
                except Exception as e:
                    logger.debug(f"Error processing vote response: {e}")
        
        # Update term if newer term seen
        if max_term > self.current_term:
            self.current_term = max_term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            return
        
        # Check if elected (need majority)
        required_votes = len(self.all_nodes) // 2 + 1
        if votes_granted + 1 >= required_votes:  # +1 for own vote
            logger.info(f"Won election with {votes_granted + 1} votes (need {required_votes})")
            await self._become_leader()
    
    async def _become_leader(self):
        """Transition to leader state."""
        self.state = NodeState.LEADER
        logger.info(f"Became leader for term {self.current_term}")
        
        # Initialize next_index and match_index
        last_log_index = len(self.log)
        for node in self.all_nodes:
            node_id = f"{node.get_host()}:{node.get_port()}"
            self.next_index[node_id] = last_log_index
            self.match_index[node_id] = -1
        
        # Send initial heartbeat
        await self._send_heartbeats()
    
    async def _send_heartbeats(self):
        """Send heartbeat messages to all followers."""
        for node in self.all_nodes:
            if not self.network.is_node_me(node):
                asyncio.create_task(self._send_append_entries(node))
    
    async def _send_append_entries(self, target_node: NodeMetadata):
        """Send AppendEntries RPC to a specific node."""
        node_id = f"{target_node.get_host()}:{target_node.get_port()}"
        next_idx = self.next_index.get(node_id, len(self.log))
        
        # Determine entries to send
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
        entries = self.log[next_idx:] if next_idx < len(self.log) else []
        
        # Serialize entries
        serialized_entries = [
            {
                'term': entry.term,
                'index': entry.index,
                'command': entry.command,
                'timestamp': entry.timestamp
            }
            for entry in entries
        ]
        
        append_request = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self._get_node_id(),
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=serialized_entries,
            leader_commit=self.commit_index
        )
        
        message = Message(
            msg_type=MessageType.APPEND_ENTRIES.value,
            data=self._serialize_append_request(append_request),
            sender=self.metadata
        )
        
        # Send and get response
        response = await self.network.send_message_with_response(target_node, message)
        
        if response and response.msg_type == MessageType.APPEND_RESPONSE.value:
            await self._process_append_response(target_node, response)
    
    async def _process_append_response(self, node: NodeMetadata, response: Message):
        """Process AppendEntries response."""
        try:
            append_resp = self._deserialize_append_response(response.data)
            node_id = f"{node.get_host()}:{node.get_port()}"
            
            # Update term if higher
            if append_resp.term > self.current_term:
                self.current_term = append_resp.term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
                return
            
            if append_resp.success:
                # Update indices for successful replication
                if append_resp.match_index >= 0:
                    self.match_index[node_id] = append_resp.match_index
                    self.next_index[node_id] = append_resp.match_index + 1
            else:
                # Decrement next_index for retry
                if self.next_index[node_id] > 0:
                    self.next_index[node_id] -= 1
        
        except Exception as e:
            logger.debug(f"Error processing append response from {node}: {e}")
    
    async def _update_commit_index(self):
        """Update commit index based on replication progress."""
        if self.state != NodeState.LEADER:
            return
        
        # Find highest index replicated on majority
        match_indices = [self.match_index.get(f"{n.get_host()}:{n.get_port()}", -1) for n in self.all_nodes]
        match_indices.sort(reverse=True)
        
        majority_index = match_indices[len(match_indices) // 2]
        
        # Update commit index (ensure we don't commit entries from previous terms)
        if majority_index > self.commit_index and majority_index < len(self.log):
            if self.log[majority_index].term == self.current_term:
                self.commit_index = majority_index
                logger.debug(f"Updated commit_index to {self.commit_index}")
    
    async def _apply_entries(self):
        """Apply committed entries to state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                self._apply_entry(entry)
    
    def _apply_entry(self, entry: LogEntry):
        """Apply a log entry to the state machine."""
        try:
            # Simple key-value state machine
            if entry.command.startswith("SET "):
                parts = entry.command[4:].split(" ", 1)
                if len(parts) == 2:
                    key, value = parts
                    self.state_machine[key] = value
                    logger.debug(f"Applied: SET {key} = {value}")
            elif entry.command.startswith("GET "):
                key = entry.command[4:]
                value = self.state_machine.get(key, "NOT_FOUND")
                logger.debug(f"Applied: GET {key} = {value}")
            
            # Call user callback if registered
            if self.on_commit:
                self.on_commit(entry)
        
        except Exception as e:
            logger.error(f"Error applying entry: {e}")
    
    async def _handle_network_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming network messages."""
        try:
            message = await self.network._receive_message(reader)
            
            if not message:
                return
            
            response = None
            
            if message.msg_type == MessageType.VOTE_REQUEST.value:
                response = await self._handle_vote_request(message)
            elif message.msg_type == MessageType.APPEND_ENTRIES.value:
                response = await self._handle_append_entries(message)
            
            # Send response
            if response:
                await self.network._send_message(writer, response)
        
        except Exception as e:
            logger.debug(f"Error handling connection: {e}")
        
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _handle_vote_request(self, message: Message) -> Optional[Message]:
        """Handle incoming vote request."""
        try:
            vote_req = self._deserialize_vote_request(message.data)
            
            # Update term if higher
            if vote_req.term > self.current_term:
                self.current_term = vote_req.term
                self.voted_for = None
                self.state = NodeState.FOLLOWER
            
            # Grant vote if conditions met
            can_vote = (
                vote_req.term >= self.current_term and
                (self.voted_for is None or self.voted_for == vote_req.candidate_id)
            )
            
            if can_vote:
                # Check if candidate's log is at least as up-to-date
                last_log_index = len(self.log) - 1
                last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
                
                if vote_req.last_log_term > last_log_term or \
                   (vote_req.last_log_term == last_log_term and vote_req.last_log_index >= last_log_index):
                    self.voted_for = vote_req.candidate_id
                    self.last_heartbeat_time = time.time()
                    can_vote = True
                else:
                    can_vote = False
            
            vote_resp = VoteResponse(
                term=self.current_term,
                vote_granted=can_vote
            )
            
            logger.debug(f"Vote request from {vote_req.candidate_id}: granted={can_vote}")
            
            return Message(
                msg_type=MessageType.VOTE_RESPONSE.value,
                data=self._serialize_vote_response(vote_resp),
                sender=self.metadata
            )
        
        except Exception as e:
            logger.error(f"Error handling vote request: {e}")
            return None
    
    async def _handle_append_entries(self, message: Message) -> Optional[Message]:
        """Handle incoming append entries request."""
        try:
            append_req = self._deserialize_append_request(message.data)
            
            # Update term if higher
            if append_req.term > self.current_term:
                self.current_term = append_req.term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
            
            # Reset election timer
            if append_req.term >= self.current_term:
                self.last_heartbeat_time = time.time()
            
            success = False
            match_index = -1
            
            # Check if we have entry at prev_log_index with term prev_log_term
            if append_req.prev_log_index < 0 or (
                append_req.prev_log_index < len(self.log) and
                self.log[append_req.prev_log_index].term == append_req.prev_log_term
            ):
                # Append new entries
                start_index = append_req.prev_log_index + 1
                for i, entry_data in enumerate(append_req.entries):
                    log_index = start_index + i
                    new_entry = LogEntry(
                        term=entry_data['term'],
                        index=entry_data['index'],
                        command=entry_data['command'],
                        timestamp=entry_data.get('timestamp', time.time())
                    )
                    
                    if log_index < len(self.log):
                        if self.log[log_index].term != entry_data['term']:
                            self.log = self.log[:log_index]
                            self.log.append(new_entry)
                    else:
                        self.log.append(new_entry)
                
                match_index = start_index + len(append_req.entries) - 1
                success = True
                
                # Update commit index
                if append_req.leader_commit > self.commit_index:
                    self.commit_index = min(append_req.leader_commit, len(self.log) - 1)
                    await self._apply_entries()
            
            append_resp = AppendEntriesResponse(
                term=self.current_term,
                success=success,
                match_index=match_index
            )
            
            logger.debug(f"AppendEntries from {append_req.leader_id}: success={success}")
            
            return Message(
                msg_type=MessageType.APPEND_RESPONSE.value,
                data=self._serialize_append_response(append_resp),
                sender=self.metadata
            )
        
        except Exception as e:
            logger.error(f"Error handling append entries: {e}")
            return None
    
    async def append_entry(self, command: str) -> bool:
        """
        Append a new entry to the log (only works on leader).
        
        Args:
            command: The command to append (e.g., "SET key value")
        
        Returns:
            True if entry was appended, False otherwise
        """
        if self.state != NodeState.LEADER:
            logger.warning("Only leader can append entries")
            return False
        
        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command
        )
        
        self.log.append(entry)
        logger.info(f"Appended entry: {command}")
        return True
    
    def get_state(self) -> Dict[str, Any]:
        """Get current state information."""
        return {
            'state': self.state.value,
            'term': self.current_term,
            'voted_for': self.voted_for,
            'log_length': len(self.log),
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'state_machine': self.state_machine.copy()
        }
    
    def _get_node_id(self) -> str:
        """Get unique identifier for this node."""
        return f"{self.metadata.get_host()}:{self.metadata.get_port()}"
    
    def _random_election_timeout(self) -> float:
        """Generate random election timeout."""
        return random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)
    
    # Serialization helpers
    @staticmethod
    def _serialize_vote_request(req: VoteRequest) -> Dict[str, Any]:
        return {
            'term': req.term,
            'candidate_id': req.candidate_id,
            'last_log_index': req.last_log_index,
            'last_log_term': req.last_log_term
        }
    
    @staticmethod
    def _deserialize_vote_request(data: Dict[str, Any]) -> VoteRequest:
        return VoteRequest(
            term=data['term'],
            candidate_id=data['candidate_id'],
            last_log_index=data['last_log_index'],
            last_log_term=data['last_log_term']
        )
    
    @staticmethod
    def _serialize_vote_response(resp: VoteResponse) -> Dict[str, Any]:
        return {
            'term': resp.term,
            'vote_granted': resp.vote_granted
        }
    
    @staticmethod
    def _deserialize_vote_response(data: Dict[str, Any]) -> VoteResponse:
        return VoteResponse(
            term=data['term'],
            vote_granted=data['vote_granted']
        )
    
    @staticmethod
    def _serialize_append_request(req: AppendEntriesRequest) -> Dict[str, Any]:
        return {
            'term': req.term,
            'leader_id': req.leader_id,
            'prev_log_index': req.prev_log_index,
            'prev_log_term': req.prev_log_term,
            'entries': req.entries,
            'leader_commit': req.leader_commit
        }
    
    @staticmethod
    def _deserialize_append_request(data: Dict[str, Any]) -> AppendEntriesRequest:
        return AppendEntriesRequest(
            term=data['term'],
            leader_id=data['leader_id'],
            prev_log_index=data['prev_log_index'],
            prev_log_term=data['prev_log_term'],
            entries=data['entries'],
            leader_commit=data['leader_commit']
        )
    
    @staticmethod
    def _serialize_append_response(resp: AppendEntriesResponse) -> Dict[str, Any]:
        return {
            'term': resp.term,
            'success': resp.success,
            'match_index': resp.match_index
        }
    
    @staticmethod
    def _deserialize_append_response(data: Dict[str, Any]) -> AppendEntriesResponse:
        return AppendEntriesResponse(
            term=data['term'],
            success=data['success'],
            match_index=data.get('match_index', -1)
        )