// Cluster state received from WebSocket (single node)
export interface ClusterState {
  leader_id: string;
  node_id: string;
  state: string;
  term: number;
  commit_index: number;
  last_applied: number;
  timestamp: number;
  node: NodeStatus;
}

// Status of a single node
export interface NodeStatus {
  id: string;
  state: string;
  term: number;
  commit_index: number;
  last_applied: number;
  is_leader: boolean;
}

// Extended node state with leader_id
export interface NodeState {
  id: string;
  state: string;
  term: number;
  commit_index: number;
  last_applied: number;
  is_leader: boolean;
  leader_id: string;
}

// Connection status
export type ConnectionStatus = 'connected' | 'connecting' | 'disconnected';

// Node state enum values
export const NodeStateEnum = {
  FOLLOWER: 'Follower',
  CANDIDATE: 'Candidate',
  LEADER: 'Leader',
} as const;

export type NodeStateType = typeof NodeStateEnum[keyof typeof NodeStateEnum];
