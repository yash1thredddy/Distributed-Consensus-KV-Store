// Cluster state received from WebSocket
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

// Connection status
export type ConnectionStatus = 'connected' | 'connecting' | 'disconnected';

// Node state enum values
export const NodeState = {
  FOLLOWER: 'Follower',
  CANDIDATE: 'Candidate',
  LEADER: 'Leader',
} as const;

export type NodeStateType = typeof NodeState[keyof typeof NodeState];
