import { useState, useEffect, useCallback, useRef } from 'react';
import { ClusterState, NodeState, ConnectionStatus } from '../types';

interface NodeConnection {
  nodeId: string;
  port: number;
  ws: WebSocket | null;
  status: ConnectionStatus;
  state: NodeState | null;
  error: string | null;
}

interface UseMultiNodeWebSocketOptions {
  nodes: { nodeId: string; port: number }[];
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
}

interface UseMultiNodeWebSocketResult {
  nodeConnections: Map<string, NodeConnection>;
  aggregatedState: AggregatedClusterState | null;
  reconnectAll: () => void;
  reconnectNode: (nodeId: string) => void;
}

export interface AggregatedClusterState {
  nodes: NodeState[];
  leader_id: string;
  term: number;
  timestamp: number;
}

export function useMultiNodeWebSocket({
  nodes,
  reconnectInterval = 3000,
  maxReconnectAttempts = 10,
}: UseMultiNodeWebSocketOptions): UseMultiNodeWebSocketResult {
  const [nodeConnections, setNodeConnections] = useState<Map<string, NodeConnection>>(new Map());
  const [aggregatedState, setAggregatedState] = useState<AggregatedClusterState | null>(null);

  const reconnectAttemptsRef = useRef<Map<string, number>>(new Map());
  const reconnectTimeoutsRef = useRef<Map<string, number>>(new Map());
  const wsRef = useRef<Map<string, WebSocket>>(new Map());

  const updateNodeConnection = useCallback((nodeId: string, update: Partial<NodeConnection>) => {
    setNodeConnections((prev) => {
      const newMap = new Map(prev);
      const existing = newMap.get(nodeId) || {
        nodeId,
        port: 0,
        ws: null,
        status: 'disconnected' as ConnectionStatus,
        state: null,
        error: null,
      };
      newMap.set(nodeId, { ...existing, ...update });
      return newMap;
    });
  }, []);

  const connectNode = useCallback((nodeId: string, port: number) => {
    // Close existing connection if any
    const existingWs = wsRef.current.get(nodeId);
    if (existingWs && existingWs.readyState === WebSocket.OPEN) {
      return;
    }

    updateNodeConnection(nodeId, { status: 'connecting', error: null, port });

    try {
      const url = `ws://localhost:${port}/ws/cluster`;
      const ws = new WebSocket(url);
      wsRef.current.set(nodeId, ws);

      ws.onopen = () => {
        updateNodeConnection(nodeId, { status: 'connected', ws });
        reconnectAttemptsRef.current.set(nodeId, 0);
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data) as ClusterState;
          const nodeState: NodeState = {
            id: data.node_id,
            state: data.state,
            term: data.term,
            commit_index: data.commit_index,
            last_applied: data.last_applied,
            is_leader: data.state === 'Leader',
            leader_id: data.leader_id,
          };
          updateNodeConnection(nodeId, { state: nodeState });
        } catch (e) {
          console.error(`Failed to parse WebSocket message from ${nodeId}:`, e);
        }
      };

      ws.onerror = () => {
        updateNodeConnection(nodeId, { error: 'Connection error' });
      };

      ws.onclose = () => {
        updateNodeConnection(nodeId, { status: 'disconnected', ws: null });
        wsRef.current.delete(nodeId);

        // Attempt to reconnect
        const attempts = reconnectAttemptsRef.current.get(nodeId) || 0;
        if (attempts < maxReconnectAttempts) {
          reconnectAttemptsRef.current.set(nodeId, attempts + 1);
          const timeout = window.setTimeout(() => {
            connectNode(nodeId, port);
          }, reconnectInterval);
          reconnectTimeoutsRef.current.set(nodeId, timeout);
        } else {
          updateNodeConnection(nodeId, { error: 'Max reconnection attempts reached' });
        }
      };
    } catch (e) {
      updateNodeConnection(nodeId, {
        error: `Failed to connect: ${e}`,
        status: 'disconnected',
      });
    }
  }, [updateNodeConnection, reconnectInterval, maxReconnectAttempts]);

  const reconnectNode = useCallback((nodeId: string) => {
    const node = nodes.find(n => n.nodeId === nodeId);
    if (node) {
      reconnectAttemptsRef.current.set(nodeId, 0);
      const existingWs = wsRef.current.get(nodeId);
      if (existingWs) {
        existingWs.close();
      }
      connectNode(nodeId, node.port);
    }
  }, [nodes, connectNode]);

  const reconnectAll = useCallback(() => {
    nodes.forEach(({ nodeId, port }) => {
      reconnectAttemptsRef.current.set(nodeId, 0);
      const existingWs = wsRef.current.get(nodeId);
      if (existingWs) {
        existingWs.close();
      }
      connectNode(nodeId, port);
    });
  }, [nodes, connectNode]);

  // Connect to all nodes on mount
  useEffect(() => {
    nodes.forEach(({ nodeId, port }) => {
      connectNode(nodeId, port);
    });

    return () => {
      // Cleanup
      reconnectTimeoutsRef.current.forEach((timeout) => clearTimeout(timeout));
      wsRef.current.forEach((ws) => ws.close());
      wsRef.current.clear();
    };
  }, [nodes, connectNode]);

  // Aggregate state from all nodes
  useEffect(() => {
    const nodeStates: NodeState[] = [];
    let maxTerm = 0;
    let leaderId = '';

    nodeConnections.forEach((conn) => {
      if (conn.state) {
        nodeStates.push(conn.state);
        if (conn.state.term > maxTerm) {
          maxTerm = conn.state.term;
        }
        if (conn.state.is_leader) {
          leaderId = conn.state.id;
        }
        // Also check leader_id from follower's perspective
        if (conn.state.leader_id && !leaderId) {
          leaderId = conn.state.leader_id;
        }
      }
    });

    if (nodeStates.length > 0) {
      // Sort nodes by ID for consistent display
      nodeStates.sort((a, b) => a.id.localeCompare(b.id));

      setAggregatedState({
        nodes: nodeStates,
        leader_id: leaderId,
        term: maxTerm,
        timestamp: Date.now(),
      });
    }
  }, [nodeConnections]);

  return {
    nodeConnections,
    aggregatedState,
    reconnectAll,
    reconnectNode,
  };
}
