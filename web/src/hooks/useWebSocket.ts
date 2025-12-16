import { useState, useEffect, useCallback, useRef } from 'react';
import { ClusterState, ConnectionStatus } from '../types';

interface UseWebSocketOptions {
  url: string;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
}

interface UseWebSocketResult {
  clusterState: ClusterState | null;
  connectionStatus: ConnectionStatus;
  error: string | null;
  reconnect: () => void;
}

export function useWebSocket({
  url,
  reconnectInterval = 3000,
  maxReconnectAttempts = 10,
}: UseWebSocketOptions): UseWebSocketResult {
  const [clusterState, setClusterState] = useState<ClusterState | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('disconnected');
  const [error, setError] = useState<string | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef<number | null>(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return;
    }

    setConnectionStatus('connecting');
    setError(null);

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        setConnectionStatus('connected');
        reconnectAttemptsRef.current = 0;
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data) as ClusterState;
          setClusterState(data);
        } catch (e) {
          console.error('Failed to parse WebSocket message:', e);
        }
      };

      ws.onerror = () => {
        setError('WebSocket connection error');
      };

      ws.onclose = () => {
        setConnectionStatus('disconnected');
        wsRef.current = null;

        // Attempt to reconnect
        if (reconnectAttemptsRef.current < maxReconnectAttempts) {
          reconnectAttemptsRef.current++;
          reconnectTimeoutRef.current = window.setTimeout(() => {
            connect();
          }, reconnectInterval);
        } else {
          setError('Max reconnection attempts reached');
        }
      };
    } catch (e) {
      setError(`Failed to connect: ${e}`);
      setConnectionStatus('disconnected');
    }
  }, [url, reconnectInterval, maxReconnectAttempts]);

  const reconnect = useCallback(() => {
    reconnectAttemptsRef.current = 0;
    if (wsRef.current) {
      wsRef.current.close();
    }
    connect();
  }, [connect]);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [connect]);

  return {
    clusterState,
    connectionStatus,
    error,
    reconnect,
  };
}
