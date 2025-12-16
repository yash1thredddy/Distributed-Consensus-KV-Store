import { useState, useEffect, useRef, useCallback } from 'react';
import { AggregatedClusterState } from './useMultiNodeWebSocket';

// Data point for time-series charts
export interface MetricDataPoint {
  timestamp: number;
  time: string; // formatted time for display
  term: number;
  commitIndex: number;
  lastApplied: number;
  leaderCount: number;
  followerCount: number;
  candidateCount: number;
  connectedNodes: number;
  totalNodes: number;
  replicationLag: number;
  // Per-node commit indices for replication chart
  nodeCommits: Record<string, number>;
}

interface UseMetricsHistoryOptions {
  maxDataPoints?: number;
  sampleIntervalMs?: number;
}

interface UseMetricsHistoryResult {
  history: MetricDataPoint[];
  clearHistory: () => void;
}

export function useMetricsHistory(
  aggregatedState: AggregatedClusterState | null,
  connectedCount: number,
  totalNodes: number,
  options: UseMetricsHistoryOptions = {}
): UseMetricsHistoryResult {
  const { maxDataPoints = 60, sampleIntervalMs = 1000 } = options;

  const [history, setHistory] = useState<MetricDataPoint[]>([]);
  const lastSampleTimeRef = useRef<number>(0);

  const clearHistory = useCallback(() => {
    setHistory([]);
  }, []);

  useEffect(() => {
    if (!aggregatedState) return;

    const now = Date.now();

    // Only sample at the specified interval
    if (now - lastSampleTimeRef.current < sampleIntervalMs) {
      return;
    }
    lastSampleTimeRef.current = now;

    // Calculate metrics
    const nodes = aggregatedState.nodes;
    const commitIndices = nodes.map(n => n.commit_index);
    const maxCommit = commitIndices.length > 0 ? Math.max(...commitIndices) : 0;
    const minCommit = commitIndices.length > 0 ? Math.min(...commitIndices) : 0;

    const leaderCount = nodes.filter(n => n.is_leader).length;
    const followerCount = nodes.filter(n => n.state === 'Follower').length;
    const candidateCount = nodes.filter(n => n.state === 'Candidate').length;

    // Per-node commit indices
    const nodeCommits: Record<string, number> = {};
    nodes.forEach(n => {
      nodeCommits[n.id] = n.commit_index;
    });

    const dataPoint: MetricDataPoint = {
      timestamp: now,
      time: new Date(now).toLocaleTimeString(),
      term: aggregatedState.term,
      commitIndex: maxCommit,
      lastApplied: Math.max(...nodes.map(n => n.last_applied), 0),
      leaderCount,
      followerCount,
      candidateCount,
      connectedNodes: connectedCount,
      totalNodes,
      replicationLag: maxCommit - minCommit,
      nodeCommits,
    };

    setHistory(prev => {
      const newHistory = [...prev, dataPoint];
      // Keep only the last maxDataPoints
      if (newHistory.length > maxDataPoints) {
        return newHistory.slice(-maxDataPoints);
      }
      return newHistory;
    });
  }, [aggregatedState, connectedCount, totalNodes, maxDataPoints, sampleIntervalMs]);

  return { history, clearHistory };
}
