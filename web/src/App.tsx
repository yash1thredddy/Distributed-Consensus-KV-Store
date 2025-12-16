import { useState, useMemo } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
} from 'recharts';
import { useMultiNodeWebSocket, AggregatedClusterState } from './hooks/useMultiNodeWebSocket';
import { useMetricsHistory, MetricDataPoint } from './hooks/useMetricsHistory';
import { NodeState, ConnectionStatus } from './types';

// Default cluster nodes - can be customized via UI
const DEFAULT_CLUSTER_NODES = [
  { nodeId: 'node1', port: 8081 },
  { nodeId: 'node2', port: 8082 },
  { nodeId: 'node3', port: 8083 },
  { nodeId: 'node4', port: 8084 },
  { nodeId: 'node5', port: 8085 },
];

// Threshold for switching to summary view
const SUMMARY_VIEW_THRESHOLD = 10;

// Load saved config from localStorage
function loadClusterNodes(): { nodeId: string; port: number }[] {
  const saved = localStorage.getItem('cluster_nodes');
  if (saved) {
    try {
      return JSON.parse(saved);
    } catch {
      // Invalid JSON, use default
    }
  }
  return DEFAULT_CLUSTER_NODES;
}

// Save config to localStorage
function saveClusterNodes(nodes: { nodeId: string; port: number }[]) {
  localStorage.setItem('cluster_nodes', JSON.stringify(nodes));
}

function App() {
  const [clusterNodes, setClusterNodes] = useState(loadClusterNodes);
  const [showConfig, setShowConfig] = useState(false);
  const [newNodeId, setNewNodeId] = useState('');
  const [newNodePort, setNewNodePort] = useState('');
  const [viewMode, setViewMode] = useState<'auto' | 'cards' | 'summary'>('auto');

  const { nodeConnections, aggregatedState, reconnectAll, reconnectNode } = useMultiNodeWebSocket({
    nodes: clusterNodes,
  });

  // Count connected nodes
  const connectedCount = Array.from(nodeConnections.values()).filter(
    (conn) => conn.status === 'connected'
  ).length;

  // Use metrics history for graphs
  const { history, clearHistory } = useMetricsHistory(
    aggregatedState,
    connectedCount,
    clusterNodes.length,
    { maxDataPoints: 60, sampleIntervalMs: 1000 }
  );

  const overallStatus: ConnectionStatus =
    connectedCount === 0 ? 'disconnected' :
    connectedCount < clusterNodes.length ? 'connecting' : 'connected';

  // Determine actual view mode
  const actualViewMode = viewMode === 'auto'
    ? (clusterNodes.length > SUMMARY_VIEW_THRESHOLD ? 'summary' : 'cards')
    : viewMode;

  // Add a new node
  const handleAddNode = () => {
    const port = parseInt(newNodePort, 10);
    if (newNodeId && !isNaN(port) && port > 0) {
      if (clusterNodes.some(n => n.nodeId === newNodeId)) {
        alert('Node ID already exists');
        return;
      }
      const newNodes = [...clusterNodes, { nodeId: newNodeId, port }];
      setClusterNodes(newNodes);
      saveClusterNodes(newNodes);
      setNewNodeId('');
      setNewNodePort('');
    }
  };

  // Remove a node
  const handleRemoveNode = (nodeId: string) => {
    const newNodes = clusterNodes.filter(n => n.nodeId !== nodeId);
    setClusterNodes(newNodes);
    saveClusterNodes(newNodes);
  };

  // Reset to defaults
  const handleResetToDefaults = () => {
    setClusterNodes(DEFAULT_CLUSTER_NODES);
    localStorage.removeItem('cluster_nodes');
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      {/* Header */}
      <header className="bg-gray-800 border-b border-gray-700">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold">Distributed KV Store</h1>
              <p className="text-gray-400 text-sm">Raft Cluster Dashboard</p>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-sm text-gray-400">
                {connectedCount}/{clusterNodes.length} nodes connected
              </span>
              <StatusIndicator status={overallStatus} />
              <select
                value={viewMode}
                onChange={(e) => setViewMode(e.target.value as 'auto' | 'cards' | 'summary')}
                className="px-3 py-2 bg-gray-700 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="auto">Auto View</option>
                <option value="cards">Card View</option>
                <option value="summary">Summary View</option>
              </select>
              <button
                onClick={() => setShowConfig(!showConfig)}
                className="px-4 py-2 bg-gray-600 hover:bg-gray-700 rounded-lg text-sm font-medium transition-colors"
              >
                {showConfig ? 'Hide Config' : 'Configure'}
              </button>
              <button
                onClick={reconnectAll}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-sm font-medium transition-colors"
              >
                Reconnect All
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Configuration Panel */}
      {showConfig && (
        <div className="bg-gray-800 border-b border-gray-700">
          <div className="max-w-7xl mx-auto px-4 py-4">
            <h3 className="text-lg font-bold mb-4">Cluster Configuration</h3>
            <div className="flex flex-wrap gap-4 mb-4">
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  placeholder="Node ID (e.g., node6)"
                  value={newNodeId}
                  onChange={(e) => setNewNodeId(e.target.value)}
                  className="px-3 py-2 bg-gray-700 rounded-lg text-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <input
                  type="number"
                  placeholder="Port (e.g., 8086)"
                  value={newNodePort}
                  onChange={(e) => setNewNodePort(e.target.value)}
                  className="px-3 py-2 bg-gray-700 rounded-lg text-white text-sm w-32 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <button
                  onClick={handleAddNode}
                  className="px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg text-sm font-medium transition-colors"
                >
                  Add Node
                </button>
              </div>
              <button
                onClick={handleResetToDefaults}
                className="px-4 py-2 bg-yellow-600 hover:bg-yellow-700 rounded-lg text-sm font-medium transition-colors"
              >
                Reset to Defaults
              </button>
              <button
                onClick={clearHistory}
                className="px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm font-medium transition-colors"
              >
                Clear Graph History
              </button>
            </div>
            <div className="text-sm text-gray-400">
              Current nodes: {clusterNodes.map(n => `${n.nodeId}:${n.port}`).join(', ')}
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {/* Cluster Overview - Always shown */}
        <ClusterOverview
          aggregatedState={aggregatedState}
          connectedCount={connectedCount}
          totalNodes={clusterNodes.length}
          nodeConnections={nodeConnections}
        />

        {/* Cluster Health - Card or Summary view */}
        {actualViewMode === 'cards' ? (
          <ClusterHealthCards
            clusterNodes={clusterNodes}
            aggregatedState={aggregatedState}
            nodeConnections={nodeConnections}
            onRemoveNode={handleRemoveNode}
            onReconnectNode={reconnectNode}
          />
        ) : (
          <ClusterHealthSummary
            aggregatedState={aggregatedState}
            nodeConnections={nodeConnections}
            clusterNodes={clusterNodes}
          />
        )}

        {/* Real-time Graphs */}
        <MetricsGraphs history={history} aggregatedState={aggregatedState} />

        {/* Raw Data (for debugging) */}
        <details className="bg-gray-800 rounded-lg p-6">
          <summary className="text-lg font-bold cursor-pointer text-gray-300 hover:text-white">
            Raw WebSocket Data
          </summary>
          <pre className="mt-4 p-4 bg-gray-900 rounded-lg text-sm text-gray-300 overflow-auto max-h-64">
            {aggregatedState
              ? JSON.stringify(aggregatedState, null, 2)
              : 'No data received'}
          </pre>
        </details>
      </main>

      {/* Footer */}
      <footer className="bg-gray-800 border-t border-gray-700 mt-8">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <p className="text-center text-gray-400 text-sm">
            Distributed KV Store - Raft Consensus Implementation
          </p>
        </div>
      </footer>
    </div>
  );
}

// Status indicator component
function StatusIndicator({ status }: { status: ConnectionStatus }) {
  const colors = {
    connected: 'bg-green-500',
    connecting: 'bg-yellow-500 animate-pulse',
    disconnected: 'bg-red-500',
  };

  const labels = {
    connected: 'Connected',
    connecting: 'Connecting...',
    disconnected: 'Disconnected',
  };

  return (
    <div className="flex items-center gap-2">
      <div className={`w-3 h-3 rounded-full ${colors[status]}`} />
      <span className="text-sm">{labels[status]}</span>
    </div>
  );
}

// Cluster Overview - Production-ready summary
interface ClusterOverviewProps {
  aggregatedState: AggregatedClusterState | null;
  connectedCount: number;
  totalNodes: number;
  nodeConnections: Map<string, { status: ConnectionStatus; state: NodeState | null; error: string | null }>;
}

function ClusterOverview({ aggregatedState, connectedCount, totalNodes, nodeConnections }: ClusterOverviewProps) {
  const unhealthyNodes = Array.from(nodeConnections.entries())
    .filter(([, conn]) => conn.status !== 'connected')
    .map(([nodeId]) => nodeId);

  const healthPercent = totalNodes > 0 ? Math.round((connectedCount / totalNodes) * 100) : 0;

  // Calculate metrics
  const nodes = aggregatedState?.nodes || [];
  const commitIndices = nodes.map(n => n.commit_index);
  const maxCommit = commitIndices.length > 0 ? Math.max(...commitIndices) : 0;
  const minCommit = commitIndices.length > 0 ? Math.min(...commitIndices) : 0;
  const replicationLag = maxCommit - minCommit;

  const leaderCount = nodes.filter(n => n.is_leader).length;

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-bold">Cluster Overview</h2>
        {aggregatedState && (
          <div className="flex items-center gap-4 text-sm">
            <span className="px-3 py-1 bg-green-600 rounded-full font-medium">
              Leader: {aggregatedState.leader_id || 'Electing...'}
            </span>
            <span className="px-3 py-1 bg-blue-600 rounded-full font-medium">
              Term: {aggregatedState.term}
            </span>
          </div>
        )}
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
        {/* Health */}
        <div className="bg-gray-700 rounded-lg p-4">
          <p className="text-gray-400 text-xs uppercase tracking-wider">Cluster Health</p>
          <p className={`text-3xl font-bold ${healthPercent >= 80 ? 'text-green-400' : healthPercent >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
            {healthPercent}%
          </p>
          <p className="text-gray-400 text-xs">{connectedCount}/{totalNodes} nodes</p>
        </div>

        {/* Leader Status */}
        <div className="bg-gray-700 rounded-lg p-4">
          <p className="text-gray-400 text-xs uppercase tracking-wider">Leaders</p>
          <p className={`text-3xl font-bold ${leaderCount === 1 ? 'text-green-400' : leaderCount === 0 ? 'text-red-400' : 'text-yellow-400'}`}>
            {leaderCount}
          </p>
          <p className="text-gray-400 text-xs">{leaderCount === 1 ? 'Healthy' : leaderCount === 0 ? 'Electing' : 'Split!'}</p>
        </div>

        {/* Replication Lag */}
        <div className="bg-gray-700 rounded-lg p-4">
          <p className="text-gray-400 text-xs uppercase tracking-wider">Replication Lag</p>
          <p className={`text-3xl font-bold ${replicationLag === 0 ? 'text-green-400' : replicationLag < 10 ? 'text-yellow-400' : 'text-red-400'}`}>
            {replicationLag}
          </p>
          <p className="text-gray-400 text-xs">entries behind</p>
        </div>

        {/* Commit Index */}
        <div className="bg-gray-700 rounded-lg p-4">
          <p className="text-gray-400 text-xs uppercase tracking-wider">Commit Index</p>
          <p className="text-3xl font-bold text-white font-mono">{maxCommit}</p>
          <p className="text-gray-400 text-xs">highest</p>
        </div>

        {/* Current Term */}
        <div className="bg-gray-700 rounded-lg p-4">
          <p className="text-gray-400 text-xs uppercase tracking-wider">Current Term</p>
          <p className="text-3xl font-bold text-blue-400 font-mono">{aggregatedState?.term || 0}</p>
          <p className="text-gray-400 text-xs">consensus round</p>
        </div>

        {/* Unhealthy Nodes */}
        <div className="bg-gray-700 rounded-lg p-4">
          <p className="text-gray-400 text-xs uppercase tracking-wider">Unhealthy</p>
          <p className={`text-3xl font-bold ${unhealthyNodes.length === 0 ? 'text-green-400' : 'text-red-400'}`}>
            {unhealthyNodes.length}
          </p>
          <p className="text-gray-400 text-xs truncate" title={unhealthyNodes.join(', ')}>
            {unhealthyNodes.length > 0 ? unhealthyNodes.slice(0, 3).join(', ') + (unhealthyNodes.length > 3 ? '...' : '') : 'All healthy'}
          </p>
        </div>
      </div>
    </div>
  );
}

// Cluster health with individual cards
interface ClusterHealthCardsProps {
  clusterNodes: { nodeId: string; port: number }[];
  aggregatedState: AggregatedClusterState | null;
  nodeConnections: Map<string, { status: ConnectionStatus; state: NodeState | null; error: string | null }>;
  onRemoveNode: (nodeId: string) => void;
  onReconnectNode: (nodeId: string) => void;
}

function ClusterHealthCards({ clusterNodes, aggregatedState, nodeConnections, onRemoveNode, onReconnectNode }: ClusterHealthCardsProps) {
  if (clusterNodes.length === 0) {
    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h2 className="text-xl font-bold text-white mb-4">Node Details</h2>
        <div className="flex items-center justify-center h-32">
          <p className="text-gray-400">No nodes configured. Add nodes using the Configure button.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <h2 className="text-xl font-bold text-white mb-4">Node Details</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
        {clusterNodes.map(({ nodeId }) => {
          const conn = nodeConnections.get(nodeId);
          const nodeState = aggregatedState?.nodes.find(n => n.id === nodeId);

          return (
            <NodeCard
              key={nodeId}
              nodeId={nodeId}
              nodeState={nodeState || null}
              connectionStatus={conn?.status || 'disconnected'}
              isLeader={nodeState?.is_leader || false}
              error={conn?.error || null}
              onRemove={() => onRemoveNode(nodeId)}
              onReconnect={() => onReconnectNode(nodeId)}
            />
          );
        })}
      </div>
    </div>
  );
}

// Summary view for large clusters
interface ClusterHealthSummaryProps {
  aggregatedState: AggregatedClusterState | null;
  nodeConnections: Map<string, { status: ConnectionStatus; state: NodeState | null; error: string | null }>;
  clusterNodes: { nodeId: string; port: number }[];
}

function ClusterHealthSummary({ aggregatedState, nodeConnections, clusterNodes }: ClusterHealthSummaryProps) {
  // Group nodes by state
  const nodesByState = useMemo(() => {
    const groups: Record<string, string[]> = {
      Leader: [],
      Follower: [],
      Candidate: [],
      Disconnected: [],
    };

    clusterNodes.forEach(({ nodeId }) => {
      const conn = nodeConnections.get(nodeId);
      const nodeState = aggregatedState?.nodes.find(n => n.id === nodeId);

      if (!conn || conn.status !== 'connected' || !nodeState) {
        groups.Disconnected.push(nodeId);
      } else {
        const state = nodeState.state;
        if (groups[state]) {
          groups[state].push(nodeId);
        } else {
          groups.Follower.push(nodeId); // Default
        }
      }
    });

    return groups;
  }, [aggregatedState, nodeConnections, clusterNodes]);

  const stateColors: Record<string, { bg: string; text: string }> = {
    Leader: { bg: 'bg-green-600', text: 'text-green-400' },
    Follower: { bg: 'bg-blue-600', text: 'text-blue-400' },
    Candidate: { bg: 'bg-yellow-600', text: 'text-yellow-400' },
    Disconnected: { bg: 'bg-red-600', text: 'text-red-400' },
  };

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <h2 className="text-xl font-bold text-white mb-4">Node Summary</h2>
      <div className="space-y-4">
        {Object.entries(nodesByState).map(([state, nodes]) => (
          <div key={state} className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <span className={`px-2 py-1 rounded text-sm font-medium ${stateColors[state]?.bg || 'bg-gray-600'}`}>
                  {state}
                </span>
                <span className={`text-2xl font-bold ${stateColors[state]?.text || 'text-white'}`}>
                  {nodes.length}
                </span>
                <span className="text-gray-400">nodes</span>
              </div>
            </div>
            {nodes.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {nodes.map(nodeId => (
                  <span
                    key={nodeId}
                    className="px-2 py-1 bg-gray-800 rounded text-sm text-gray-300 font-mono"
                  >
                    {nodeId}
                  </span>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

// Node card component
interface NodeCardProps {
  nodeId: string;
  nodeState: NodeState | null;
  connectionStatus: ConnectionStatus;
  isLeader: boolean;
  error: string | null;
  onRemove: () => void;
  onReconnect: () => void;
}

function NodeCard({ nodeId, nodeState, connectionStatus, isLeader, error, onRemove, onReconnect }: NodeCardProps) {
  const stateColors: Record<string, string> = {
    Leader: 'text-green-400',
    Follower: 'text-blue-400',
    Candidate: 'text-yellow-400',
  };

  const borderColor = isLeader
    ? 'border-green-500'
    : connectionStatus === 'connected'
      ? 'border-gray-600'
      : 'border-red-500';

  return (
    <div className={`bg-gray-700 rounded-lg p-4 border-2 ${borderColor} relative group`}>
      {/* Action buttons - show on hover */}
      <div className="absolute top-2 right-2 flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <button
          onClick={onReconnect}
          className="p-1 bg-blue-600 hover:bg-blue-700 rounded text-xs"
          title="Reconnect"
        >
          ↻
        </button>
        <button
          onClick={onRemove}
          className="p-1 bg-red-600 hover:bg-red-700 rounded text-xs"
          title="Remove"
        >
          ×
        </button>
      </div>

      <div className="flex items-center justify-between mb-2">
        <span className="font-bold text-white">{nodeId}</span>
        <div className={`w-2 h-2 rounded-full ${
          connectionStatus === 'connected' ? 'bg-green-500' :
          connectionStatus === 'connecting' ? 'bg-yellow-500 animate-pulse' : 'bg-red-500'
        }`} />
      </div>

      {nodeState ? (
        <div className="space-y-1 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-400">State:</span>
            <span className={stateColors[nodeState.state] || 'text-gray-300'}>
              {nodeState.state}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Term:</span>
            <span className="text-white font-mono">{nodeState.term}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Commit:</span>
            <span className="text-white font-mono">{nodeState.commit_index}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Applied:</span>
            <span className="text-white font-mono">{nodeState.last_applied}</span>
          </div>
        </div>
      ) : (
        <div className="text-gray-400 text-sm">
          {error || (connectionStatus === 'connecting' ? 'Connecting...' : 'Disconnected')}
        </div>
      )}
    </div>
  );
}

// Real-time metrics graphs
interface MetricsGraphsProps {
  history: MetricDataPoint[];
  aggregatedState: AggregatedClusterState | null;
}

function MetricsGraphs({ history, aggregatedState }: MetricsGraphsProps) {
  if (history.length < 2) {
    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h2 className="text-xl font-bold mb-4">Real-time Metrics</h2>
        <div className="flex items-center justify-center h-64">
          <p className="text-gray-400">Collecting data... (graphs will appear after a few seconds)</p>
        </div>
      </div>
    );
  }

  // Get unique node IDs for replication chart
  const nodeIds = aggregatedState?.nodes.map(n => n.id).sort() || [];
  const nodeColors = ['#22c55e', '#3b82f6', '#eab308', '#ef4444', '#a855f7', '#06b6d4', '#f97316', '#ec4899'];

  return (
    <div className="space-y-6">
      {/* Commit Index & Replication */}
      <div className="bg-gray-800 rounded-lg p-6">
        <h2 className="text-xl font-bold mb-4">Commit Index Over Time</h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="time" stroke="#9ca3af" tick={{ fontSize: 12 }} />
              <YAxis stroke="#9ca3af" tick={{ fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: '8px' }}
                labelStyle={{ color: '#fff' }}
              />
              <Legend />
              <Area
                type="monotone"
                dataKey="commitIndex"
                name="Max Commit Index"
                stroke="#22c55e"
                fill="#22c55e"
                fillOpacity={0.3}
              />
              <Area
                type="monotone"
                dataKey="lastApplied"
                name="Last Applied"
                stroke="#3b82f6"
                fill="#3b82f6"
                fillOpacity={0.3}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Per-Node Replication */}
      {nodeIds.length > 0 && (
        <div className="bg-gray-800 rounded-lg p-6">
          <h2 className="text-xl font-bold mb-4">Per-Node Commit Index</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={history}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="time" stroke="#9ca3af" tick={{ fontSize: 12 }} />
                <YAxis stroke="#9ca3af" tick={{ fontSize: 12 }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Legend />
                {nodeIds.map((nodeId, index) => (
                  <Line
                    key={nodeId}
                    type="monotone"
                    dataKey={`nodeCommits.${nodeId}`}
                    name={nodeId}
                    stroke={nodeColors[index % nodeColors.length]}
                    dot={false}
                    strokeWidth={2}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Cluster Health */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Node States Distribution */}
        <div className="bg-gray-800 rounded-lg p-6">
          <h2 className="text-xl font-bold mb-4">Node States Over Time</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={history}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="time" stroke="#9ca3af" tick={{ fontSize: 12 }} />
                <YAxis stroke="#9ca3af" tick={{ fontSize: 12 }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Legend />
                <Area
                  type="stepAfter"
                  dataKey="leaderCount"
                  name="Leaders"
                  stackId="1"
                  stroke="#22c55e"
                  fill="#22c55e"
                />
                <Area
                  type="stepAfter"
                  dataKey="followerCount"
                  name="Followers"
                  stackId="1"
                  stroke="#3b82f6"
                  fill="#3b82f6"
                />
                <Area
                  type="stepAfter"
                  dataKey="candidateCount"
                  name="Candidates"
                  stackId="1"
                  stroke="#eab308"
                  fill="#eab308"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Replication Lag */}
        <div className="bg-gray-800 rounded-lg p-6">
          <h2 className="text-xl font-bold mb-4">Replication Lag</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={history}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="time" stroke="#9ca3af" tick={{ fontSize: 12 }} />
                <YAxis stroke="#9ca3af" tick={{ fontSize: 12 }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="replicationLag"
                  name="Replication Lag"
                  stroke="#ef4444"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Term History */}
      <div className="bg-gray-800 rounded-lg p-6">
        <h2 className="text-xl font-bold mb-4">Term History (Leader Elections)</h2>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="time" stroke="#9ca3af" tick={{ fontSize: 12 }} />
              <YAxis stroke="#9ca3af" tick={{ fontSize: 12 }} domain={['dataMin', 'dataMax']} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: '8px' }}
                labelStyle={{ color: '#fff' }}
              />
              <Legend />
              <Line
                type="stepAfter"
                dataKey="term"
                name="Current Term"
                stroke="#a855f7"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}

export default App;
