import { ClusterState, ConnectionStatus } from '../types';
import { NodeCard } from './NodeCard';

interface ClusterHealthProps {
  clusterState: ClusterState | null;
  connectionStatus: ConnectionStatus;
}

export function ClusterHealth({ clusterState, connectionStatus }: ClusterHealthProps) {
  if (!clusterState) {
    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h2 className="text-xl font-bold text-white mb-4">Cluster Health</h2>
        <div className="flex items-center justify-center h-32">
          <p className="text-gray-400">
            {connectionStatus === 'connecting'
              ? 'Connecting to cluster...'
              : 'No cluster data available'}
          </p>
        </div>
      </div>
    );
  }

  // For now, we only have the current node's info
  // In a full implementation, we'd aggregate data from all nodes
  const nodes = [clusterState.node];

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-bold text-white">Cluster Health</h2>
        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-400">Term:</span>
          <span className="text-white font-mono bg-gray-700 px-2 py-1 rounded">
            {clusterState.term}
          </span>
          <span className="text-gray-400 ml-4">Leader:</span>
          <span className="text-green-400 font-mono bg-gray-700 px-2 py-1 rounded">
            {clusterState.leader_id || 'Unknown'}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
        {nodes.map((node) => (
          <NodeCard
            key={node.id}
            node={node}
            isLeader={node.is_leader}
            connectionStatus={connectionStatus}
          />
        ))}
      </div>
    </div>
  );
}
