import { NodeStatus, NodeState, ConnectionStatus } from '../types';

interface NodeCardProps {
  node: NodeStatus;
  isLeader: boolean;
  connectionStatus: ConnectionStatus;
}

export function NodeCard({ node, isLeader, connectionStatus }: NodeCardProps) {
  const getStateColor = () => {
    if (connectionStatus !== 'connected') {
      return 'bg-gray-700 border-gray-600';
    }
    switch (node.state) {
      case NodeState.LEADER:
        return 'bg-green-900 border-green-500';
      case NodeState.CANDIDATE:
        return 'bg-yellow-900 border-yellow-500';
      case NodeState.FOLLOWER:
      default:
        return 'bg-blue-900 border-blue-500';
    }
  };

  const getStateTextColor = () => {
    if (connectionStatus !== 'connected') {
      return 'text-gray-400';
    }
    switch (node.state) {
      case NodeState.LEADER:
        return 'text-green-400';
      case NodeState.CANDIDATE:
        return 'text-yellow-400';
      case NodeState.FOLLOWER:
      default:
        return 'text-blue-400';
    }
  };

  const getConnectionIndicator = () => {
    switch (connectionStatus) {
      case 'connected':
        return <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>;
      case 'connecting':
        return <span className="w-2 h-2 bg-yellow-500 rounded-full animate-pulse"></span>;
      case 'disconnected':
      default:
        return <span className="w-2 h-2 bg-red-500 rounded-full"></span>;
    }
  };

  return (
    <div className={`rounded-lg border-2 p-4 ${getStateColor()} transition-all duration-300`}>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-lg font-bold text-white flex items-center gap-2">
          {node.id}
          {isLeader && (
            <span className="text-xs bg-green-600 text-white px-2 py-0.5 rounded">
              LEADER
            </span>
          )}
        </h3>
        {getConnectionIndicator()}
      </div>

      <div className="space-y-2 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-400">State:</span>
          <span className={`font-semibold ${getStateTextColor()}`}>
            {node.state}
          </span>
        </div>

        <div className="flex justify-between">
          <span className="text-gray-400">Term:</span>
          <span className="text-white font-mono">{node.term}</span>
        </div>

        <div className="flex justify-between">
          <span className="text-gray-400">Commit Index:</span>
          <span className="text-white font-mono">{node.commit_index}</span>
        </div>

        <div className="flex justify-between">
          <span className="text-gray-400">Last Applied:</span>
          <span className="text-white font-mono">{node.last_applied}</span>
        </div>
      </div>
    </div>
  );
}
