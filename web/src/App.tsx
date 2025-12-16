import { useWebSocket } from './hooks/useWebSocket';
import { ClusterHealth } from './components/ClusterHealth';
import { MetricsChart } from './components/MetricsChart';
import { ConnectionStatus } from './components/ConnectionStatus';

function App() {
  // Default WebSocket URL - can be configured via environment variable
  const wsUrl = import.meta.env.VITE_WS_URL || 'ws://localhost:8080/ws/cluster';

  const { clusterState, connectionStatus, error, reconnect } = useWebSocket({
    url: wsUrl,
  });

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
            <ConnectionStatus
              status={connectionStatus}
              error={error}
              onReconnect={reconnect}
            />
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {/* Cluster Health */}
        <ClusterHealth
          clusterState={clusterState}
          connectionStatus={connectionStatus}
        />

        {/* Metrics Chart */}
        <MetricsChart clusterState={clusterState} />

        {/* Info Panel */}
        <div className="bg-gray-800 rounded-lg p-6">
          <h2 className="text-xl font-bold mb-4">Cluster Information</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <InfoCard
              label="Node ID"
              value={clusterState?.node_id || '-'}
            />
            <InfoCard
              label="Current Term"
              value={clusterState?.term?.toString() || '-'}
            />
            <InfoCard
              label="Commit Index"
              value={clusterState?.commit_index?.toString() || '-'}
            />
            <InfoCard
              label="Last Applied"
              value={clusterState?.last_applied?.toString() || '-'}
            />
          </div>
        </div>

        {/* Raw Data (for debugging) */}
        <details className="bg-gray-800 rounded-lg p-6">
          <summary className="text-lg font-bold cursor-pointer text-gray-300 hover:text-white">
            Raw WebSocket Data
          </summary>
          <pre className="mt-4 p-4 bg-gray-900 rounded-lg text-sm text-gray-300 overflow-auto">
            {clusterState
              ? JSON.stringify(clusterState, null, 2)
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

interface InfoCardProps {
  label: string;
  value: string;
}

function InfoCard({ label, value }: InfoCardProps) {
  return (
    <div className="bg-gray-700 rounded-lg p-4">
      <p className="text-gray-400 text-sm">{label}</p>
      <p className="text-white text-xl font-mono font-bold">{value}</p>
    </div>
  );
}

export default App;
