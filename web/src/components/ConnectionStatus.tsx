import { ConnectionStatus as ConnectionStatusType } from '../types';

interface ConnectionStatusProps {
  status: ConnectionStatusType;
  error: string | null;
  onReconnect: () => void;
}

export function ConnectionStatus({ status, error, onReconnect }: ConnectionStatusProps) {
  const getStatusConfig = () => {
    switch (status) {
      case 'connected':
        return {
          color: 'bg-green-500',
          text: 'Connected',
          textColor: 'text-green-400',
        };
      case 'connecting':
        return {
          color: 'bg-yellow-500',
          text: 'Connecting...',
          textColor: 'text-yellow-400',
        };
      case 'disconnected':
      default:
        return {
          color: 'bg-red-500',
          text: 'Disconnected',
          textColor: 'text-red-400',
        };
    }
  };

  const config = getStatusConfig();

  return (
    <div className="flex items-center gap-4">
      <div className="flex items-center gap-2">
        <span className={`w-3 h-3 rounded-full ${config.color} ${status === 'connecting' ? 'animate-pulse' : ''}`}></span>
        <span className={`text-sm font-medium ${config.textColor}`}>
          {config.text}
        </span>
      </div>

      {error && (
        <span className="text-sm text-red-400">{error}</span>
      )}

      {status === 'disconnected' && (
        <button
          onClick={onReconnect}
          className="text-sm bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded transition-colors"
        >
          Reconnect
        </button>
      )}
    </div>
  );
}
