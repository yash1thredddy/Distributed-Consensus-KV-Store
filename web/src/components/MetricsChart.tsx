import { useState, useEffect } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { ClusterState } from '../types';

interface MetricsChartProps {
  clusterState: ClusterState | null;
}

interface DataPoint {
  time: string;
  commitIndex: number;
  lastApplied: number;
}

const MAX_DATA_POINTS = 60;

export function MetricsChart({ clusterState }: MetricsChartProps) {
  const [data, setData] = useState<DataPoint[]>([]);

  useEffect(() => {
    if (!clusterState) return;

    const newPoint: DataPoint = {
      time: new Date(clusterState.timestamp).toLocaleTimeString(),
      commitIndex: clusterState.commit_index,
      lastApplied: clusterState.last_applied,
    };

    setData((prev) => {
      const updated = [...prev, newPoint];
      if (updated.length > MAX_DATA_POINTS) {
        return updated.slice(-MAX_DATA_POINTS);
      }
      return updated;
    });
  }, [clusterState]);

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <h2 className="text-xl font-bold text-white mb-4">Replication Metrics</h2>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="time"
              stroke="#9CA3AF"
              tick={{ fill: '#9CA3AF', fontSize: 12 }}
            />
            <YAxis stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 12 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1F2937',
                border: '1px solid #374151',
                borderRadius: '0.5rem',
              }}
              labelStyle={{ color: '#F3F4F6' }}
              itemStyle={{ color: '#F3F4F6' }}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="commitIndex"
              stroke="#10B981"
              strokeWidth={2}
              dot={false}
              name="Commit Index"
            />
            <Line
              type="monotone"
              dataKey="lastApplied"
              stroke="#3B82F6"
              strokeWidth={2}
              dot={false}
              name="Last Applied"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
