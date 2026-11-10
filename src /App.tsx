import React, { useState, useEffect } from 'react';
import { streamSimulator } from './engine/streamSimulator';
import { Navigation, NavTab } from './components/Navigation';
import { DAGTopologyView } from './components/DAGTopologyView';
import { WorkerClusterView } from './components/WorkerClusterView';
import { FleetTelemetryView } from './components/FleetTelemetryView';
import { ChaosEngineeringLab } from './components/ChaosEngineeringLab';
import { PrometheusMetricsView } from './components/PrometheusMetricsView';
import { PythonCodebaseViewer } from './components/PythonCodebaseViewer';
import { 
  Activity, 
  Layers, 
  AlertTriangle, 
  CheckCircle2, 
  Cpu, 
  HardDrive 
} from 'lucide-react';

export default function App() {
  const [activeTab, setActiveTab] = useState<NavTab>('topology');
  const [, setTick] = useState(0);

  // Subscribe to simulator updates
  useEffect(() => {
    const unsubscribe = streamSimulator.subscribe(() => {
      setTick(t => t + 1);
    });
    return () => {
      unsubscribe();
    };
  }, []);

  const workers = streamSimulator.getWorkers();
  const partitions = streamSimulator.getPartitions();
  const metrics = streamSimulator.getMetrics();
  const rebalanceHistory = streamSimulator.getRebalanceHistory();
  const latestEvents = streamSimulator.getLatestEvents();
  const recentAnomalies = streamSimulator.getRecentAnomalies();
  const isRunning = streamSimulator.isEngineRunning();
  const throughputMultiplier = streamSimulator.getThroughputMultiplier();

  const handleToggleEngine = () => {
    if (isRunning) {
      streamSimulator.pauseEngine();
    } else {
      streamSimulator.resumeEngine();
    }
  };

  const handleKillWorker = (workerId: number) => {
    streamSimulator.killWorker(workerId);
  };

  const handleReviveWorker = (workerId: number) => {
    streamSimulator.reviveWorker(workerId);
  };

  const handleTriggerRebalance = () => {
    streamSimulator.rebalanceCluster('manual');
  };

  const handleSetThroughputMultiplier = (mult: number) => {
    streamSimulator.setThroughputMode(mult);
  };

  const handleSearchTrucks = (query: string) => {
    return streamSimulator.searchTrucks(query);
  };

  return (
    <div className="flex flex-col min-h-screen bg-slate-950 text-slate-100 font-sans">
      {/* Top Bar Contract Compliant Header */}
      <Navigation
        activeTab={activeTab}
        onTabChange={setActiveTab}
        isEngineRunning={isRunning}
        onToggleEngine={handleToggleEngine}
        activeEps={metrics.currentThroughputEps}
      />

      {/* Sub-header Contextual Status Strip */}
      <div className="px-6 py-2 bg-slate-900/60 border-b border-slate-800/80 flex flex-wrap items-center justify-between text-xs gap-4">
        <div className="flex items-center gap-3 text-slate-400">
          <span className="text-slate-200 font-semibold">StreamForge Cluster:</span>
          <span>50,000 IoT Trucks</span>
          <span>·</span>
          <span>20 Kafka Partitions</span>
          <span>·</span>
          <span className="text-cyan-400 font-mono">5-Min Rolling Windows</span>
          <span>·</span>
          <span className="text-indigo-400 font-mono">RocksDB State Store</span>
        </div>

        <div className="flex items-center gap-4 text-xs font-mono">
          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 font-sans">Workers:</span>
            <span className={`${metrics.crashedWorkers > 0 ? 'text-amber-400' : 'text-emerald-400'} font-bold`}>
              {metrics.healthyWorkers} / 20 Active
            </span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 font-sans">Throughput:</span>
            <span className="text-cyan-400 font-bold tabular-nums">
              {metrics.currentThroughputEps.toLocaleString()} eps
            </span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 font-sans">P99 Latency:</span>
            <span className="text-emerald-400 font-bold tabular-nums">
              {metrics.p99LatencyMs}ms
            </span>
          </div>

          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 font-sans">EOS Semantics:</span>
            <span className="text-emerald-400 font-bold">
              0 Duplicates / 0 Dropped
            </span>
          </div>
        </div>
      </div>

      {/* Main View Port */}
      <main className="flex-1 flex flex-col min-h-0">
        {activeTab === 'topology' && (
          <DAGTopologyView
            metrics={metrics}
            onSelectNodeSnippet={(code, title) => {
              // Can navigate to code if requested
            }}
          />
        )}

        {activeTab === 'workers' && (
          <WorkerClusterView
            workers={workers}
            partitions={partitions}
            rebalanceHistory={rebalanceHistory}
            onKillWorker={handleKillWorker}
            onReviveWorker={handleReviveWorker}
            onTriggerRebalance={handleTriggerRebalance}
          />
        )}

        {activeTab === 'fleet' && (
          <FleetTelemetryView
            latestEvents={latestEvents}
            recentAnomalies={recentAnomalies}
            onSearchTrucks={handleSearchTrucks}
          />
        )}

        {activeTab === 'chaos' && (
          <ChaosEngineeringLab
            workers={workers}
            rebalanceHistory={rebalanceHistory}
            metrics={metrics}
            onKillWorker={handleKillWorker}
            onReviveWorker={handleReviveWorker}
            onTriggerRebalance={handleTriggerRebalance}
            onSetThroughputMultiplier={handleSetThroughputMultiplier}
            currentMultiplier={throughputMultiplier}
          />
        )}

        {activeTab === 'metrics' && (
          <PrometheusMetricsView
            metrics={metrics}
            partitions={partitions}
            workers={workers}
          />
        )}

        {activeTab === 'code' && (
          <PythonCodebaseViewer />
        )}
      </main>
    </div>
  );
}
