import React from 'react';
import { 
  Activity, 
  Cpu, 
  Database, 
  ShieldCheck, 
  Flame, 
  RefreshCw, 
  Code, 
  AlertTriangle,
  Zap,
  Radio
} from 'lucide-react';
import { SystemMetrics } from '../types';

interface HeaderProps {
  metrics: SystemMetrics;
  onCrashWorker4: () => void;
  onReviveAll: () => void;
  onSpikeTraffic: () => void;
  onTriggerColdChainExcursion: () => void;
  onOpenCodeModal: () => void;
  isWorker4Crashed: boolean;
  isTrafficSpiked: boolean;
}

export const Header: React.FC<HeaderProps> = ({
  metrics,
  onCrashWorker4,
  onReviveAll,
  onSpikeTraffic,
  onTriggerColdChainExcursion,
  onOpenCodeModal,
  isWorker4Crashed,
  isTrafficSpiked,
}) => {
  return (
    <header className="border-b border-slate-800 bg-slate-950 text-slate-100 sticky top-0 z-30 shadow-md">
      {/* Top Banner: Brand, Architecture, Primary Action CTA */}
      <div className="max-w-7xl mx-auto px-4 py-3 flex flex-wrap items-center justify-between gap-4">
        {/* Left: Branding & Core Architecture */}
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-amber-500 via-orange-500 to-indigo-600 p-0.5 shadow-lg shadow-orange-500/20 flex items-center justify-center">
            <div className="w-full h-full bg-slate-950 rounded-[10px] flex items-center justify-center">
              <Flame className="w-5 h-5 text-orange-400" />
            </div>
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h1 className="text-xl font-bold tracking-tight text-white flex items-center gap-2">
                StreamForge
                <span className="text-xs font-mono font-medium px-2 py-0.5 rounded-full bg-orange-500/10 text-orange-400 border border-orange-500/30">
                  Python Faust + RocksDB
                </span>
              </h1>
            </div>
            <p className="text-xs text-slate-400">
              Distributed IoT Event Processor &bull; 50,000 Trucks &bull; 5-Min Rolling Window Aggregations
            </p>
          </div>
        </div>

        {/* Right: Quick Action Controls */}
        <div className="flex flex-wrap items-center gap-2">
          {/* Prominent Use Case Action: Crash Worker #4 */}
          {!isWorker4Crashed ? (
            <button
              id="btn-crash-worker-4"
              onClick={onCrashWorker4}
              className="inline-flex items-center gap-2 px-3.5 py-1.5 rounded-lg bg-rose-500/15 hover:bg-rose-500/25 text-rose-300 border border-rose-500/40 text-xs font-medium transition-all shadow-sm active:scale-95"
              title="Simulate sudden SIGKILL on Worker Node #4 to trigger Kafka partition rebalance & RocksDB state recovery to Worker #5"
            >
              <AlertTriangle className="w-3.5 h-3.5 text-rose-400 animate-pulse" />
              <span>Crash Worker #4 (Failover Test)</span>
            </button>
          ) : (
            <button
              id="btn-revive-worker-4"
              onClick={onReviveAll}
              className="inline-flex items-center gap-2 px-3.5 py-1.5 rounded-lg bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 border border-emerald-500/40 text-xs font-medium transition-all shadow-sm active:scale-95"
            >
              <RefreshCw className="w-3.5 h-3.5 text-emerald-400" />
              <span>Revive Worker #4 & Rebalance</span>
            </button>
          )}

          {/* Traffic Spike */}
          <button
            id="btn-spike-traffic"
            onClick={onSpikeTraffic}
            className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
              isTrafficSpiked 
                ? 'bg-amber-500/25 border-amber-500/60 text-amber-300 ring-1 ring-amber-500/30' 
                : 'bg-slate-900 hover:bg-slate-800 border-slate-700 text-slate-300'
            }`}
          >
            <Zap className="w-3.5 h-3.5 text-amber-400" />
            <span>{isTrafficSpiked ? 'Spike Active (15k/s)' : 'Spike Fleet Traffic'}</span>
          </button>

          {/* Simulate Cold-Chain Anomaly */}
          <button
            id="btn-anomaly-sim"
            onClick={onTriggerColdChainExcursion}
            className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-700 text-slate-300 text-xs font-medium transition-all"
          >
            <Radio className="w-3.5 h-3.5 text-sky-400" />
            <span>Simulate Temp Excursion</span>
          </button>

          {/* View Python Code */}
          <button
            id="btn-view-python-code"
            onClick={onOpenCodeModal}
            className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-indigo-600/20 hover:bg-indigo-600/30 text-indigo-300 border border-indigo-500/40 text-xs font-medium transition-all"
          >
            <Code className="w-3.5 h-3.5 text-indigo-400" />
            <span>View Python Code</span>
          </button>
        </div>
      </div>

      {/* Metrics Ticker Bar */}
      <div className="bg-slate-900/90 border-t border-slate-800/80 px-4 py-2">
        <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-y-2 text-xs">
          <div className="flex flex-wrap items-center gap-4 text-slate-300">
            {/* Connected IoT Fleet */}
            <div className="flex items-center gap-2">
              <span className="text-slate-500 font-medium">IoT Fleet:</span>
              <span className="font-mono font-semibold text-white">50,000 Trucks</span>
              <span className="text-[10px] text-slate-400">(@10s emit)</span>
            </div>

            <div className="h-3 w-px bg-slate-800 hidden sm:block" />

            {/* Throughput */}
            <div className="flex items-center gap-2">
              <Activity className="w-3.5 h-3.5 text-emerald-400 animate-pulse" />
              <span className="text-slate-500 font-medium">Ingest Rate:</span>
              <span className="font-mono font-bold text-emerald-400">
                {metrics.currentEventRate.toLocaleString()} msgs/sec
              </span>
            </div>

            <div className="h-3 w-px bg-slate-800 hidden sm:block" />

            {/* Workers & Partitions */}
            <div className="flex items-center gap-2">
              <Cpu className="w-3.5 h-3.5 text-sky-400" />
              <span className="text-slate-500 font-medium">Faust Workers:</span>
              <span className="font-mono font-semibold text-white">
                <span className={metrics.activeWorkersCount < 20 ? 'text-rose-400 font-bold' : 'text-emerald-400'}>
                  {metrics.activeWorkersCount}
                </span>
                /20 Active
              </span>
              <span className="text-slate-500">|</span>
              <span className="font-mono text-slate-300">20 Partitions</span>
            </div>

            <div className="h-3 w-px bg-slate-800 hidden md:block" />

            {/* Event Loop Latency */}
            <div className="flex items-center gap-2">
              <span className="text-slate-500 font-medium">P99 Loop Latency:</span>
              <span className="font-mono font-semibold text-amber-400">{metrics.p99LatencyMs}ms</span>
            </div>
          </div>

          {/* Exactly-Once Guarantee & RocksDB State Tag */}
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-emerald-500/10 border border-emerald-500/25 text-emerald-400 font-mono text-[11px]">
              <ShieldCheck className="w-3.5 h-3.5" />
              <span>Exactly-Once (2PC): 0 Dropped &bull; 0 Dupes</span>
            </div>

            <div className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-indigo-500/10 border border-indigo-500/25 text-indigo-300 font-mono text-[11px]">
              <Database className="w-3.5 h-3.5" />
              <span>RocksDB Embedded LSM</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};
