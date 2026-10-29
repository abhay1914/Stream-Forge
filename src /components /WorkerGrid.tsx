import React, { useState } from 'react';
import { 
  Cpu, 
  Database, 
  Terminal, 
  AlertTriangle, 
  RefreshCw, 
  Layers, 
  Clock, 
  Search,
  CheckCircle2,
  HardDrive
} from 'lucide-react';
import { PythonWorker } from '../types';

interface WorkerGridProps {
  workers: PythonWorker[];
  onCrashWorker: (workerId: number) => void;
  onReviveWorker: (workerId: number) => void;
  onInspectWorker: (workerId: number) => void;
  selectedWorkerId: number | null;
}

export const WorkerGrid: React.FC<WorkerGridProps> = ({
  workers,
  onCrashWorker,
  onReviveWorker,
  onInspectWorker,
  selectedWorkerId,
}) => {
  const [filter, setFilter] = useState<'ALL' | 'ONLINE' | 'CRASHED'>('ALL');
  const [searchQuery, setSearchQuery] = useState('');

  const filteredWorkers = workers.filter(w => {
    if (filter === 'ONLINE' && w.status !== 'ONLINE') return false;
    if (filter === 'CRASHED' && w.status !== 'CRASHED') return false;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      const matchesName = w.name.toLowerCase().includes(q);
      const matchesPart = w.assignedPartitions.some(p => String(p).includes(q));
      return matchesName || matchesPart;
    }
    return true;
  });

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-xl">
      {/* Grid Control Header */}
      <div className="flex flex-wrap items-center justify-between pb-3 mb-4 border-b border-slate-800/80 gap-3">
        <div className="flex items-center space-x-2">
          <div className="p-1.5 rounded-lg bg-emerald-500/10 text-emerald-400">
            <Cpu className="w-5 h-5" />
          </div>
          <div>
            <h2 className="font-semibold text-slate-100 text-sm tracking-tight flex items-center gap-2">
              Distributed Python Worker Pool
              <span className="text-xs font-mono font-normal text-slate-400">
                (20 Parallel Faust Processes)
              </span>
            </h2>
            <p className="text-xs text-slate-400">
              Each worker runs an asyncio event loop + embedded RocksDB storage engine
            </p>
          </div>
        </div>

        {/* Filter & Search */}
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative">
            <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search worker or partition..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="bg-slate-950 border border-slate-800 text-slate-200 text-xs rounded-lg pl-8 pr-3 py-1.5 focus:outline-none focus:border-indigo-500 w-48 transition-all"
            />
          </div>

          <div className="flex rounded-lg bg-slate-950 p-0.5 border border-slate-800 text-xs">
            <button
              onClick={() => setFilter('ALL')}
              className={`px-2.5 py-1 rounded-md transition-all ${
                filter === 'ALL' ? 'bg-indigo-600 text-white font-medium' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              All (20)
            </button>
            <button
              onClick={() => setFilter('ONLINE')}
              className={`px-2.5 py-1 rounded-md transition-all ${
                filter === 'ONLINE' ? 'bg-emerald-600 text-white font-medium' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Online ({workers.filter(w => w.status === 'ONLINE').length})
            </button>
            <button
              onClick={() => setFilter('CRASHED')}
              className={`px-2.5 py-1 rounded-md transition-all ${
                filter === 'CRASHED' ? 'bg-rose-600 text-white font-medium' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Crashed ({workers.filter(w => w.status === 'CRASHED').length})
            </button>
          </div>
        </div>
      </div>

      {/* Workers Grid (20 Workers) */}
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-3">
        {filteredWorkers.map((worker) => {
          const isSelected = selectedWorkerId === worker.id;
          const isCrashed = worker.status === 'CRASHED';
          const isRecovering = worker.status === 'RECOVERING_STATE';
          const isAbsorber = worker.id === 5 && worker.assignedPartitions.length > 1;

          return (
            <div
              key={worker.id}
              className={`rounded-lg border p-3 flex flex-col justify-between transition-all relative ${
                isCrashed
                  ? 'bg-rose-950/30 border-rose-600/70 text-rose-100 shadow-md shadow-rose-950/40'
                  : isAbsorber
                  ? 'bg-indigo-950/40 border-indigo-500/70 text-indigo-100 shadow-md shadow-indigo-950/40 ring-1 ring-indigo-500/50'
                  : isSelected
                  ? 'bg-slate-800/90 border-indigo-400 ring-2 ring-indigo-400 text-slate-100'
                  : 'bg-slate-950/70 border-slate-800/90 hover:border-slate-700 text-slate-200'
              }`}
            >
              {/* Card Top: Worker ID, PID & Status */}
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <span className="font-mono font-bold text-xs flex items-center gap-1.5 text-white">
                    <span
                      className={`w-2 h-2 rounded-full ${
                        isCrashed
                          ? 'bg-rose-500'
                          : isRecovering
                          ? 'bg-cyan-400 animate-spin'
                          : 'bg-emerald-400'
                      }`}
                    />
                    W-{String(worker.id).padStart(2, '0')}
                  </span>
                  <span className="text-[10px] font-mono text-slate-500">
                    PID {worker.pid}
                  </span>
                </div>

                {/* Assigned Partitions Badges */}
                <div className="flex flex-wrap items-center gap-1 my-2">
                  {worker.assignedPartitions.length > 0 ? (
                    worker.assignedPartitions.map((p) => (
                      <span
                        key={p}
                        className={`text-[9px] font-mono px-1.5 py-0.5 rounded font-semibold ${
                          isAbsorber && p === 3
                            ? 'bg-amber-500/20 text-amber-300 border border-amber-500/50 animate-pulse'
                            : 'bg-slate-800 text-slate-300 border border-slate-700'
                        }`}
                        title={`Partition ${p}`}
                      >
                        P-{String(p).padStart(2, '0')}
                        {isAbsorber && p === 3 && ' (Migrated)'}
                      </span>
                    ))
                  ) : (
                    <span className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-rose-950 text-rose-300 border border-rose-800">
                      0 Partitions (Revoked)
                    </span>
                  )}
                </div>

                {/* Worker Metrics Metrics Row */}
                <div className="space-y-1.5 text-[11px] font-mono my-2.5">
                  {/* CPU Usage */}
                  <div>
                    <div className="flex justify-between text-[10px] text-slate-400 mb-0.5">
                      <span>CPU Load:</span>
                      <span className={isCrashed ? 'text-rose-400' : 'text-slate-200'}>
                        {isCrashed ? '0%' : `${worker.cpuPercent}%`}
                      </span>
                    </div>
                    <div className="w-full bg-slate-900 rounded-full h-1 overflow-hidden border border-slate-800">
                      <div
                        className={`h-full rounded-full transition-all duration-500 ${
                          isCrashed
                            ? 'bg-slate-800 w-0'
                            : worker.cpuPercent > 70
                            ? 'bg-amber-500'
                            : 'bg-emerald-500'
                        }`}
                        style={{ width: `${isCrashed ? 0 : worker.cpuPercent}%` }}
                      />
                    </div>
                  </div>

                  {/* Loop Latency & Mem */}
                  <div className="flex justify-between text-[10px] text-slate-400 pt-1">
                    <span>Loop Lag:</span>
                    <span className="text-slate-200 font-semibold">
                      {isCrashed ? '—' : `${worker.eventLoopLatencyMs}ms`}
                    </span>
                  </div>

                  <div className="flex justify-between text-[10px] text-slate-400">
                    <span>RAM:</span>
                    <span className="text-slate-300">{isCrashed ? '—' : `${worker.memoryMb} MB`}</span>
                  </div>

                  {/* RocksDB Local State summary */}
                  <div className="flex justify-between text-[10px] text-slate-400">
                    <span className="flex items-center gap-1">
                      <Database className="w-2.5 h-2.5 text-purple-400" />
                      RocksDB:
                    </span>
                    <span className="text-purple-300 font-semibold">
                      {isCrashed ? 'Changelog Sync' : `${worker.rocksdb.totalKeys.toLocaleString()} keys`}
                    </span>
                  </div>
                </div>
              </div>

              {/* Card Footer Actions */}
              <div className="pt-2 border-t border-slate-800/80 flex items-center justify-between gap-1">
                <button
                  onClick={() => onInspectWorker(worker.id)}
                  className="text-[10px] text-indigo-400 hover:text-indigo-300 font-medium py-1 px-1.5 rounded hover:bg-slate-800/80 transition-all flex items-center gap-1"
                >
                  <Terminal className="w-3 h-3" />
                  Inspect
                </button>

                {isCrashed ? (
                  <button
                    onClick={() => onReviveWorker(worker.id)}
                    className="text-[10px] bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 border border-emerald-500/40 px-2 py-0.5 rounded font-medium transition-all"
                  >
                    Restart
                  </button>
                ) : (
                  <button
                    onClick={() => onCrashWorker(worker.id)}
                    className="text-[10px] text-rose-400 hover:text-rose-300 hover:bg-rose-950/40 px-1.5 py-0.5 rounded font-mono transition-all"
                    title="Kill -9 worker process"
                  >
                    Kill -9
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
