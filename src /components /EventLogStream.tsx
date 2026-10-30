import React, { useState } from 'react';
import { Terminal, ShieldAlert, CheckCircle, RefreshCw, Filter, Trash2, ArrowDownCircle } from 'lucide-react';
import { RebalanceEvent } from '../types';

interface EventLogStreamProps {
  events: RebalanceEvent[];
  onClearLogs: () => void;
}

export const EventLogStream: React.FC<EventLogStreamProps> = ({
  events,
  onClearLogs,
}) => {
  const [filter, setFilter] = useState<string>('ALL');

  const filteredEvents = events.filter((e) => {
    if (filter === 'ALL') return true;
    return e.type === filter;
  });

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-xl flex flex-col h-full">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between pb-3 mb-3 border-b border-slate-800/80 gap-2">
        <div className="flex items-center space-x-2">
          <div className="p-1.5 rounded-lg bg-slate-800 text-slate-300">
            <Terminal className="w-5 h-5 text-indigo-400" />
          </div>
          <div>
            <h2 className="font-semibold text-slate-100 text-sm tracking-tight flex items-center gap-2">
              Distributed Engine Event Journal
              <span className="text-xs font-mono font-normal text-slate-400">
                (Kafka Coordinator &bull; Faust Asyncio &bull; RocksDB)
              </span>
            </h2>
            <p className="text-xs text-slate-400">
              Audit stream for partition rebalancing, RocksDB changelog replay, and 2PC checkpoints
            </p>
          </div>
        </div>

        {/* Controls */}
        <div className="flex items-center space-x-2 text-xs">
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="bg-slate-950 border border-slate-800 text-slate-300 rounded-lg px-2 py-1 focus:outline-none"
          >
            <option value="ALL">All Event Types</option>
            <option value="WORKER_CRASH">Worker Crashes</option>
            <option value="REBALANCE_TRIGGER">Rebalances</option>
            <option value="ROCKSDB_RESTORE">RocksDB State Replays</option>
            <option value="EOS_CHECKPOINT">2PC Commit Checkpoints</option>
          </select>

          <button
            onClick={onClearLogs}
            className="p-1 rounded bg-slate-950 hover:bg-slate-800 border border-slate-800 text-slate-400 hover:text-slate-200"
            title="Clear event logs"
          >
            <Trash2 className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Log Feed */}
      <div className="bg-slate-950 rounded-lg border border-slate-800/80 p-2.5 font-mono text-[11px] overflow-y-auto max-h-72 space-y-1.5 scrollbar-thin">
        {filteredEvents.length === 0 ? (
          <div className="text-slate-500 text-center py-6">
            No events recorded in this filter view.
          </div>
        ) : (
          filteredEvents.map((evt) => {
            const isCrash = evt.type === 'WORKER_CRASH';
            const isRestore = evt.type === 'ROCKSDB_RESTORE';
            const isRebalance = evt.type === 'REBALANCE_TRIGGER' || evt.type === 'PARTITION_ASSIGNED';
            const isCheckpoint = evt.type === 'EOS_CHECKPOINT';

            return (
              <div
                key={evt.id}
                className={`p-2 rounded border transition-all flex items-start space-x-2.5 ${
                  isCrash
                    ? 'bg-rose-950/30 border-rose-600/50 text-rose-200'
                    : isRestore
                    ? 'bg-purple-950/30 border-purple-500/50 text-purple-200'
                    : isRebalance
                    ? 'bg-amber-950/30 border-amber-500/50 text-amber-200'
                    : 'bg-slate-900/80 border-slate-800 text-slate-300'
                }`}
              >
                {/* Timestamp */}
                <span className="text-[10px] text-slate-500 whitespace-nowrap pt-0.5">
                  [{evt.timestamp}]
                </span>

                {/* Event Type Badge */}
                <span
                  className={`text-[9px] font-bold px-1.5 py-0.5 rounded whitespace-nowrap ${
                    isCrash
                      ? 'bg-rose-900 text-rose-300'
                      : isRestore
                      ? 'bg-purple-900 text-purple-300'
                      : isRebalance
                      ? 'bg-amber-900 text-amber-300'
                      : 'bg-emerald-950 text-emerald-300'
                  }`}
                >
                  {evt.type}
                </span>

                {/* Details */}
                <div className="flex-1 text-slate-300 leading-snug">
                  <span className="text-slate-400 mr-1.5 font-bold">
                    {evt.sourceWorker}:
                  </span>
                  {evt.details}
                  {evt.durationMs && (
                    <span className="ml-1.5 text-emerald-400 font-bold">
                      ({evt.durationMs}ms)
                    </span>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
};
