import React, { useState, useEffect, useCallback, useRef } from 'react';
import { 
  KafkaPartition, 
  PythonWorker, 
  TruckTelemetry, 
  SystemMetrics, 
  RebalanceEvent 
} from './types';
import { 
  generateInitialPartitions, 
  generateInitialWorkers, 
  generateSampleTrucks, 
  INITIAL_REBALANCE_EVENTS 
} from './data/mockStreamEngine';
import { Header } from './components/Header';
import { TopologyCanvas } from './components/TopologyCanvas';
import { WorkerGrid } from './components/WorkerGrid';
import { FleetMonitor } from './components/FleetMonitor';
import { StateStoreInspector } from './components/StateStoreInspector';
import { PartitionLagChart } from './components/PartitionLagChart';
import { EventLogStream } from './components/EventLogStream';
import { PythonCodeModal } from './components/PythonCodeModal';
import { PythonRestApiWorkbench } from './components/PythonRestApiWorkbench';
import { Workflow, Cpu, Truck, Database, Layers, Terminal, Server } from 'lucide-react';

export default function App() {
  // Core Distributed Cluster State
  const [partitions, setPartitions] = useState<KafkaPartition[]>(generateInitialPartitions);
  const [workers, setWorkers] = useState<PythonWorker[]>(generateInitialWorkers);
  const [trucks, setTrucks] = useState<TruckTelemetry[]>(generateSampleTrucks);
  const [events, setEvents] = useState<RebalanceEvent[]>(INITIAL_REBALANCE_EVENTS);
  
  // Selection & Active Views
  const [selectedWorkerId, setSelectedWorkerId] = useState<number | null>(4);
  const [selectedPartitionId, setSelectedPartitionId] = useState<number | null>(3);
  const [selectedTruckId, setSelectedTruckId] = useState<string | undefined>('TRK-10000');
  const [activeTab, setActiveTab] = useState<'TOPOLOGY' | 'WORKERS' | 'FLEET' | 'ROCKSDB' | 'PARTITIONS' | 'REST_API'>('TOPOLOGY');
  const [isCodeModalOpen, setIsCodeModalOpen] = useState(false);

  // Simulation flags
  const [isWorker4Crashed, setIsWorker4Crashed] = useState(false);
  const [isTrafficSpiked, setIsTrafficSpiked] = useState(false);

  // Metrics
  const [metrics, setMetrics] = useState<SystemMetrics>({
    totalTrucks: 50000,
    targetEventRate: 5000,
    currentEventRate: 5024,
    p50LatencyMs: 1.4,
    p99LatencyMs: 3.8,
    totalProcessedCount: 28405000,
    droppedCount: 0,
    duplicatedCount: 0,
    rebalancesTotal: 1,
    activeWorkersCount: 20,
    activePartitionsCount: 20,
    totalLag: 142,
    kafkaClusterState: 'HEALTHY',
  });

  // Reference for event logging
  const logEvent = useCallback((
    type: RebalanceEvent['type'],
    source: string,
    details: string,
    partitionId?: number,
    durationMs?: number
  ) => {
    const newEvent: RebalanceEvent = {
      id: `evt-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`,
      timestamp: new Date().toLocaleTimeString(),
      type,
      sourceWorker: source,
      details,
      partitionId,
      durationMs,
    };
    setEvents(prev => [newEvent, ...prev.slice(0, 49)]);
  }, []);

  // Primary Scenario: Crash Worker #4 and trigger failover to Worker #5
  const handleCrashWorker4 = useCallback(() => {
    if (isWorker4Crashed) return;
    setIsWorker4Crashed(true);

    const now = new Date().toLocaleTimeString();

    // 1. Immediately mark Worker 4 as CRASHED and revoke Partition 3
    setWorkers(prev => prev.map(w => {
      if (w.id === 4) {
        return {
          ...w,
          status: 'CRASHED',
          assignedPartitions: [],
          cpuPercent: 0,
          processedPerSec: 0,
          crashedAt: now,
        };
      }
      return w;
    }));

    // Update Partition 3 as temporarily unassigned or rebalancing
    setPartitions(prev => prev.map(p => {
      if (p.id === 3) {
        return {
          ...p,
          consumerLag: p.consumerLag + 18,
          assignedWorkerId: null,
        };
      }
      return p;
    }));

    // Log Worker 4 crash
    logEvent(
      'WORKER_CRASH',
      'faust-worker-04',
      'Process killed (SIGKILL simulated). Heartbeat session timeout expired (3000ms).'
    );

    // 2. Trigger Kafka Rebalance Event (after 200ms)
    setTimeout(() => {
      logEvent(
        'REBALANCE_TRIGGER',
        'Kafka Coordinator (Broker-101)',
        'CooperativeStickyAssignor detected member faust-worker-04 departure. Rebalancing Partition [3]...'
      );

      // 3. Rebalance Partition 3 to Worker 5 with RocksDB changelog recovery
      setTimeout(() => {
        setWorkers(prev => prev.map(w => {
          if (w.id === 5) {
            return {
              ...w,
              status: 'RECOVERING_STATE',
              assignedPartitions: [3, 4], // Worker 5 now takes Partition 3 in addition to 4
            };
          }
          return w;
        }));

        logEvent(
          'ROCKSDB_RESTORE',
          'faust-worker-05',
          'Replaying changelog from __streamforge-fleet-processor-truck_temp_5min_window-changelog for P-3. Restored 2,500 keys.',
          3,
          184
        );

        // 4. Complete recovery and return Worker 5 to ONLINE
        setTimeout(() => {
          setWorkers(prev => prev.map(w => {
            if (w.id === 5) {
              return {
                ...w,
                status: 'ONLINE',
                rocksdb: {
                  ...w.rocksdb,
                  totalKeys: 5000, // 2,500 (P-4) + 2,500 (P-3) = 5,000 keys!
                  sstableCount: w.rocksdb.sstableCount + 8,
                },
                processedPerSec: 510,
              };
            }
            return w;
          }));

          setPartitions(prev => prev.map(p => {
            if (p.id === 3) {
              return {
                ...p,
                assignedWorkerId: 5,
                consumerLag: Math.max(2, p.consumerLag - 14),
              };
            }
            return p;
          }));

          logEvent(
            'EOS_CHECKPOINT',
            'faust-worker-05',
            'State restore finished. Two-phase commit checkpoint verified: 0 records dropped, 0 duplicate processing.',
            3
          );

          setMetrics(m => ({
            ...m,
            activeWorkersCount: 19,
            rebalancesTotal: m.rebalancesTotal + 1,
            droppedCount: 0,
            duplicatedCount: 0,
          }));
        }, 600);
      }, 400);
    }, 300);
  }, [isWorker4Crashed, logEvent]);

  // Revive Worker 4 and rebalance Partition 3 back
  const handleReviveWorker4 = useCallback(() => {
    setIsWorker4Crashed(false);

    logEvent(
      'REBALANCE_TRIGGER',
      'Kafka Coordinator (Broker-101)',
      'Member faust-worker-04 rejoined consumer group. Triggering graceful partition assignment...'
    );

    setTimeout(() => {
      setWorkers(prev => prev.map(w => {
        if (w.id === 4) {
          return {
            ...w,
            status: 'ONLINE',
            assignedPartitions: [3],
            cpuPercent: 31,
            processedPerSec: 250,
          };
        }
        if (w.id === 5) {
          return {
            ...w,
            status: 'ONLINE',
            assignedPartitions: [4],
            rocksdb: {
              ...w.rocksdb,
              totalKeys: 2500,
            },
            processedPerSec: 255,
          };
        }
        return w;
      }));

      setPartitions(prev => prev.map(p => {
        if (p.id === 3) {
          return {
            ...p,
            assignedWorkerId: 4,
            consumerLag: 4,
          };
        }
        return p;
      }));

      logEvent(
        'PARTITION_ASSIGNED',
        'faust-worker-04',
        'Partition [3] reassigned. Local RocksDB WAL synced. Normal 20-worker topology restored.'
      );

      setMetrics(m => ({
        ...m,
        activeWorkersCount: 20,
      }));
    }, 500);
  }, [logEvent]);

  // Traffic Spike (burst to 15,000/s)
  const handleSpikeTraffic = useCallback(() => {
    setIsTrafficSpiked(prev => {
      const next = !prev;
      logEvent(
        'REBALANCE_TRIGGER',
        'Fleet Traffic Generator',
        next 
          ? 'Burst Ingest Triggered: 50,000 trucks emitting telemetry every 3.3s (~15,000 msgs/s).'
          : 'Traffic normalized back to 10s emit interval (5,000 msgs/s).'
      );
      return next;
    });
  }, [logEvent]);

  // Simulate Cold-Chain Anomaly on sample truck
  const handleTriggerColdChainExcursion = useCallback(() => {
    setTrucks(prev => {
      const updated = [...prev];
      const targetIdx = 0; // TRK-10000 (Frozen Seafood -22C)
      const isAlready = updated[targetIdx].status === 'CRITICAL_EXCURSION';
      
      updated[targetIdx] = {
        ...updated[targetIdx],
        status: isAlready ? 'NORMAL' : 'CRITICAL_EXCURSION',
        currentTempC: isAlready ? -21.8 : -14.2,
        rollingAvg5mC: isAlready ? -22.0 : -15.5,
      };

      logEvent(
        'EOS_CHECKPOINT',
        'faust-worker-01',
        isAlready 
          ? `[ALERT CLEARED] Truck ${updated[targetIdx].truckId} cargo temperature restored to target (-22.0°C).`
          : `[CRITICAL ALERT] Truck ${updated[targetIdx].truckId} 5-min rolling avg (-15.5°C) breached target (-22.0°C by +6.5°C). Alert emitted to fleet-temperature-aggregates.`
      );

      return updated;
    });
  }, [logEvent]);

  // Individual truck excursion toggle
  const handleTriggerTruckExcursion = useCallback((truckId: string) => {
    setTrucks(prev => prev.map(t => {
      if (t.truckId === truckId) {
        const isExcursion = t.status === 'CRITICAL_EXCURSION';
        const newStatus = isExcursion ? 'NORMAL' : 'CRITICAL_EXCURSION';
        const newCurrent = isExcursion ? t.targetTempC + 0.2 : t.targetTempC + 7.4;
        const newAvg = isExcursion ? t.targetTempC : +(t.targetTempC + 6.1).toFixed(1);

        logEvent(
          'EOS_CHECKPOINT',
          `faust-worker-0${(t.partition % 9) + 1}`,
          isExcursion
            ? `Reefer restored on ${truckId}. Temp returning to ${t.targetTempC}°C.`
            : `[ALERT] Reefer compressor malfunction on ${truckId} (${t.cargoType}). Temp climbed to ${newCurrent}°C.`
        );

        return {
          ...t,
          status: newStatus,
          currentTempC: newCurrent,
          rollingAvg5mC: newAvg,
        };
      }
      return t;
    }));
  }, [logEvent]);

  // REST API Telemetry Ingestion handler
  const handleIngestTelemetry = useCallback((telemetry: Partial<TruckTelemetry> & { truckId: string }) => {
    setTrucks(prev => {
      const idx = prev.findIndex(t => t.truckId.toUpperCase() === telemetry.truckId.toUpperCase());
      const temp = telemetry.currentTempC ?? -22.0;
      const target = telemetry.targetTempC ?? -22.0;
      const deviation = +(temp - target).toFixed(1);
      const isExcursion = Math.abs(deviation) > 2.5;

      if (idx !== -1) {
        const existing = prev[idx];
        const updated = [...prev];
        updated[idx] = {
          ...existing,
          currentTempC: temp,
          targetTempC: target,
          rollingAvg5mC: +((existing.rollingAvg5mC * 4 + temp) / 5).toFixed(1),
          status: isExcursion ? 'CRITICAL_EXCURSION' : 'NORMAL',
          lastUpdated: 'Just now via REST API',
          samplesInWindow: existing.samplesInWindow + 1,
        };
        return updated;
      } else {
        const hash = telemetry.truckId.split('').reduce((acc, c) => acc + c.charCodeAt(0), 0);
        const newTruck: TruckTelemetry = {
          truckId: telemetry.truckId.toUpperCase(),
          partition: hash % 20,
          cargoType: (telemetry.cargoType as any) || 'Frozen Seafood',
          targetTempC: target,
          currentTempC: temp,
          rollingAvg5mC: temp,
          minTemp5mC: temp,
          maxTemp5mC: temp,
          samplesInWindow: 1,
          lastUpdated: 'Just now via REST API',
          route: 'REST Ingestion Stream',
          status: isExcursion ? 'CRITICAL_EXCURSION' : 'NORMAL',
        };
        return [newTruck, ...prev.slice(0, 49)];
      }
    });

    setMetrics(m => ({
      ...m,
      totalProcessedCount: m.totalProcessedCount + 1,
    }));
  }, []);

  // Real-time ticking simulation loop (runs every second)
  useEffect(() => {
    const interval = setInterval(() => {
      // Fluctuate throughput and lag realistically
      const baseRate = isTrafficSpiked ? 15200 : 5020;
      const rateJitter = Math.floor((Math.random() - 0.5) * 160);
      const currentRate = baseRate + rateJitter;

      // Update partitions
      setPartitions(prev => prev.map(p => {
        const delta = Math.floor(currentRate / 20);
        return {
          ...p,
          currentOffset: p.currentOffset + delta,
          highWaterMark: p.highWaterMark + delta,
          consumerLag: p.id === 3 && isWorker4Crashed ? Math.min(60, p.consumerLag + 2) : Math.floor(Math.random() * 12) + 2,
          msgRatePerSec: Math.floor(currentRate / 20) + Math.floor((Math.random() - 0.5) * 10),
          totalEventsProcessed: p.totalEventsProcessed + delta,
        };
      }));

      // Update workers CPU & metrics
      setWorkers(prev => prev.map(w => {
        if (w.status === 'CRASHED') return w;
        const cpuJitter = Math.floor((Math.random() - 0.5) * 6);
        const baseCpu = isTrafficSpiked ? 62 : 32;
        const multiPartition = w.assignedPartitions.length > 1;

        return {
          ...w,
          cpuPercent: Math.min(95, Math.max(15, (multiPartition ? baseCpu + 24 : baseCpu) + cpuJitter)),
          eventLoopLatencyMs: +(1.2 + Math.random() * (isTrafficSpiked ? 2.8 : 0.8)).toFixed(2),
          totalProcessed: w.totalProcessed + Math.floor(currentRate / 20),
          processedPerSec: Math.floor(currentRate / (isWorker4Crashed ? 19 : 20)),
          rocksdb: {
            ...w.rocksdb,
            changelogOffset: w.rocksdb.changelogOffset + 120,
            lastCheckpointTime: new Date().toLocaleTimeString(),
          }
        };
      }));

      // Update system metrics
      setMetrics(m => ({
        ...m,
        currentEventRate: currentRate,
        totalProcessedCount: m.totalProcessedCount + currentRate,
        p50LatencyMs: +(1.2 + Math.random() * 0.4).toFixed(1),
        p99LatencyMs: +(isTrafficSpiked ? 6.2 + Math.random() : 3.4 + Math.random() * 0.8).toFixed(1),
      }));

      // Minor temperature jitter on trucks to simulate continuous sensor stream
      setTrucks(prev => prev.map(t => {
        if (t.status === 'CRITICAL_EXCURSION') return t;
        const jitter = +((Math.random() - 0.5) * 0.3).toFixed(1);
        const currentTemp = +(t.targetTempC + jitter).toFixed(1);
        return {
          ...t,
          currentTempC: currentTemp,
          lastUpdated: 'Just now',
        };
      }));

    }, 1000);

    return () => clearInterval(interval);
  }, [isTrafficSpiked, isWorker4Crashed]);

  const selectedWorker = workers.find(w => w.id === selectedWorkerId);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans selection:bg-indigo-500 selection:text-white">
      {/* Header with KPIs and Scenario Actions */}
      <Header
        metrics={metrics}
        onCrashWorker4={handleCrashWorker4}
        onReviveAll={handleReviveWorker4}
        onSpikeTraffic={handleSpikeTraffic}
        onTriggerColdChainExcursion={handleTriggerColdChainExcursion}
        onOpenCodeModal={() => setIsCodeModalOpen(true)}
        onSelectRestApi={() => setActiveTab('REST_API')}
        isWorker4Crashed={isWorker4Crashed}
        isTrafficSpiked={isTrafficSpiked}
      />

      {/* Main App Body */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 py-5 space-y-5">
        
        {/* Architectural Context Bar */}
        <div className="bg-slate-900/60 border border-slate-800 rounded-xl p-3 flex flex-wrap items-center justify-between gap-3 text-xs">
          <div className="flex items-center space-x-2 text-slate-300">
            <span className="font-bold text-white uppercase tracking-wider text-[11px] font-mono px-2 py-0.5 rounded bg-indigo-950 text-indigo-300 border border-indigo-800/60">
              Distributed Problem Solved
            </span>
            <span className="text-slate-300 hidden md:inline">
              Pure-Python stream processing alternative to Java Flink/Spark with embedded RocksDB &amp; strict Exactly-Once Semantics.
            </span>
          </div>

          {/* Module Navigation Tabs */}
          <div className="flex rounded-lg bg-slate-950 p-0.5 border border-slate-800 text-xs">
            <button
              onClick={() => setActiveTab('TOPOLOGY')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md transition-all font-medium ${
                activeTab === 'TOPOLOGY' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Workflow className="w-3.5 h-3.5" />
              Topology Pipeline
            </button>
            <button
              onClick={() => setActiveTab('WORKERS')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md transition-all font-medium ${
                activeTab === 'WORKERS' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Cpu className="w-3.5 h-3.5" />
              Worker Pool (20)
            </button>
            <button
              onClick={() => setActiveTab('FLEET')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md transition-all font-medium ${
                activeTab === 'FLEET' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Truck className="w-3.5 h-3.5" />
              Fleet 5-Min Windows
            </button>
            <button
              onClick={() => setActiveTab('ROCKSDB')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md transition-all font-medium ${
                activeTab === 'ROCKSDB' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Database className="w-3.5 h-3.5" />
              RocksDB State Store
            </button>
            <button
              onClick={() => setActiveTab('PARTITIONS')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md transition-all font-medium ${
                activeTab === 'PARTITIONS' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Layers className="w-3.5 h-3.5" />
              Kafka Partitions
            </button>
            <button
              onClick={() => setActiveTab('REST_API')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md transition-all font-medium ${
                activeTab === 'REST_API' ? 'bg-indigo-600 text-white shadow' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Server className="w-3.5 h-3.5" />
              Python REST API
            </button>
          </div>
        </div>

        {/* Primary View based on Tab */}
        {activeTab === 'TOPOLOGY' && (
          <div className="space-y-5">
            <TopologyCanvas
              partitions={partitions}
              workers={workers}
              onSelectWorker={(id) => {
                setSelectedWorkerId(id);
                setActiveTab('WORKERS');
              }}
              onSelectPartition={(id) => {
                setSelectedPartitionId(id);
                setActiveTab('PARTITIONS');
              }}
              selectedWorkerId={selectedWorkerId}
              selectedPartitionId={selectedPartitionId}
              isWorker4Crashed={isWorker4Crashed}
            />

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
              <FleetMonitor
                trucks={trucks.slice(0, 8)}
                onTriggerTruckExcursion={handleTriggerTruckExcursion}
                onSelectTruck={(t) => setSelectedTruckId(t.truckId)}
                selectedTruckId={selectedTruckId}
              />
              <EventLogStream
                events={events}
                onClearLogs={() => setEvents([])}
              />
            </div>
          </div>
        )}

        {activeTab === 'WORKERS' && (
          <div className="space-y-5">
            <WorkerGrid
              workers={workers}
              onCrashWorker={(id) => {
                if (id === 4) handleCrashWorker4();
                else {
                  setWorkers(prev => prev.map(w => w.id === id ? { ...w, status: 'CRASHED', cpuPercent: 0 } : w));
                  logEvent('WORKER_CRASH', `faust-worker-${String(id).padStart(2, '0')}`, `Worker process ${id} terminated.`);
                }
              }}
              onReviveWorker={(id) => {
                if (id === 4) handleReviveWorker4();
                else {
                  setWorkers(prev => prev.map(w => w.id === id ? { ...w, status: 'ONLINE', cpuPercent: 30 } : w));
                  logEvent('PARTITION_ASSIGNED', `faust-worker-${String(id).padStart(2, '0')}`, `Worker ${id} restarted and state restored.`);
                }
              }}
              onInspectWorker={(id) => {
                setSelectedWorkerId(id);
                setActiveTab('ROCKSDB');
              }}
              selectedWorkerId={selectedWorkerId}
            />

            <EventLogStream
              events={events}
              onClearLogs={() => setEvents([])}
            />
          </div>
        )}

        {activeTab === 'FLEET' && (
          <div className="space-y-5">
            <FleetMonitor
              trucks={trucks}
              onTriggerTruckExcursion={handleTriggerTruckExcursion}
              onSelectTruck={(t) => setSelectedTruckId(t.truckId)}
              selectedTruckId={selectedTruckId}
            />
          </div>
        )}

        {activeTab === 'ROCKSDB' && (
          <div className="space-y-5">
            <StateStoreInspector
              selectedWorker={selectedWorker}
              trucks={trucks}
            />
            <EventLogStream
              events={events}
              onClearLogs={() => setEvents([])}
            />
          </div>
        )}

        {activeTab === 'PARTITIONS' && (
          <div className="space-y-5">
            <PartitionLagChart
              partitions={partitions}
              onSelectPartition={(id) => setSelectedPartitionId(id)}
              selectedPartitionId={selectedPartitionId}
              isWorker4Crashed={isWorker4Crashed}
            />
            <EventLogStream
              events={events}
              onClearLogs={() => setEvents([])}
            />
          </div>
        )}

        {activeTab === 'REST_API' && (
          <PythonRestApiWorkbench
            trucks={trucks}
            workers={workers}
            partitions={partitions}
            metrics={metrics}
            onIngestTelemetry={handleIngestTelemetry}
            onLogEvent={logEvent}
            onReviveWorker={handleReviveWorker4}
          />
        )}

      </main>

      {/* Python Architecture Code Modal */}
      <PythonCodeModal
        isOpen={isCodeModalOpen}
        onClose={() => setIsCodeModalOpen(false)}
      />

      {/* Footer */}
      <footer className="border-t border-slate-800/80 bg-slate-950 py-4 text-center text-xs text-slate-500">
        <div className="max-w-7xl mx-auto px-4 flex flex-wrap items-center justify-between gap-2">
          <span>
            StreamForge Engine &bull; Apache Kafka + Faust Python Streaming + Embedded RocksDB
          </span>
          <span className="font-mono text-[11px] text-slate-400">
            50,000 Connected Trucks &bull; Exactly-Once Semantics (EOS) Guaranteed
          </span>
        </div>
      </footer>
    </div>
  );
}
