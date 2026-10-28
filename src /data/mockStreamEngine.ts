import { KafkaPartition, PythonWorker, TruckTelemetry, SystemMetrics, RebalanceEvent } from '../types';

// Deterministic truck partition hasher (Murmur2 emulation)
export function getTruckPartition(truckId: string, totalPartitions: number = 20): number {
  let hash = 0;
  for (let i = 0; i < truckId.length; i++) {
    hash = (hash * 31 + truckId.charCodeAt(i)) >>> 0;
  }
  return hash % totalPartitions;
}

export function generateInitialPartitions(): KafkaPartition[] {
  return Array.from({ length: 20 }, (_, idx) => ({
    id: idx,
    leaderBroker: (idx % 3) + 101, // Kafka brokers 101, 102, 103
    currentOffset: 1420500 + Math.floor(Math.random() * 50000),
    highWaterMark: 1420500 + Math.floor(Math.random() * 50000) + 12,
    consumerLag: Math.floor(Math.random() * 15) + 2,
    assignedWorkerId: idx + 1, // Default 1:1 mapping (Partition 0 -> Worker 1, etc.)
    msgRatePerSec: 250 + Math.floor(Math.random() * 30),
    totalEventsProcessed: 1420500,
  }));
}

export function generateInitialWorkers(): PythonWorker[] {
  return Array.from({ length: 20 }, (_, idx) => {
    const workerId = idx + 1;
    return {
      id: workerId,
      name: `faust-worker-${String(workerId).padStart(2, '0')}`,
      pid: 24800 + workerId,
      status: 'ONLINE',
      assignedPartitions: [idx], // Worker 1 has Partition 0, Worker 2 has Partition 1, etc.
      cpuPercent: 28 + Math.floor(Math.random() * 18),
      memoryMb: 412 + Math.floor(Math.random() * 64),
      eventLoopLatencyMs: +(1.2 + Math.random() * 1.5).toFixed(2),
      processedPerSec: 250 + Math.floor(Math.random() * 30),
      totalProcessed: 1420000 + Math.floor(Math.random() * 20000),
      asyncioTasksActive: 16 + Math.floor(Math.random() * 8),
      rebalanceCount: 1,
      rocksdb: {
        totalKeys: 2500, // 50,000 trucks / 20 partitions = 2,500 trucks per partition!
        memtableSizeMb: +(8.4 + Math.random() * 3).toFixed(1),
        sstableCount: 14 + (idx % 5),
        lsmCompactionLevel: 3,
        changelogOffset: 891400 + Math.floor(Math.random() * 2000),
        bloomFilterHitRate: 98.6 + +(Math.random() * 0.8).toFixed(1),
        walSizeBytes: 1048576 * (1 + (idx % 3)),
        lastCheckpointTime: new Date(Date.now() - 4000).toLocaleTimeString(),
      },
    };
  });
}

const CARGO_PROFILES: Array<{
  cargoType: TruckTelemetry['cargoType'];
  target: number;
  routes: string[];
}> = [
  { cargoType: 'Frozen Seafood', target: -22.0, routes: ['Seattle -> Denver (I-90)', 'Portland -> Sacramento (I-5)', 'Boston -> Atlanta (I-95)'] },
  { cargoType: 'Vaccines & Pharma', target: 4.0, routes: ['Chicago -> Indianapolis (I-65)', 'Dallas -> Phoenix (I-10)', 'Memphis -> Nashville (I-40)'] },
  { cargoType: 'Dairy & Fresh Milk', target: 2.5, routes: ['Madison -> Minneapolis (I-94)', 'Boise -> Salt Lake (I-84)', 'Columbus -> Pittsburgh (I-70)'] },
  { cargoType: 'Organic Berries', target: 1.5, routes: ['Fresno -> San Francisco (CA-99)', 'Salinas -> Los Angeles (US-101)', 'Orlando -> Charlotte (I-95)'] },
  { cargoType: 'Poultry', target: -18.0, routes: ['Fayetteville -> Kansas City (I-49)', 'Gainesville -> Raleigh (I-85)', 'Omaha -> Des Moines (I-80)'] },
];

export function generateSampleTrucks(): TruckTelemetry[] {
  const trucks: TruckTelemetry[] = [];
  const totalSamples = 30; // representative live monitored fleet sample set

  for (let i = 0; i < totalSamples; i++) {
    const truckNumber = 10000 + i * 1637;
    const truckId = `TRK-${truckNumber}`;
    const partition = getTruckPartition(truckId, 20);
    const profile = CARGO_PROFILES[i % CARGO_PROFILES.length];
    const route = profile.routes[i % profile.routes.length];
    
    // Simulate slight variance around target
    const isExcursion = i === 4 || i === 19; // intentional realistic cold-chain anomalies
    const currentTemp = isExcursion 
      ? +(profile.target + 6.2 + (Math.random() * 1.5)).toFixed(1)
      : +(profile.target + ((Math.random() - 0.5) * 1.2)).toFixed(1);
    
    const rollingAvg = isExcursion
      ? +(profile.target + 5.1).toFixed(1)
      : +(profile.target + ((Math.random() - 0.5) * 0.6)).toFixed(1);

    trucks.push({
      truckId,
      partition,
      cargoType: profile.cargoType,
      targetTempC: profile.target,
      currentTempC: currentTemp,
      rollingAvg5mC: rollingAvg,
      minTemp5mC: +(Math.min(currentTemp, rollingAvg) - 0.4).toFixed(1),
      maxTemp5mC: +(Math.max(currentTemp, rollingAvg) + 0.5).toFixed(1),
      samplesInWindow: 30, // 5 min / 10s = 30 samples
      lastUpdated: 'Just now',
      route,
      status: isExcursion ? 'CRITICAL_EXCURSION' : 'NORMAL',
    });
  }

  return trucks;
}

export const INITIAL_REBALANCE_EVENTS: RebalanceEvent[] = [
  {
    id: 'evt-001',
    timestamp: '11:41:02',
    type: 'EOS_CHECKPOINT',
    sourceWorker: 'Coordinator (Broker-101)',
    details: 'Cluster-wide Two-Phase Commit checkpoint completed. All 20 RocksDB state stores synchronized.',
  },
  {
    id: 'evt-002',
    timestamp: '11:42:15',
    type: 'PARTITION_ASSIGNED',
    sourceWorker: 'faust-worker-04',
    partitionId: 3,
    details: 'Partition [3] active. Initialized local RocksDB SSTables with 2,500 truck keys.',
  },
  {
    id: 'evt-003',
    timestamp: '11:43:00',
    type: 'EOS_CHECKPOINT',
    sourceWorker: 'faust-worker-05',
    partitionId: 4,
    details: 'Transaction committed: offset 1420500, WAL flushed, 0 lost records.',
  }
];

