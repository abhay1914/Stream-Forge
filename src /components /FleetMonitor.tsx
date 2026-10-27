import React, { useState } from 'react';
import { 
  Truck, 
  Thermometer, 
  AlertTriangle, 
  CheckCircle, 
  Search, 
  Filter, 
  Clock, 
  TrendingUp, 
  Flame,
  Snowflake,
  ShieldAlert
} from 'lucide-react';
import { TruckTelemetry } from '../types';

interface FleetMonitorProps {
  trucks: TruckTelemetry[];
  onTriggerTruckExcursion: (truckId: string) => void;
  onSelectTruck: (truck: TruckTelemetry) => void;
  selectedTruckId?: string;
}

export const FleetMonitor: React.FC<FleetMonitorProps> = ({
  trucks,
  onTriggerTruckExcursion,
  onSelectTruck,
  selectedTruckId,
}) => {
  const [filter, setFilter] = useState<'ALL' | 'EXCURSIONS' | 'NORMAL'>('ALL');
  const [cargoFilter, setCargoFilter] = useState<string>('ALL');
  const [search, setSearch] = useState('');

  const filteredTrucks = trucks.filter((t) => {
    if (filter === 'EXCURSIONS' && t.status !== 'CRITICAL_EXCURSION') return false;
    if (filter === 'NORMAL' && t.status !== 'NORMAL') return false;
    if (cargoFilter !== 'ALL' && t.cargoType !== cargoFilter) return false;
    if (search) {
      const q = search.toLowerCase();
      return t.truckId.toLowerCase().includes(q) || t.route.toLowerCase().includes(q);
    }
    return true;
  });

  const excursionCount = trucks.filter(t => t.status === 'CRITICAL_EXCURSION').length;

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-xl">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between pb-3 mb-4 border-b border-slate-800/80 gap-3">
        <div className="flex items-center space-x-2">
          <div className="p-1.5 rounded-lg bg-sky-500/10 text-sky-400">
            <Truck className="w-5 h-5" />
          </div>
          <div>
            <h2 className="font-semibold text-slate-100 text-sm tracking-tight flex items-center gap-2">
              IoT Fleet Live Windowed Aggregations
              <span className="text-xs font-mono font-normal text-slate-400">
                (50,000 Total Fleet &bull; 5-Min Rolling Windows)
              </span>
            </h2>
            <p className="text-xs text-slate-400">
              Faust workers compute rolling averages every 10s using embedded RocksDB tables
            </p>
          </div>
        </div>

        {/* Filters & Search */}
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <div className="relative">
            <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search Truck ID or Route..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="bg-slate-950 border border-slate-800 text-slate-200 rounded-lg pl-8 pr-3 py-1.5 focus:outline-none focus:border-indigo-500 w-44"
            />
          </div>

          {/* Cargo Dropdown */}
          <select
            value={cargoFilter}
            onChange={(e) => setCargoFilter(e.target.value)}
            className="bg-slate-950 border border-slate-800 text-slate-300 rounded-lg px-2.5 py-1.5 focus:outline-none"
          >
            <option value="ALL">All Cargo Types</option>
            <option value="Frozen Seafood">Frozen Seafood (-22°C)</option>
            <option value="Vaccines & Pharma">Vaccines & Pharma (+4°C)</option>
            <option value="Dairy & Fresh Milk">Dairy & Fresh Milk (+2.5°C)</option>
            <option value="Organic Berries">Organic Berries (+1.5°C)</option>
            <option value="Poultry">Poultry (-18°C)</option>
          </select>

          {/* Status buttons */}
          <div className="flex rounded-lg bg-slate-950 p-0.5 border border-slate-800">
            <button
              onClick={() => setFilter('ALL')}
              className={`px-2.5 py-1 rounded-md transition-all ${
                filter === 'ALL' ? 'bg-indigo-600 text-white font-medium' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              All
            </button>
            <button
              onClick={() => setFilter('EXCURSIONS')}
              className={`px-2.5 py-1 rounded-md transition-all flex items-center gap-1 ${
                filter === 'EXCURSIONS' ? 'bg-rose-600 text-white font-medium' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Excursions ({excursionCount})
            </button>
          </div>
        </div>
      </div>

      {/* Fleet Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs text-slate-300">
          <thead className="bg-slate-950/80 text-slate-400 uppercase font-mono text-[10px] tracking-wider border-b border-slate-800">
            <tr>
              <th className="py-2.5 px-3">Truck ID & Route</th>
              <th className="py-2.5 px-3">Cargo & Target</th>
              <th className="py-2.5 px-3">Kafka Partition</th>
              <th className="py-2.5 px-3">Current Temp</th>
              <th className="py-2.5 px-3">5-Min Rolling Avg</th>
              <th className="py-2.5 px-3">Window Range (Min/Max)</th>
              <th className="py-2.5 px-3">Status</th>
              <th className="py-2.5 px-3 text-right">Simulation</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-mono">
            {filteredTrucks.map((truck) => {
              const isExcursion = truck.status === 'CRITICAL_EXCURSION';
              const isSelected = selectedTruckId === truck.truckId;
              const deviation = +(truck.rollingAvg5mC - truck.targetTempC).toFixed(1);

              return (
                <tr
                  key={truck.truckId}
                  onClick={() => onSelectTruck(truck)}
                  className={`cursor-pointer transition-all hover:bg-slate-800/50 ${
                    isSelected ? 'bg-slate-800/80' : isExcursion ? 'bg-rose-950/20' : ''
                  }`}
                >
                  {/* Truck ID & Route */}
                  <td className="py-2.5 px-3">
                    <div className="font-bold text-white flex items-center gap-1.5">
                      <Truck className="w-3.5 h-3.5 text-sky-400" />
                      {truck.truckId}
                    </div>
                    <div className="text-[10px] text-slate-400 font-sans truncate max-w-[150px]">
                      {truck.route}
                    </div>
                  </td>

                  {/* Cargo & Target */}
                  <td className="py-2.5 px-3 font-sans">
                    <div className="text-slate-200 font-medium flex items-center gap-1">
                      {truck.targetTempC < 0 ? (
                        <Snowflake className="w-3 h-3 text-sky-400" />
                      ) : (
                        <Thermometer className="w-3 h-3 text-emerald-400" />
                      )}
                      {truck.cargoType}
                    </div>
                    <div className="text-[10px] text-slate-400 font-mono">
                      Target: {truck.targetTempC > 0 ? `+${truck.targetTempC}` : truck.targetTempC}&deg;C
                    </div>
                  </td>

                  {/* Kafka Partition */}
                  <td className="py-2.5 px-3">
                    <span className="px-2 py-0.5 rounded bg-slate-800 text-indigo-300 border border-slate-700 text-[10px] font-semibold">
                      Partition-{truck.partition}
                    </span>
                  </td>

                  {/* Current Temp */}
                  <td className="py-2.5 px-3">
                    <span className={`font-bold ${isExcursion ? 'text-rose-400' : 'text-slate-200'}`}>
                      {truck.currentTempC > 0 ? `+${truck.currentTempC}` : truck.currentTempC}&deg;C
                    </span>
                  </td>

                  {/* 5-Min Rolling Avg */}
                  <td className="py-2.5 px-3">
                    <div className="flex items-center space-x-2">
                      <span className={`font-bold text-sm ${isExcursion ? 'text-rose-400' : 'text-emerald-400'}`}>
                        {truck.rollingAvg5mC > 0 ? `+${truck.rollingAvg5mC}` : truck.rollingAvg5mC}&deg;C
                      </span>
                      <span className={`text-[10px] ${deviation > 0 ? 'text-rose-400' : 'text-sky-400'}`}>
                        ({deviation > 0 ? `+${deviation}` : deviation}&deg;C)
                      </span>
                    </div>
                    <div className="text-[10px] text-slate-500 font-sans">
                      {truck.samplesInWindow} samples in RocksDB
                    </div>
                  </td>

                  {/* Window Min/Max */}
                  <td className="py-2.5 px-3 text-[11px] text-slate-400">
                    <span>Min: {truck.minTemp5mC}&deg;C</span> &bull; <span>Max: {truck.maxTemp5mC}&deg;C</span>
                  </td>

                  {/* Status Badge */}
                  <td className="py-2.5 px-3">
                    {isExcursion ? (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-300 border border-rose-500/40 text-[10px] font-semibold animate-pulse">
                        <AlertTriangle className="w-3 h-3 text-rose-400" />
                        Cold-Chain Excursion
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 text-[10px]">
                        <CheckCircle className="w-3 h-3 text-emerald-400" />
                        Compliant
                      </span>
                    )}
                  </td>

                  {/* Simulation Action */}
                  <td className="py-2.5 px-3 text-right">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        onTriggerTruckExcursion(truck.truckId);
                      }}
                      className="px-2 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white text-[10px] font-sans transition-all border border-slate-700"
                      title="Trigger reefer unit failure for this truck"
                    >
                      {isExcursion ? 'Reset Temp' : 'Spike Temp'}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

