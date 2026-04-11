import { RotateCcw } from "lucide-react";
import { useFilterStore } from "@/stores/filterStore";
import { useMapStore } from "@/stores/mapStore";

function toISODate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export default function TimeFrame() {
  const { dateFrom, dateTo, setDateRange, clearDateRange } = useFilterStore();
  const clearAll = useMapStore((s) => s.clearAll);

  const today = toISODate(new Date());
  const oneWeek = toISODate(new Date(Date.now() + 7 * 24 * 60 * 60 * 1000));

  const from = dateFrom || today;
  const to = dateTo || oneWeek;

  function handleFromChange(val: string) {
    const newTo = to < val ? val : to;
    setDateRange(val, newTo);
  }

  function handleToChange(val: string) {
    setDateRange(from, val);
  }

  function handleResetAll() {
    clearAll();
    clearDateRange();
    // Re-enable all airlines
    useMapStore.setState({ activeAirlines: new Set(["FR", "W6"]) });
  }

  return (
    <div className="flex items-center gap-1.5">
      <div className="relative flex-1">
        <span className="absolute left-2 top-1/2 -translate-y-1/2 text-[10px] text-[#484f58] pointer-events-none">
          From
        </span>
        <input
          type="date"
          value={from}
          min={today}
          onChange={(e) => handleFromChange(e.target.value)}
          onClick={(e) => { try { (e.target as HTMLInputElement).showPicker(); } catch {} }}
          className="w-full bg-[#0d1117] border border-[#30363d] rounded-md py-1.5 pl-10 pr-1 text-[11px] text-[#c9d1d9] focus:outline-none focus:border-[#58a6ff] transition-colors [color-scheme:dark] cursor-pointer [&::-webkit-calendar-picker-indicator]:absolute [&::-webkit-calendar-picker-indicator]:inset-0 [&::-webkit-calendar-picker-indicator]:w-full [&::-webkit-calendar-picker-indicator]:h-full [&::-webkit-calendar-picker-indicator]:opacity-0 [&::-webkit-calendar-picker-indicator]:cursor-pointer"
        />
      </div>
      <div className="relative flex-1">
        <span className="absolute left-2 top-1/2 -translate-y-1/2 text-[10px] text-[#484f58] pointer-events-none">
          To
        </span>
        <input
          type="date"
          value={to}
          min={from}
          onChange={(e) => handleToChange(e.target.value)}
          onClick={(e) => { try { (e.target as HTMLInputElement).showPicker(); } catch {} }}
          className="w-full bg-[#0d1117] border border-[#30363d] rounded-md py-1.5 pl-7 pr-1 text-[11px] text-[#c9d1d9] focus:outline-none focus:border-[#58a6ff] transition-colors [color-scheme:dark] cursor-pointer [&::-webkit-calendar-picker-indicator]:absolute [&::-webkit-calendar-picker-indicator]:inset-0 [&::-webkit-calendar-picker-indicator]:w-full [&::-webkit-calendar-picker-indicator]:h-full [&::-webkit-calendar-picker-indicator]:opacity-0 [&::-webkit-calendar-picker-indicator]:cursor-pointer"
        />
      </div>
      <button
        onClick={handleResetAll}
        title="Reset all filters"
        className="shrink-0 p-1.5 rounded-md border border-[#30363d] text-[#8b949e] hover:text-[#e5534b] hover:border-[#e5534b]/40 transition-colors"
      >
        <RotateCcw size={12} />
      </button>
    </div>
  );
}
