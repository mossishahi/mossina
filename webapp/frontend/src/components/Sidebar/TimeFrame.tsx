import { useState, useEffect } from "react";
import { Calendar, X } from "lucide-react";
import { useFilterStore } from "@/stores/filterStore";

function toISODate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export default function TimeFrame() {
  const { dateFrom, dateTo, setDateRange, clearDateRange } = useFilterStore();

  const today = toISODate(new Date());
  const oneWeek = toISODate(
    new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
  );

  const [from, setFrom] = useState(dateFrom || today);
  const [to, setTo] = useState(dateTo || oneWeek);

  useEffect(() => {
    if (dateFrom) setFrom(dateFrom);
    if (dateTo) setTo(dateTo);
  }, [dateFrom, dateTo]);

  function handleApply() {
    setDateRange(from, to);
  }

  function handleClear() {
    clearDateRange();
    setFrom(today);
    setTo(oneWeek);
  }

  return (
    <div>
      <h3 className="text-xs font-medium text-[#8b949e] uppercase tracking-wider mb-2 flex items-center gap-1.5">
        <Calendar size={12} />
        Time Frame
      </h3>
      <div className="space-y-2">
        <div className="flex gap-2">
          <div className="flex-1">
            <label className="text-xs text-[#484f58] block mb-1">From</label>
            <input
              type="date"
              value={from}
              onChange={(e) => setFrom(e.target.value)}
              className="w-full bg-[#161b22] border border-[#30363d] rounded-lg py-1.5 px-2 text-sm text-[#c9d1d9] focus:outline-none focus:border-[#58a6ff] transition-colors [color-scheme:dark]"
            />
          </div>
          <div className="flex-1">
            <label className="text-xs text-[#484f58] block mb-1">To</label>
            <input
              type="date"
              value={to}
              onChange={(e) => setTo(e.target.value)}
              className="w-full bg-[#161b22] border border-[#30363d] rounded-lg py-1.5 px-2 text-sm text-[#c9d1d9] focus:outline-none focus:border-[#58a6ff] transition-colors [color-scheme:dark]"
            />
          </div>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleApply}
            className="flex-1 py-1.5 bg-[#238636] hover:bg-[#2ea043] text-white text-sm font-medium rounded-lg transition-colors"
          >
            Apply
          </button>
          <button
            onClick={handleClear}
            className="px-3 py-1.5 bg-[#161b22] hover:bg-[#30363d] text-[#8b949e] text-sm border border-[#30363d] rounded-lg transition-colors flex items-center gap-1"
          >
            <X size={12} />
            Clear
          </button>
        </div>
      </div>
    </div>
  );
}
