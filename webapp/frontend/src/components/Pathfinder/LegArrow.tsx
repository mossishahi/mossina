import { useState, useRef, useEffect } from "react";
import { AIRLINE_META } from "@/api/types";

export interface LegFilterValue {
  airline: string | null;
}

export const emptyLeg = (): LegFilterValue => ({ airline: null });

interface Props {
  value: LegFilterValue;
  onChange: (val: LegFilterValue) => void;
  airlines: string[];
}

export default function LegArrow({ value, onChange, airlines }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const color = value.airline
    ? AIRLINE_META[value.airline]?.color || "#8b949e"
    : "#8b949e";

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  return (
    <div className="relative flex items-center" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        className="text-[15px] font-bold transition-colors hover:opacity-80 px-1.5"
        style={{ color }}
        title={value.airline ? AIRLINE_META[value.airline]?.name || value.airline : "Any airline"}
      >
        ——→
      </button>

      {open && (
        <div className="absolute top-full left-1/2 -translate-x-1/2 mt-1 z-50 bg-[#161b22] border border-[#30363d] rounded-lg shadow-xl w-32 p-1.5">
          <button
            onClick={() => { onChange({ airline: null }); setOpen(false); }}
            className={`w-full text-left px-2 py-1 text-[10px] rounded hover:bg-[#21262d] ${
              !value.airline ? "text-[#58a6ff] font-semibold" : "text-[#8b949e]"
            }`}
          >
            Any airline
          </button>
          {airlines.map((code) => {
            const meta = AIRLINE_META[code];
            const active = value.airline === code;
            return (
              <button
                key={code}
                onClick={() => { onChange({ airline: active ? null : code }); setOpen(false); }}
                className={`w-full text-left px-2 py-1 text-[10px] rounded hover:bg-[#21262d] flex items-center gap-1.5 ${
                  active ? "font-semibold" : ""
                }`}
              >
                <span
                  className="w-2 h-2 rounded-full shrink-0"
                  style={{ background: meta?.color || "#888" }}
                />
                <span style={{ color: active ? meta?.color : "#c9d1d9" }}>
                  {meta?.name || code}
                </span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
