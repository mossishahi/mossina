/**
 * Dual-handle range slider. Two stacked HTML range inputs share the same
 * track; only their thumbs are interactive so they look like one control.
 *
 * Min handle is clamped to <= max value; max handle to >= min value.
 */
interface Props {
  min: number;
  max: number;
  value: [number, number];
  onChange: (next: [number, number]) => void;
}

export default function RangeSlider({ min, max, value, onChange }: Props) {
  const [lo, hi] = value;
  const span = Math.max(1, max - min);
  const loPct = ((lo - min) / span) * 100;
  const hiPct = ((hi - min) / span) * 100;

  function handleLo(e: React.ChangeEvent<HTMLInputElement>) {
    const v = Math.min(Number(e.target.value), hi);
    if (v !== lo) onChange([v, hi]);
  }

  function handleHi(e: React.ChangeEvent<HTMLInputElement>) {
    const v = Math.max(Number(e.target.value), lo);
    if (v !== hi) onChange([lo, v]);
  }

  return (
    <div className="relative h-5 w-full select-none">
      <div className="absolute top-1/2 -translate-y-1/2 left-0 right-0 h-[3px] bg-[#21262d] rounded-full" />
      <div
        className="absolute top-1/2 -translate-y-1/2 h-[3px] bg-[#58a6ff] rounded-full"
        style={{ left: `${loPct}%`, right: `${100 - hiPct}%` }}
      />
      <input
        type="range"
        min={min}
        max={max}
        step={1}
        value={lo}
        onChange={handleLo}
        className="dual-range"
        style={{ zIndex: lo > max - 1 ? 4 : 3 }}
        aria-label="Minimum stay (days)"
      />
      <input
        type="range"
        min={min}
        max={max}
        step={1}
        value={hi}
        onChange={handleHi}
        className="dual-range"
        style={{ zIndex: 4 }}
        aria-label="Maximum stay (days)"
      />
    </div>
  );
}
