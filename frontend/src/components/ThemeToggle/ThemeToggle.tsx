import { Sun, Moon } from "lucide-react";
import { useThemeStore } from "@/stores/themeStore";

export default function ThemeToggle() {
  const theme = useThemeStore((s) => s.theme);
  const toggle = useThemeStore((s) => s.toggle);
  const isDark = theme === "dark";

  return (
    <button
      onClick={toggle}
      title={isDark ? "Switch to light map" : "Switch to dark map"}
      aria-label="Toggle map theme"
      className={
        "absolute top-4 right-4 z-20 inline-flex items-center justify-center w-9 h-9 rounded-lg backdrop-blur-xl border shadow-lg transition-colors " +
        (isDark
          ? "bg-black/80 border-[#30363d] text-[#c9d1d9] hover:text-white hover:border-[#58a6ff]/50"
          : "bg-white/85 border-[#d0d7de] text-[#24292f] hover:text-black hover:border-[#0969da]/60")
      }
    >
      {isDark ? <Sun size={15} /> : <Moon size={15} />}
    </button>
  );
}
