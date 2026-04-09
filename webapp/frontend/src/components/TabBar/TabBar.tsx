import { Route, RotateCcw } from "lucide-react";
import { useTabStore } from "@/stores/tabStore";
import type { TabId } from "@/stores/tabStore";

const TABS: { id: TabId; label: string; icon: typeof Route }[] = [
  { id: "paths", label: "Paths", icon: Route },
  { id: "cycles", label: "Cycles", icon: RotateCcw },
];

export default function TabBar() {
  const activeTab = useTabStore((s) => s.activeTab);
  const setActiveTab = useTabStore((s) => s.setActiveTab);

  return (
    <div className="flex items-end px-2 pt-2 gap-0.5 bg-[#0d1117] border-b border-[#30363d]"
      style={{ borderTopLeftRadius: "10px", borderTopRightRadius: "10px" }}
    >
      {TABS.map((tab) => {
        const active = activeTab === tab.id;
        const Icon = tab.icon;
        return (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-1.5 px-4 py-1.5 text-xs font-semibold transition-colors ${
              active
                ? "text-[#58a6ff] bg-black/80 border border-[#30363d] border-b-transparent -mb-px"
                : "text-[#484f58] hover:text-[#8b949e] bg-transparent border border-transparent"
            }`}
            style={{
              borderTopLeftRadius: "8px",
              borderTopRightRadius: "8px",
              borderBottomLeftRadius: "0",
              borderBottomRightRadius: "0",
            }}
          >
            <Icon size={12} />
            {tab.label}
          </button>
        );
      })}
    </div>
  );
}
