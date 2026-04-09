import { create } from "zustand";

export type TabId = "paths" | "cycles";

interface TabStore {
  activeTab: TabId;
  setActiveTab: (tab: TabId) => void;
}

export const useTabStore = create<TabStore>((set) => ({
  activeTab: "paths",
  setActiveTab: (tab) => set({ activeTab: tab }),
}));
