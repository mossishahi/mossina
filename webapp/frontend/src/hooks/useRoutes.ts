import { useQuery } from "@tanstack/react-query";
import { getRoutes } from "@/api/client";
import { useMapStore } from "@/stores/mapStore";

export function useRoutes() {
  const activeAirlines = useMapStore((s) => s.activeAirlines);

  return useQuery({
    queryKey: ["routes", [...activeAirlines].sort().join(",")],
    queryFn: () => getRoutes(),
    staleTime: 5 * 60 * 1000,
    select: (routes) =>
      routes.filter((r) => activeAirlines.has(r.airline)),
  });
}
