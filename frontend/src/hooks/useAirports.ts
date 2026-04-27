import { useQuery } from "@tanstack/react-query";
import { getAirports } from "@/api/client";

export function useAirports() {
  return useQuery({
    queryKey: ["airports"],
    queryFn: getAirports,
    staleTime: 5 * 60 * 1000,
  });
}
