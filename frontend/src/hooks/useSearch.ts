import { useMutation } from "@tanstack/react-query";
import { searchPaths, searchCycles } from "@/api/client";
import type { PathSearchRequest, CycleSearchRequest } from "@/api/types";

export function useSearchPaths() {
  return useMutation({
    mutationFn: (body: PathSearchRequest) => searchPaths(body),
  });
}

export function useSearchCycles() {
  return useMutation({
    mutationFn: (body: CycleSearchRequest) => searchCycles(body),
  });
}
