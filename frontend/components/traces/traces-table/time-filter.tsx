"use client";

import { parseAsStringLiteral, useQueryState } from "nuqs";

import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

export function TracesTimeFilter() {
  const [field, setField] = useQueryState(
    "timeField",
    parseAsStringLiteral(["start", "activity"] as const).withDefault("activity")
  );

  return (
    <Select value={field} onValueChange={(value) => void setField(value === "start" ? "start" : "activity")}>
      <SelectTrigger className="w-36" aria-label="Filter time by">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="activity">Last activity</SelectItem>
        <SelectItem value="start">Started</SelectItem>
      </SelectContent>
    </Select>
  );
}
