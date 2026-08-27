"use client";

import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const COLORS = ["#14532d", "#c98a2c", "#1e7d43", "#b3261e", "#57685f"];

export function TimeseriesChart({
  data,
  xKey,
  series,
  height = 260,
}: {
  data: Record<string, unknown>[];
  xKey: string;
  series: string[];
  height?: number;
}) {
  if (data.length === 0) {
    return (
      <div className="flex h-[260px] items-center justify-center text-sm text-[var(--foreground-muted)]">
        No time series data available.
      </div>
    );
  }
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
        <XAxis
          dataKey={xKey}
          tickFormatter={(v) => String(v).slice(0, 10)}
          tick={{ fontSize: 11, fill: "var(--foreground-muted)" }}
        />
        <YAxis tick={{ fontSize: 11, fill: "var(--foreground-muted)" }} />
        <Tooltip
          labelFormatter={(v) => String(v).slice(0, 10)}
          contentStyle={{ fontSize: 12, borderRadius: 8, borderColor: "var(--border)" }}
        />
        {series.map((key, i) => (
          <Line
            key={key}
            type="monotone"
            dataKey={key}
            stroke={COLORS[i % COLORS.length]}
            dot={false}
            strokeWidth={2}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}
