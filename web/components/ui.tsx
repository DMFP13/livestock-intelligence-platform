import type { ReactNode } from "react";

export function Panel({
  title,
  action,
  children,
  className = "",
}: {
  title?: string;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section
      className={`rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5 shadow-sm ${className}`}
    >
      {(title || action) && (
        <div className="mb-4 flex items-center justify-between gap-3">
          {title && <h2 className="text-sm font-semibold tracking-wide text-[var(--foreground)]">{title}</h2>}
          {action}
        </div>
      )}
      {children}
    </section>
  );
}

const STATUS_STYLES: Record<string, string> = {
  green: "bg-[var(--ok-soft)] text-[var(--ok)]",
  yellow: "bg-[var(--warn-soft)] text-[var(--warn)]",
  red: "bg-[var(--danger-soft)] text-[var(--danger)]",
  neutral: "bg-[var(--surface-muted)] text-[var(--foreground-muted)]",
};

export function KpiCard({
  label,
  value,
  status = "neutral",
  hint,
}: {
  label: string;
  value: string | number;
  status?: "green" | "yellow" | "red" | "neutral";
  hint?: string;
}) {
  return (
    <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-4 shadow-sm">
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-medium uppercase tracking-wide text-[var(--foreground-muted)]">
          {label}
        </span>
        <span className={`h-2 w-2 rounded-full ${STATUS_STYLES[status].split(" ")[0]}`} />
      </div>
      <div className="mt-2 text-2xl font-semibold text-[var(--foreground)]">{value}</div>
      {hint && <div className="mt-1 text-xs text-[var(--foreground-muted)]">{hint}</div>}
    </div>
  );
}

export function Badge({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: "green" | "yellow" | "red" | "neutral";
}) {
  return (
    <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${STATUS_STYLES[tone]}`}>{children}</span>
  );
}

export function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-dashed border-[var(--border)] bg-[var(--surface-muted)] px-4 py-8 text-center text-sm text-[var(--foreground-muted)]">
      {message}
    </div>
  );
}

export function Table({
  columns,
  rows,
}: {
  columns: { key: string; label: string }[];
  rows: Record<string, ReactNode>[];
}) {
  if (rows.length === 0) {
    return <EmptyState message="No rows to show." />;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
      <table className="w-full text-sm">
        <thead className="bg-[var(--surface-muted)] text-left">
          <tr>
            {columns.map((c) => (
              <th key={c.key} className="whitespace-nowrap px-3 py-2 font-medium text-[var(--foreground-muted)]">
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-t border-[var(--border)]">
              {columns.map((c) => (
                <td key={c.key} className="whitespace-nowrap px-3 py-2">
                  {row[c.key] ?? "—"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function Button({
  children,
  onClick,
  variant = "primary",
  type = "button",
  disabled,
}: {
  children: ReactNode;
  onClick?: () => void;
  variant?: "primary" | "secondary";
  type?: "button" | "submit";
  disabled?: boolean;
}) {
  const base = "rounded-lg px-4 py-2 text-sm font-medium transition-colors disabled:opacity-50";
  const styles =
    variant === "primary"
      ? "bg-[var(--primary)] text-white hover:bg-[var(--primary-strong)]"
      : "border border-[var(--border)] bg-[var(--surface)] text-[var(--foreground)] hover:bg-[var(--surface-muted)]";
  return (
    <button type={type} onClick={onClick} disabled={disabled} className={`${base} ${styles}`}>
      {children}
    </button>
  );
}
