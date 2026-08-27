"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const TABS = [
  { href: "/", label: "Farm Overview" },
  { href: "/market-signals", label: "Market Signals" },
  { href: "/reports", label: "Reports" },
  { href: "/upload", label: "Upload Data" },
];

function isActive(pathname: string, href: string) {
  if (href === "/") return pathname === "/" || pathname.startsWith("/farms/") || pathname.startsWith("/animals/");
  return pathname.startsWith(href);
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="flex min-h-screen flex-col">
      <header className="no-print sticky top-0 z-10 border-b border-[var(--border)] bg-[var(--surface)]/95 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-6 px-6 py-3">
          <div className="flex items-baseline gap-2">
            <span className="text-lg font-semibold tracking-tight text-[var(--primary-strong)]">
              Livestock Intelligence
            </span>
            <span className="rounded-full bg-[var(--accent-soft)] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-[var(--accent)]">
              Dev
            </span>
          </div>
          <nav className="flex gap-1 rounded-full border border-[var(--border)] bg-[var(--surface-muted)] p-1">
            {TABS.map((tab) => (
              <Link
                key={tab.href}
                href={tab.href}
                className={`rounded-full px-4 py-1.5 text-sm font-medium transition-colors ${
                  isActive(pathname, tab.href)
                    ? "bg-[var(--primary)] text-white"
                    : "text-[var(--foreground-muted)] hover:text-[var(--foreground)]"
                }`}
              >
                {tab.label}
              </Link>
            ))}
          </nav>
          <div className="flex items-center gap-2 text-xs text-[var(--foreground-muted)]">
            <span className="flex items-center gap-1.5">
              <span className="h-1.5 w-1.5 rounded-full bg-[var(--ok)]" />
              Data live
            </span>
          </div>
        </div>
      </header>
      <main className="mx-auto w-full max-w-7xl flex-1 px-6 py-8">{children}</main>
    </div>
  );
}
