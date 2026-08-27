import { AnimalDetailClient } from "./AnimalDetailClient";

export default async function AnimalDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ farm?: string }>;
}) {
  const { id } = await params;
  const { farm } = await searchParams;
  return <AnimalDetailClient id={id} farmId={farm} />;
}
